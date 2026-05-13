#include "jobs/transfer_engine/transfer_engine.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "common/buffer_pool.hpp"
#include "common/config.hpp"
#include "common/content_hash.hpp"
#include "common/filesystem_utils.hpp"
#include "common/fixed_string.hpp"
#include "common/hash_utils.hpp"
#include "core/metadata_record_writer.hpp"
#include "core/metadata_buffer_codec.hpp"
#include "core/nfs_backend.hpp"
#include "common/path_utils.hpp"
#include "common/preallocated_ring.hpp"
#include "common/records.hpp"
#include "common/socket_utils.hpp"
#include "common/slot_pool.hpp"
#include "common/state_machine.hpp"
#include "common/version.hpp"
#include "common/watermarks.hpp"
#include "core/pipeline_buffers.hpp"
#include "jobs/buffer_discarder/buffer_discarder.hpp"
#include "jobs/buffer_generator/buffer_generator.hpp"
#include "jobs/buffer_transport/buffer_transport.hpp"
#include "jobs/checker/checker.hpp"
#include "jobs/data_cacher/data_cacher.hpp"
#include "jobs/data_hasher/data_hasher.hpp"
#include "jobs/file_metadata_generator/file_metadata_generator.hpp"
#include "jobs/metadata_stats_discarder/metadata_stats_discarder.hpp"
#include "jobs/metadata_record_writer_job/metadata_record_writer_job.hpp"
#include "jobs/nfs_data_reader/nfs_data_buffer_reader.hpp"
#include "jobs/nfs_data_reader/nfs_data_reader.hpp"
#include "jobs/nfs_meta_reader/nfs_meta_reader.hpp"
#include "monitoring/status_monitor.hpp"

namespace hypersync {

namespace {

enum class PriorityMessageType : std::uint32_t {
    session_start = 1,
    file_record = 2,
    file_decision = 3,
    file_ack = 4,
    session_end = 5,
    pause = 6,
    resume = 7,
    directory_record = 8,
};

struct FileRecordMessage {
    std::uint64_t file_id = 0;
    std::uint64_t folder_hash = 0;
    FixedString<kDataBufRelPathBytes> rel_path;
    std::uint64_t size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
};

struct FileDecisionMessage {
    std::uint64_t file_id = 0;
    bool skip = false;
};

struct DirectoryRecordMessage {
    FixedString<kDataBufRelPathBytes> rel_path;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
};

struct FileAckMessage {
    std::uint64_t file_id = 0;
    bool hash_verified = false;
    std::uint64_t bytes_written = 0;
};

struct ReceiverFileContext {
    FileRecordMessage record;
    std::uint64_t bytes_written = 0;
    std::uint64_t next_pause_after_bytes = 0;
};

struct ReceiverSharedState {
    ReceiverRuntimeConfig runtime;
    bool skip_verify = false;
    bool session_done = false;
    std::uint64_t session_large_chunk_bytes = kLargeChunkBytes;
    std::size_t small_pool_slots = 0;
    std::size_t large_pool_slots = 0;
    int priority_fd = -1;
    int data_fd = -1;
    std::mutex metadata_mutex;
    std::condition_variable metadata_cv;
    std::mutex priority_write_mutex;
    std::unordered_map<std::uint64_t, FileRecordMessage> pending_records;
    std::vector<FileSpec> directory_specs;
};

struct CachedFilePlan {
    std::uint64_t data_hash = 0;
    std::size_t chunk_count = 0;
    std::vector<std::uint64_t> entry_ids;
};

struct PreparedTransfer {
    FileSpec file;
    FileRecordMessage record;
    std::uint64_t data_hash = 0;
    std::size_t chunk_count = 0;
    bool content_loaded = false;
    bool cached = false;
    std::filesystem::path source_path;
    std::vector<std::uint64_t> cached_entry_ids;
};

struct PendingTransferFile {
    FileSpec file;
    FileRecordMessage record;
};

struct AwaitingAckTransfer {
    PreparedTransfer prepared;
    std::uint64_t data_hash = 0;
    std::size_t chunk_count = 0;
};

struct PreparedTransferQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    PreallocatedRing<PreparedTransfer> queue;
    std::size_t max_entries = 1;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr producer_error;
};

struct ReceiverQueuedChunk {
    DataSlotHandle handle;
    FileRecordMessage record;
};

struct ReceiverChunkQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    PreallocatedRing<ReceiverQueuedChunk> queue;
    std::uint64_t capacity_bytes = 0;
    std::uint64_t queued_bytes = 0;
    bool paused = false;
    bool input_done = false;
    std::exception_ptr worker_error;
};

struct FlatMetadataWorkQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<FileSpec> folders;
    std::optional<std::chrono::steady_clock::time_point> stop_at;
    std::size_t active = 0;
    bool done = false;
    std::exception_ptr error;
};

struct DiffTargetFolderQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<FileSpec> folders;
    std::size_t active = 0;
    std::size_t max_entries = 65536;
    bool input_done = false;
    bool done = false;
    std::exception_ptr error;
};

struct DataReadFileQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    std::deque<FileSpec> files;
    std::optional<std::chrono::steady_clock::time_point> stop_at;
    std::size_t max_entries = 1024;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr error;
};

std::string metadata_part_suffix(std::size_t index) {
    std::ostringstream out;
    out << std::setw(5) << std::setfill('0') << index;
    return out.str();
}

std::string metadata_extension_for_format(MetadataRecordFormat format) {
    switch (format) {
        case MetadataRecordFormat::text:
            return "txt";
        case MetadataRecordFormat::csv:
            return "csv";
        case MetadataRecordFormat::parquet:
            return "parquet";
    }
    return "txt";
}

std::filesystem::path metadata_partition_output_path(const std::filesystem::path& output_path,
                                                     MetadataRecordFormat format,
                                                     std::size_t partitions,
                                                     std::size_t index) {
    if (partitions == 1U) {
        return output_path;
    }
    return output_path / ("part-" + metadata_part_suffix(index) + "." + metadata_extension_for_format(format));
}

std::filesystem::path metadata_partition_socket_path(const std::filesystem::path& output_path, std::size_t index) {
    const std::filesystem::path candidate = output_path / ("part-" + metadata_part_suffix(index) + ".sock");
    if (candidate.string().size() <= 90U) {
        return candidate;
    }
    std::ostringstream name;
    name << "hypersync-" << std::hex << hash64(output_path.string()) << '-'
         << metadata_part_suffix(index) << ".sock";
    return std::filesystem::path("/tmp") / name.str();
}

std::filesystem::path metadata_partition_report_path(const std::filesystem::path& output_path, std::size_t index) {
    return output_path / ("part-" + metadata_part_suffix(index) + ".report");
}

std::size_t metadata_partition_for_path(std::string_view path, std::size_t partitions) {
    return static_cast<std::size_t>(hash64(path) % std::max<std::size_t>(1U, partitions));
}

std::size_t metadata_transport_payload_bytes(RawBufferPool& pool, const BufferHandle& handle) {
    if (handle.pool_id == kMetadataBufferPoolId) {
        const auto& buffer = metadata_buffer(pool, handle);
        return offsetof(MetadataBuffer, bytes) + buffer.bytes_used;
    }
    if (handle.pool_id == kMetadataBatchBufferPoolId) {
        const auto& buffer = metadata_batch_buffer(pool, handle);
        return offsetof(MetadataBatchBuffer, bytes) + buffer.bytes_used;
    }
    return pool.buffer_size_bytes();
}

void write_metadata_partition_report(const std::filesystem::path& path,
                                     std::uint64_t files_written,
                                     std::uint64_t folders_written) {
    std::ofstream output(path, std::ios::out | std::ios::trunc);
    if (!output) {
        throw std::runtime_error("failed to open metadata partition report: " + path.string());
    }
    output << files_written << ' ' << folders_written << '\n';
}

std::pair<std::uint64_t, std::uint64_t> read_metadata_partition_report(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("failed to read metadata partition report: " + path.string());
    }
    std::uint64_t files_written = 0;
    std::uint64_t folders_written = 0;
    input >> files_written >> folders_written;
    if (!input) {
        throw std::runtime_error("malformed metadata partition report: " + path.string());
    }
    return {files_written, folders_written};
}

void run_metadata_writer_partition_process(const MetadataRecordWriterConfig& writer_config,
                                           const std::filesystem::path& output_path,
                                           std::size_t partitions,
                                           std::size_t partition_index) {
    MetadataRecordWriterConfig partition_config = writer_config;
    partition_config.output_path =
        metadata_partition_output_path(output_path, writer_config.format, partitions, partition_index);
    if (partition_config.run_info.has_value()) {
        partition_config.run_info->run_id += "-part-" + metadata_part_suffix(partition_index);
        partition_config.run_info->settings_json +=
            ",{\"partition\":" + std::to_string(partition_index) +
            ",\"partitions\":" + std::to_string(partitions) + "}";
    }

    RawBufferPool receive_pool(kMetadataBatchBufferPoolId,
                               64U,
                               sizeof(MetadataBatchBuffer),
                               alignof(MetadataBatchBuffer));
    BufferPoolRegistry receive_registry;
    receive_registry.register_pool(receive_pool);
    BufQueue writer_queue(receive_pool.capacity());
    BufferReceiverJob receiver(1U,
                               receive_pool,
                               writer_queue,
                               BufferTransportEndpoint::unix_socket(metadata_partition_socket_path(output_path, partition_index)));
    MetadataRecordWriterJob writer(1U, writer_queue, receive_registry, partition_config);

    receiver.start();
    writer.start();
    receiver.wait();
    writer.wait();

    const auto stats = writer.writer_stats();
    write_metadata_partition_report(metadata_partition_report_path(output_path, partition_index),
                                    stats.files_written,
                                    stats.folders_written);
}

class PartitionedMetadataWriter {
public:
    PartitionedMetadataWriter(MetadataRecordWriterConfig writer_config,
                              std::filesystem::path output_path,
                              std::size_t partitions)
        : writer_config_(std::move(writer_config)),
          output_path_(std::move(output_path)),
          partitions_(std::max<std::size_t>(1U, partitions)) {
        if (partitions_ <= 1U) {
            throw std::invalid_argument("partitioned metadata writer requires more than one partition");
        }
        std::filesystem::create_directories(output_path_);
        children_.reserve(partitions_);
        states_.reserve(partitions_);

        for (std::size_t index = 0; index < partitions_; ++index) {
            std::filesystem::remove(metadata_partition_socket_path(output_path_, index));
            std::filesystem::remove(metadata_partition_report_path(output_path_, index));
            const pid_t child = ::fork();
            if (child < 0) {
                throw std::runtime_error("failed to fork metadata writer partition process");
            }
            if (child == 0) {
                try {
                    run_metadata_writer_partition_process(writer_config_, output_path_, partitions_, index);
                    _Exit(0);
                } catch (const std::exception& error) {
                    std::cerr << "metadata writer partition " << index << " failed: "
                              << error.what() << '\n';
                    _Exit(101);
                } catch (...) {
                    std::cerr << "metadata writer partition " << index << " failed\n";
                    _Exit(101);
                }
            }
            children_.push_back(child);
        }

        for (std::size_t index = 0; index < partitions_; ++index) {
            states_.push_back(std::make_unique<PartitionSendState>(
                64U,
                metadata_partition_socket_path(output_path_, index)));
            states_.back()->sender.start();
        }
    }

    ~PartitionedMetadataWriter() {
        if (!closed_) {
            try {
                close();
            } catch (...) {
            }
        }
    }

    void write_batch(const std::vector<FileSpec>& files, const MetadataFolderRecord& folder) {
        if (!writer_config_.write_folders && (!writer_config_.write_files || files.empty())) {
            return;
        }

        const std::size_t partition = metadata_partition_for_path(folder.spec.rel_path, partitions_);
        std::lock_guard<std::mutex> lock(states_[partition]->mutex);
        bool include_folder_record = writer_config_.write_folders;
        auto& batch = start_folder_batch(partition, folder, include_folder_record);
        include_folder_record = false;

        if (writer_config_.write_files) {
            for (const auto& file : files) {
                if (!append_folder_metadata_batch_file(metadata_batch_buffer(states_[partition]->pool,
                                                                            states_[partition]->open_batch),
                                                       file)) {
                    flush_batch(partition);
                    auto& continued_batch = start_folder_batch(partition, folder, include_folder_record);
                    if (!append_folder_metadata_batch_file(continued_batch, file)) {
                        throw std::runtime_error("metadata file record does not fit folder metadata batch buffer");
                    }
                }
            }
        }
        (void)batch;
        flush_batch(partition);
    }

    void close() {
        if (closed_) {
            return;
        }
        closed_ = true;
        for (std::size_t index = 0; index < partitions_; ++index) {
            std::lock_guard<std::mutex> lock(states_[index]->mutex);
            flush_batch(index);
        }
        for (auto& state : states_) {
            state->queue.close();
        }
        std::exception_ptr sender_error;
        for (auto& state : states_) {
            try {
                state->sender.wait();
            } catch (...) {
                if (!sender_error) {
                    sender_error = std::current_exception();
                }
            }
        }

        bool child_failed = false;
        for (const pid_t child : children_) {
            int status = 0;
            if (::waitpid(child, &status, 0) < 0) {
                throw std::runtime_error("failed waiting for metadata writer partition process");
            }
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                child_failed = true;
            }
        }
        if (child_failed) {
            throw std::runtime_error("one or more metadata writer partition processes failed");
        }
        if (sender_error) {
            std::rethrow_exception(sender_error);
        }
        for (std::size_t index = 0; index < partitions_; ++index) {
            const auto [files, folders] = read_metadata_partition_report(
                metadata_partition_report_path(output_path_, index));
            files_written_ += files;
            folders_written_ += folders;
            std::filesystem::remove(metadata_partition_report_path(output_path_, index));
            std::filesystem::remove(metadata_partition_socket_path(output_path_, index));
        }
    }

    [[nodiscard]] std::uint64_t files_written() const {
        return files_written_;
    }

    [[nodiscard]] std::uint64_t folders_written() const {
        return folders_written_;
    }

private:
    struct PartitionSendState {
        RawBufferPool pool;
        BufferPoolRegistry registry;
        BufQueue queue;
        BufferSenderJob sender;
        BufferHandle open_batch;
        bool has_open_batch = false;
        std::mutex mutex;

        PartitionSendState(std::size_t pool_slots, const std::filesystem::path& socket_path)
            : pool(kMetadataBatchBufferPoolId,
                   pool_slots,
                   sizeof(MetadataBatchBuffer),
                   alignof(MetadataBatchBuffer)),
              queue(pool_slots),
              sender(1U,
                     queue,
                     registry,
                     BufferTransportEndpoint::unix_socket(socket_path),
                     metadata_transport_payload_bytes) {
            registry.register_pool(pool);
        }
    };

    MetadataBatchBuffer& acquire_batch(std::size_t partition) {
        PartitionSendState& state = *states_[partition];
        if (!state.has_open_batch) {
            state.open_batch = state.pool.acquire_spin();
            reset_metadata_batch(metadata_batch_buffer(state.pool, state.open_batch));
            state.has_open_batch = true;
        }
        return metadata_batch_buffer(state.pool, state.open_batch);
    }

    MetadataBatchBuffer& start_folder_batch(std::size_t partition,
                                            const MetadataFolderRecord& folder,
                                            bool include_folder_record) {
        PartitionSendState& state = *states_[partition];
        if (state.has_open_batch) {
            throw std::runtime_error("cannot start folder metadata batch while another batch is open");
        }
        state.open_batch = state.pool.acquire_spin();
        if (!reset_folder_metadata_batch(metadata_batch_buffer(state.pool, state.open_batch),
                                         folder,
                                         include_folder_record)) {
            state.pool.release(state.open_batch);
            state.has_open_batch = false;
            throw std::runtime_error("metadata folder path does not fit metadata batch buffer");
        }
        state.has_open_batch = true;
        return metadata_batch_buffer(state.pool, state.open_batch);
    }

    void flush_batch(std::size_t partition) {
        PartitionSendState& state = *states_[partition];
        if (!state.has_open_batch) {
            return;
        }
        BufferHandle handle = state.open_batch;
        if (metadata_batch_buffer(state.pool, handle).record_count == 0U) {
            state.pool.release(handle);
        } else if (!state.queue.push_wait(handle)) {
            state.pool.release(handle);
            throw std::runtime_error("metadata sender queue closed while pushing scan batch");
        }
        state.has_open_batch = false;
    }

    MetadataRecordWriterConfig writer_config_;
    std::filesystem::path output_path_;
    std::size_t partitions_;
    std::vector<std::unique_ptr<PartitionSendState>> states_;
    std::vector<pid_t> children_;
    std::uint64_t files_written_ = 0;
    std::uint64_t folders_written_ = 0;
    bool closed_ = false;
};

struct DataReadBenchmarkSnapshot {
    std::size_t files_found = 0;
    std::size_t folders_found = 0;
    std::size_t files_read = 0;
    std::size_t files_failed = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t bytes_read = 0;
    std::size_t data_buffer_slots = 0;
    std::size_t data_queue_depth = 0;
    double elapsed_seconds = 0.0;
    double bytes_per_second = 0.0;
    double gigabits_per_second = 0.0;
};

struct DataReadBenchmarkStats {
    std::atomic<std::size_t> files_found {0};
    std::atomic<std::size_t> folders_found {0};
    std::atomic<std::size_t> files_read {0};
    std::atomic<std::size_t> files_failed {0};
    std::atomic<std::uint64_t> logical_size_bytes {0};
    std::atomic<std::uint64_t> bytes_read {0};
    std::uint32_t print_interval_seconds = 5;
    std::chrono::steady_clock::time_point started_at {};
    std::chrono::steady_clock::time_point last_print_at {};
    std::mutex print_mutex;
    std::mutex printer_mutex;
    std::condition_variable printer_cv;
    std::atomic<bool> printer_done {false};
};

struct HashInventorySnapshot {
    std::size_t files_found = 0;
    std::size_t folders_found = 0;
    std::size_t files_hashed = 0;
    std::size_t files_failed = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t bytes_read = 0;
    std::uint64_t bytes_hashed = 0;
    double elapsed_seconds = 0.0;
    double read_elapsed_seconds = 0.0;
    double read_bytes_per_second = 0.0;
    double read_gigabits_per_second = 0.0;
    double bytes_per_second = 0.0;
};

enum class HashMode {
    file,
    blocks,
};

struct HashInventoryStats {
    std::atomic<std::size_t> files_found {0};
    std::atomic<std::size_t> folders_found {0};
    std::atomic<std::size_t> files_hashed {0};
    std::atomic<std::size_t> files_failed {0};
    std::atomic<std::uint64_t> logical_size_bytes {0};
    std::atomic<std::uint64_t> bytes_read {0};
    std::atomic<std::uint64_t> bytes_hashed {0};
    std::atomic<std::uint64_t> read_elapsed_nanoseconds {0};
    std::chrono::steady_clock::time_point started_at {};
};

enum class HashChunkKind {
    begin,
    data,
    end,
    failed,
};

struct HashChunkMessage {
    HashChunkKind kind = HashChunkKind::data;
    std::uint64_t file_id = 0;
    std::uint64_t block_index = 0;
    std::uint64_t offset = 0;
    std::uint64_t data_len = 0;
    FileSpec file;
    DataSlotHandle handle;
    bool has_data_slot = false;
};

struct HashChunkShardQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    std::deque<HashChunkMessage> messages;
    std::size_t max_entries = 1;
    std::size_t queued_bytes = 0;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr error;
};

struct ActiveHashFile {
    FileSpec file;
    Md5State md5;
    Sha256State sha256;
    Hash64State xxh64;
    Xxh3_64State xxh3_64;
    Xxh3_128State xxh3_128;
    std::uint64_t bytes_hashed = 0;
    std::uint64_t next_data_offset = 0;
    std::map<std::uint64_t, DataSlotHandle> pending_data_slots;
    std::vector<std::string> block_hashes;
    std::uint64_t next_block_index = 0;
    std::uint64_t current_block_bytes = 0;
    std::uint64_t current_block_expected = 0;
};

struct HashSpeedWorkerResult {
    std::uint64_t bytes_hashed = 0;
    std::uint64_t iterations = 0;
    std::uint64_t digest_mix = 0;
};

void append_u32(std::string& out, std::uint32_t value) {
    out.push_back(static_cast<char>((value >> 24U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 16U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 8U) & 0xFFU));
    out.push_back(static_cast<char>(value & 0xFFU));
}

void append_u64(std::string& out, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xFFU));
    }
}

void append_bool(std::string& out, bool value) {
    out.push_back(value ? '\x01' : '\x00');
}

void write_u32_be(char* out, std::size_t& offset, std::uint32_t value) {
    out[offset++] = static_cast<char>((value >> 24U) & 0xFFU);
    out[offset++] = static_cast<char>((value >> 16U) & 0xFFU);
    out[offset++] = static_cast<char>((value >> 8U) & 0xFFU);
    out[offset++] = static_cast<char>(value & 0xFFU);
}

void write_u64_be(char* out, std::size_t& offset, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out[offset++] = static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xFFU);
    }
}

void write_u32_be_at(char* out, std::size_t offset, std::uint32_t value) {
    out[offset++] = static_cast<char>((value >> 24U) & 0xFFU);
    out[offset++] = static_cast<char>((value >> 16U) & 0xFFU);
    out[offset++] = static_cast<char>((value >> 8U) & 0xFFU);
    out[offset] = static_cast<char>(value & 0xFFU);
}

void append_string(std::string& out, std::string_view value) {
    append_u32(out, static_cast<std::uint32_t>(value.size()));
    out.append(value);
}

std::uint32_t read_u32(std::string_view input, std::size_t& offset) {
    if (offset + 4U > input.size()) {
        throw std::runtime_error("protocol underflow reading u32");
    }
    const auto* bytes = reinterpret_cast<const unsigned char*>(input.data() + offset);
    offset += 4U;
    return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
           (static_cast<std::uint32_t>(bytes[1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[2]) << 8U) |
           static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t read_u64(std::string_view input, std::size_t& offset) {
    if (offset + 8U > input.size()) {
        throw std::runtime_error("protocol underflow reading u64");
    }
    const auto* bytes = reinterpret_cast<const unsigned char*>(input.data() + offset);
    offset += 8U;
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8U) | static_cast<std::uint64_t>(bytes[i]);
    }
    return value;
}

bool read_bool(std::string_view input, std::size_t& offset) {
    if (offset + 1U > input.size()) {
        throw std::runtime_error("protocol underflow reading bool");
    }
    return input[offset++] != '\0';
}

template <std::size_t Capacity>
void read_fixed_string(std::string_view input, std::size_t& offset, FixedString<Capacity>& value) {
    const std::uint32_t length = read_u32(input, offset);
    if (length > Capacity) {
        throw std::runtime_error("protocol string exceeds fixed buffer capacity");
    }
    if (offset + length > input.size()) {
        throw std::runtime_error("protocol underflow reading fixed string");
    }
    value.assign(input.substr(offset, length));
    offset += length;
}

void send_priority_payload(int fd, PriorityMessageType type, const std::string& payload) {
    std::string header;
    header.reserve(8);
    append_u32(header, static_cast<std::uint32_t>(type));
    append_u32(header, static_cast<std::uint32_t>(payload.size()));
    write_all(fd, header.data(), header.size());
    if (!payload.empty()) {
        write_all(fd, payload.data(), payload.size());
    }
}

bool read_priority_payload(int fd, PriorityMessageType& type, std::string& payload) {
    std::array<char, 8> header {};
    if (!read_exact_or_eof(fd, header.data(), header.size())) {
        return false;
    }
    std::size_t offset = 0;
    const std::string_view header_view(header.data(), header.size());
    type = static_cast<PriorityMessageType>(read_u32(header_view, offset));
    const std::uint32_t payload_size = read_u32(header_view, offset);
    payload.assign(payload_size, '\0');
    if (payload_size != 0U && !read_exact_or_eof(fd, payload.data(), payload.size())) {
        throw std::runtime_error("unexpected EOF while reading priority payload");
    }
    return true;
}

void send_session_start(int fd, const EngineConfig& config) {
    std::string payload;
    append_u32(payload, 1U);
    append_u32(payload, static_cast<std::uint32_t>(config.large_chunk_bytes));
    append_bool(payload, config.skip_verify);
    send_priority_payload(fd, PriorityMessageType::session_start, payload);
}

void send_file_record(int fd, const FileRecordMessage& record) {
    std::string payload;
    append_u64(payload, record.file_id);
    append_u64(payload, record.folder_hash);
    append_u64(payload, record.size);
    append_u64(payload, record.mtime);
    append_u32(payload, record.mode);
    append_u32(payload, record.uid);
    append_u32(payload, record.gid);
    append_string(payload, record.rel_path);
    send_priority_payload(fd, PriorityMessageType::file_record, payload);
}

FileRecordMessage decode_file_record(std::string_view payload) {
    std::size_t offset = 0;
    FileRecordMessage record;
    record.file_id = read_u64(payload, offset);
    record.folder_hash = read_u64(payload, offset);
    record.size = read_u64(payload, offset);
    record.mtime = read_u64(payload, offset);
    record.mode = read_u32(payload, offset);
    record.uid = read_u32(payload, offset);
    record.gid = read_u32(payload, offset);
    read_fixed_string(payload, offset, record.rel_path);
    return record;
}

void send_file_decision(int fd, const FileDecisionMessage& decision) {
    std::string payload;
    append_u64(payload, decision.file_id);
    append_bool(payload, decision.skip);
    send_priority_payload(fd, PriorityMessageType::file_decision, payload);
}

FileDecisionMessage decode_file_decision(std::string_view payload) {
    std::size_t offset = 0;
    FileDecisionMessage decision;
    decision.file_id = read_u64(payload, offset);
    decision.skip = read_bool(payload, offset);
    return decision;
}

void send_directory_record(int fd, const DirectoryRecordMessage& record) {
    std::string payload;
    append_u64(payload, record.mtime);
    append_u32(payload, record.mode);
    append_u32(payload, record.uid);
    append_u32(payload, record.gid);
    append_string(payload, record.rel_path);
    send_priority_payload(fd, PriorityMessageType::directory_record, payload);
}

DirectoryRecordMessage decode_directory_record(std::string_view payload) {
    std::size_t offset = 0;
    DirectoryRecordMessage record;
    record.mtime = read_u64(payload, offset);
    record.mode = read_u32(payload, offset);
    record.uid = read_u32(payload, offset);
    record.gid = read_u32(payload, offset);
    read_fixed_string(payload, offset, record.rel_path);
    return record;
}

void send_file_ack(int fd, const FileAckMessage& ack) {
    std::string payload;
    append_u64(payload, ack.file_id);
    append_bool(payload, ack.hash_verified);
    append_u64(payload, ack.bytes_written);
    send_priority_payload(fd, PriorityMessageType::file_ack, payload);
}

FileAckMessage decode_file_ack(std::string_view payload) {
    std::size_t offset = 0;
    FileAckMessage ack;
    ack.file_id = read_u64(payload, offset);
    ack.hash_verified = read_bool(payload, offset);
    ack.bytes_written = read_u64(payload, offset);
    return ack;
}

void send_session_end(int fd) {
    send_priority_payload(fd, PriorityMessageType::session_end, {});
}

void send_pause(int fd) {
    send_priority_payload(fd, PriorityMessageType::pause, {});
}

void send_resume(int fd) {
    send_priority_payload(fd, PriorityMessageType::resume, {});
}

void send_data_slot(int fd, const DataSlotPool& pool, const DataSlotHandle& handle) {
    const DataBufTrailer& trailer = pool.trailer(handle);

    std::string header;
    header.reserve(80);
    append_u64(header, trailer.file_id);
    append_u64(header, trailer.folder_hash);
    append_u64(header, trailer.data_offset);
    append_u64(header, trailer.data_len);
    append_u64(header, trailer.file_size);
    append_u64(header, trailer.data_hash);
    append_u64(header, trailer.mtime);
    append_u32(header, trailer.mode);
    append_u32(header, trailer.uid);
    append_u32(header, trailer.gid);
    append_u32(header, trailer.chunk_hash);
    append_u32(header, trailer.flags);
    append_u32(header, static_cast<std::uint32_t>(trailer.rel_path.size()));
    write_all(fd, header.data(), header.size());
    if (!trailer.rel_path.empty()) {
        write_all(fd, trailer.rel_path.data(), trailer.rel_path.size());
    }
    if (trailer.data_len != 0U) {
        write_all(fd, pool.data(handle), static_cast<std::size_t>(trailer.data_len));
    }
}

bool read_data_slot(int fd, DataSlotPool& pool, DataSlotHandle& handle) {
    std::array<char, 80> header {};
    if (!read_exact_or_eof(fd, header.data(), header.size())) {
        return false;
    }

    std::size_t offset = 0;
    const std::string_view header_view(header.data(), header.size());
    const std::uint64_t file_id = read_u64(header_view, offset);
    const std::uint64_t folder_hash = read_u64(header_view, offset);
    const std::uint64_t data_offset = read_u64(header_view, offset);
    const std::uint64_t data_len = read_u64(header_view, offset);
    const std::uint64_t file_size = read_u64(header_view, offset);
    const std::uint64_t data_hash = read_u64(header_view, offset);
    const std::uint64_t mtime = read_u64(header_view, offset);
    const std::uint32_t mode = read_u32(header_view, offset);
    const std::uint32_t uid = read_u32(header_view, offset);
    const std::uint32_t gid = read_u32(header_view, offset);
    const std::uint32_t chunk_hash = read_u32(header_view, offset);
    const std::uint32_t flags = read_u32(header_view, offset);
    const std::uint32_t path_length = read_u32(header_view, offset);

    const DataSlotClass slot_class = (flags & kFlagSmallFile) != 0U ? DataSlotClass::small : DataSlotClass::large;
    handle = pool.acquire_wait_or_throw(slot_class, static_cast<std::size_t>(data_len));
    DataBufTrailer& trailer = pool.trailer(handle);
    trailer.file_id = file_id;
    trailer.folder_hash = folder_hash;
    trailer.data_offset = data_offset;
    trailer.data_len = data_len;
    trailer.file_size = file_size;
    trailer.data_hash = data_hash;
    trailer.mtime = mtime;
    trailer.mode = mode;
    trailer.uid = uid;
    trailer.gid = gid;
    trailer.chunk_hash = chunk_hash;
    trailer.flags = flags;
    trailer.rel_path.resize_for_overwrite(path_length);
    trailer.slot_valid = kSlotValid;

    if (path_length != 0U && !read_exact_or_eof(fd, trailer.rel_path.data(), path_length)) {
        pool.release(handle);
        throw std::runtime_error("unexpected EOF while reading chunk path");
    }
    if (data_len != 0U &&
        !read_exact_or_eof(fd, pool.data(handle), static_cast<std::size_t>(data_len))) {
        pool.release(handle);
        throw std::runtime_error("unexpected EOF while reading chunk payload");
    }
    return true;
}

struct PackedSmallFileEntry {
    FileRecordMessage record;
    std::uint64_t data_hash = 0;
    std::string_view data;
};

constexpr std::size_t kPackedSmallFileCountBytes = 4;
constexpr std::size_t kPackedSmallFileEntryFixedBytes =
    8 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4;

bool is_packed_small_file_slot(const DataSlotPool& pool, const DataSlotHandle& handle) {
    return (pool.trailer(handle).flags & kFlagPackedSmallFiles) != 0U;
}

std::size_t packed_small_file_entry_bytes(const PreparedTransfer& prepared) {
    return kPackedSmallFileEntryFixedBytes +
           prepared.record.rel_path.size() +
           static_cast<std::size_t>(prepared.record.size);
}

bool can_pack_small_file(const PreparedTransfer& prepared, const EngineConfig& config) {
    return !prepared.cached &&
           prepared.record.size <= config.small_file_threshold &&
           packed_small_file_entry_bytes(prepared) + kPackedSmallFileCountBytes <= config.large_chunk_bytes;
}

std::vector<std::uint64_t> packed_small_file_ids(const DataSlotPool& pool, const DataSlotHandle& handle) {
    const DataBufTrailer& trailer = pool.trailer(handle);
    std::string_view payload(pool.data(handle), static_cast<std::size_t>(trailer.data_len));
    std::size_t offset = 0;
    const std::uint32_t count = read_u32(payload, offset);
    std::vector<std::uint64_t> ids;
    ids.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index) {
        const std::uint64_t file_id = read_u64(payload, offset);
        const std::uint64_t data_len = read_u64(payload, offset);
        (void)read_u64(payload, offset);
        (void)read_u64(payload, offset);
        (void)read_u64(payload, offset);
        (void)read_u32(payload, offset);
        (void)read_u32(payload, offset);
        (void)read_u32(payload, offset);
        const std::uint32_t path_len = read_u32(payload, offset);
        if (offset + path_len + data_len > payload.size()) {
            throw std::runtime_error("packed small file payload is truncated");
        }
        offset += path_len + static_cast<std::size_t>(data_len);
        ids.push_back(file_id);
    }
    if (offset != payload.size()) {
        throw std::runtime_error("packed small file payload has trailing bytes");
    }
    return ids;
}

std::vector<PackedSmallFileEntry> decode_packed_small_file_entries(const DataSlotPool& pool,
                                                                   const DataSlotHandle& handle) {
    const DataBufTrailer& trailer = pool.trailer(handle);
    std::string_view payload(pool.data(handle), static_cast<std::size_t>(trailer.data_len));
    std::size_t offset = 0;
    const std::uint32_t count = read_u32(payload, offset);
    std::vector<PackedSmallFileEntry> entries;
    entries.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index) {
        PackedSmallFileEntry entry;
        entry.record.file_id = read_u64(payload, offset);
        const std::uint64_t data_len = read_u64(payload, offset);
        entry.record.size = read_u64(payload, offset);
        entry.data_hash = read_u64(payload, offset);
        entry.record.mtime = read_u64(payload, offset);
        entry.record.mode = read_u32(payload, offset);
        entry.record.uid = read_u32(payload, offset);
        entry.record.gid = read_u32(payload, offset);
        const std::uint32_t path_len = read_u32(payload, offset);
        if (path_len > entry.record.rel_path.capacity()) {
            throw std::runtime_error("packed small file path exceeds protocol capacity");
        }
        if (offset + path_len + data_len > payload.size()) {
            throw std::runtime_error("packed small file payload is truncated");
        }
        entry.record.rel_path.assign(payload.substr(offset, path_len));
        offset += path_len;
        entry.data = payload.substr(offset, static_cast<std::size_t>(data_len));
        offset += static_cast<std::size_t>(data_len);
        entries.push_back(entry);
    }
    if (offset != payload.size()) {
        throw std::runtime_error("packed small file payload has trailing bytes");
    }
    return entries;
}

std::size_t path_depth(std::string_view rel_path) {
    if (rel_path.empty()) {
        return 0;
    }
    return 1U + static_cast<std::size_t>(std::count(rel_path.begin(), rel_path.end(), '/'));
}

bool target_matches_record(const NfsBackend& backend, const FileRecordMessage& record) {
    if (!backend.metadata_matches(record.rel_path, record.size, record.mtime)) {
        return false;
    }

    const auto existing = backend.stat_path(record.rel_path);
    if (!existing.has_value()) {
        return false;
    }
    return existing->mode == record.mode && existing->uid == record.uid && existing->gid == record.gid;
}

FileRecordMessage make_file_record_message(const FileSpec& file) {
    const RecBuf record = make_recbuf(file);
    FileRecordMessage message;
    message.file_id = record.own_hash;
    message.folder_hash = record.folder_hash;
    message.rel_path = record.rel_path;
    message.size = record.size;
    message.mtime = record.mtime;
    message.mode = record.mode;
    message.uid = record.uid;
    message.gid = record.gid;
    return message;
}

DirectoryRecordMessage make_directory_record_message(const FileSpec& directory) {
    DirectoryRecordMessage message;
    message.rel_path = normalize_path(directory.rel_path);
    message.mtime = directory.mtime;
    message.mode = directory.mode;
    message.uid = directory.uid;
    message.gid = directory.gid;
    return message;
}

FileSpec make_directory_spec(const DirectoryRecordMessage& record) {
    FileSpec spec;
    spec.rel_path = normalize_path(record.rel_path.view());
    spec.mtime = record.mtime;
    spec.mode = record.mode;
    spec.uid = record.uid;
    spec.gid = record.gid;
    return spec;
}

struct SessionStartInfo {
    std::uint64_t large_chunk_bytes = kLargeChunkBytes;
    bool skip_verify = false;
};

SessionStartInfo decode_session_start(std::string_view payload) {
    std::size_t offset = 0;
    (void)read_u32(payload, offset);
    SessionStartInfo info;
    info.large_chunk_bytes = read_u32(payload, offset);
    info.skip_verify = read_bool(payload, offset);
    return info;
}

double percentage_of(std::uint64_t value, std::uint64_t total) {
    if (total == 0) {
        return 0.0;
    }
    return static_cast<double>(value) * 100.0 / static_cast<double>(total);
}

std::uint64_t receiver_queue_capacity_bytes(const ReceiverSharedState& state) {
    if (state.runtime.backpressure_window_bytes != 0) {
        return state.runtime.backpressure_window_bytes;
    }
    return std::max<std::uint64_t>(state.session_large_chunk_bytes * 8U, kLargeChunkBytes * 8U);
}

std::string json_quote(std::string_view value) {
    std::string out;
    out.reserve(value.size() + 2U);
    out.push_back('"');
    for (const char ch : value) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    out.push_back('"');
    return out;
}

std::string utc_time_string(std::chrono::system_clock::time_point time_point) {
    const std::time_t seconds = std::chrono::system_clock::to_time_t(time_point);
    std::tm tm {};
#if defined(_WIN32)
    gmtime_s(&tm, &seconds);
#else
    gmtime_r(&seconds, &tm);
#endif
    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return out.str();
}

std::uint64_t unix_time_nanoseconds(std::chrono::system_clock::time_point time_point) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(time_point.time_since_epoch()).count());
}

MetadataScanRunInfo make_metadata_scan_run_info(const std::filesystem::path& source_root,
                                                bool recursive,
                                                const std::string& output_format,
                                                const std::string& metadata_records,
                                                std::size_t meta_reader_threads,
                                                std::size_t metadata_async_depth,
                                                std::size_t record_buffer_slots,
                                                double max_duration_seconds,
                                                std::uint32_t stats_interval_seconds) {
    const auto started = std::chrono::system_clock::now();
    const std::uint64_t started_ns = unix_time_nanoseconds(started);
    MetadataScanRunInfo info;
    info.run_id = std::to_string(started_ns);
    info.started_at_utc = utc_time_string(started);
    info.started_unix_ns = started_ns;
    info.source_root = source_root.string();

    std::ostringstream settings;
    settings << '{'
             << "\"app\":\"hypersync\","
             << "\"version\":" << json_quote(kVersion) << ','
             << "\"mode\":\"metadata_scan\","
             << "\"source\":" << json_quote(source_root.string()) << ','
             << "\"recursive\":" << (recursive ? "true" : "false") << ','
             << "\"output_format\":" << json_quote(output_format) << ','
             << "\"records\":" << json_quote(metadata_records) << ','
             << "\"meta_reader_threads\":" << meta_reader_threads << ','
             << "\"metadata_async_depth\":" << metadata_async_depth << ','
             << "\"record_buffer_slots\":" << record_buffer_slots << ','
             << "\"max_duration_seconds\":" << max_duration_seconds << ','
             << "\"stats_interval_seconds\":" << stats_interval_seconds << '}';
    info.settings_json = settings.str();
    return info;
}

void enqueue_flat_folder_work(FlatMetadataWorkQueue& queue, std::vector<FileSpec>&& folders) {
    if (folders.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.done || (queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at)) {
            queue.done = true;
            return;
        }
        for (auto& folder : folders) {
            queue.folders.push_back(std::move(folder));
        }
    }
    queue.cv.notify_all();
}

void finish_flat_folder_work(FlatMetadataWorkQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0) {
            --queue.active;
        }
        if ((queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at) ||
            (queue.folders.empty() && queue.active == 0)) {
            queue.done = true;
            if (queue.stop_at.has_value()) {
                queue.folders.clear();
            }
        }
    }
    queue.cv.notify_all();
}

void fail_flat_folder_work(FlatMetadataWorkQueue& queue, std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0) {
            --queue.active;
        }
        queue.done = true;
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv.notify_all();
}

void request_flat_folder_stop(FlatMetadataWorkQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.done = true;
        queue.folders.clear();
    }
    queue.cv.notify_all();
}

bool flat_metadata_scan_should_stop(const FlatMetadataWorkQueue& queue) {
    return queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at;
}

std::optional<FileSpec> take_flat_folder_work(FlatMetadataWorkQueue& queue, bool wait_for_work) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.done || queue.error || !queue.folders.empty() ||
               (queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at);
    };

    if (wait_for_work) {
        if (queue.stop_at.has_value()) {
            queue.cv.wait_until(lock, *queue.stop_at, ready);
        } else {
            queue.cv.wait(lock, ready);
        }
    } else if (!ready()) {
        return std::nullopt;
    }

    if (queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at) {
        queue.done = true;
        queue.folders.clear();
    }
    if (queue.error || queue.done || queue.folders.empty()) {
        return std::nullopt;
    }

    FileSpec folder = std::move(queue.folders.front());
    queue.folders.pop_front();
    ++queue.active;
    return folder;
}

bool enqueue_diff_target_folder(DiffTargetFolderQueue& queue, FileSpec folder) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv.wait(lock, [&queue]() {
        return queue.done || queue.error || queue.folders.size() < queue.max_entries;
    });
    if (queue.done || queue.error) {
        queue.cv.notify_all();
        return false;
    }
    queue.folders.push_back(std::move(folder));
    lock.unlock();
    queue.cv.notify_all();
    return true;
}

bool enqueue_diff_target_folders(DiffTargetFolderQueue& queue, std::vector<FileSpec>&& folders) {
    for (auto& folder : folders) {
        if (!enqueue_diff_target_folder(queue, std::move(folder))) {
            return false;
        }
    }
    return true;
}

std::optional<FileSpec> take_diff_target_folder_work(DiffTargetFolderQueue& queue, bool wait_for_work) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.done || queue.error || !queue.folders.empty() || queue.input_done;
    };
    if (wait_for_work) {
        queue.cv.wait(lock, ready);
    } else if (!ready()) {
        return std::nullopt;
    }
    if (queue.error || queue.done || queue.folders.empty()) {
        return std::nullopt;
    }

    FileSpec folder = std::move(queue.folders.front());
    queue.folders.pop_front();
    ++queue.active;
    lock.unlock();
    queue.cv.notify_all();
    return folder;
}

void finish_diff_target_folder_work(DiffTargetFolderQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0) {
            --queue.active;
        }
        if (queue.input_done && queue.folders.empty() && queue.active == 0) {
            queue.done = true;
        }
    }
    queue.cv.notify_all();
}

void mark_diff_target_input_done(DiffTargetFolderQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.input_done = true;
        if (queue.folders.empty() && queue.active == 0) {
            queue.done = true;
        }
    }
    queue.cv.notify_all();
}

void fail_diff_target_folder_work(DiffTargetFolderQueue& queue,
                                  std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0) {
            --queue.active;
        }
        queue.done = true;
        queue.folders.clear();
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv.notify_all();
}

DataReadBenchmarkSnapshot snapshot_data_read_stats(const DataReadBenchmarkStats& stats) {
    const auto now = std::chrono::steady_clock::now();
    const double elapsed = stats.started_at == std::chrono::steady_clock::time_point{}
                               ? 0.0
                               : std::chrono::duration<double>(now - stats.started_at).count();
    DataReadBenchmarkSnapshot snapshot;
    snapshot.files_found = stats.files_found.load(std::memory_order_relaxed);
    snapshot.folders_found = stats.folders_found.load(std::memory_order_relaxed);
    snapshot.files_read = stats.files_read.load(std::memory_order_relaxed);
    snapshot.files_failed = stats.files_failed.load(std::memory_order_relaxed);
    snapshot.logical_size_bytes = stats.logical_size_bytes.load(std::memory_order_relaxed);
    snapshot.bytes_read = stats.bytes_read.load(std::memory_order_relaxed);
    snapshot.elapsed_seconds = elapsed;
    snapshot.bytes_per_second = elapsed > 0.0 ? static_cast<double>(snapshot.bytes_read) / elapsed : 0.0;
    snapshot.gigabits_per_second = snapshot.bytes_per_second * 8.0 / 1'000'000'000.0;
    return snapshot;
}

bool parse_data_copy_mode(std::string_view value) {
    if (value.empty() || value == "copy" || value == "true" || value == "on" || value == "yes") {
        return true;
    }
    if (value == "no-copy" || value == "nocopy" || value == "false" || value == "off" || value == "no") {
        return false;
    }
    throw std::runtime_error("data copy mode must be copy or no-copy");
}

std::string data_copy_mode_name(bool copy_payload_to_buffer) {
    return copy_payload_to_buffer ? "copy" : "no-copy";
}

MonitorQueueSnapshot monitor_buf_queue(std::string name, const BufQueue& queue) {
    MonitorQueueSnapshot snapshot;
    snapshot.name = std::move(name);
    snapshot.capacity = queue.capacity();
    snapshot.depth = queue.size();
    snapshot.high_watermark = queue.high_watermark();
    snapshot.pushed = queue.push_count();
    snapshot.popped = queue.pop_count();
    snapshot.closed = queue.closed();
    return snapshot;
}

MonitorQueueSnapshot monitor_flat_folder_queue(std::string name, FlatMetadataWorkQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    MonitorQueueSnapshot snapshot;
    snapshot.name = std::move(name);
    snapshot.capacity = 0;
    snapshot.depth = queue.folders.size();
    snapshot.high_watermark = queue.folders.size();
    snapshot.closed = queue.done;
    snapshot.detail = "active=" + std::to_string(queue.active);
    return snapshot;
}

MonitorQueueSnapshot monitor_data_file_queue(std::string name, DataReadFileQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    MonitorQueueSnapshot snapshot;
    snapshot.name = std::move(name);
    snapshot.capacity = queue.max_entries;
    snapshot.depth = queue.files.size();
    snapshot.high_watermark = queue.files.size();
    snapshot.closed = queue.input_done || queue.stop;
    return snapshot;
}

std::uint64_t file_spec_logical_size(const FileSpec& file) {
    return file.declared_size != 0 ? file.declared_size : file.content.size();
}

bool diff_file_specs_match(const FileSpec& source, const FileSpec& target, const std::string& compare_mode) {
    const std::uint64_t source_size = file_spec_logical_size(source);
    const std::uint64_t target_size = file_spec_logical_size(target);
    if (compare_mode == "size") {
        return source_size == target_size;
    }
    if (compare_mode == "time") {
        return source_size == target_size && source.mtime == target.mtime;
    }
    if (!source.content_hash.empty() && !target.content_hash.empty() &&
        (source.hash_algorithm.empty() || target.hash_algorithm.empty() ||
         source.hash_algorithm == target.hash_algorithm)) {
        return source_size == target_size && source.content_hash == target.content_hash;
    }
    return source_size == target_size && source.mtime == target.mtime;
}

std::string diff_csv_quote(std::string_view value) {
    if (value.find_first_of(",\"\r\n") == std::string_view::npos) {
        return std::string(value);
    }
    std::string quoted = "\"";
    for (char ch : value) {
        if (ch == '"') {
            quoted += "\"\"";
        } else {
            quoted.push_back(ch);
        }
    }
    quoted.push_back('"');
    return quoted;
}

void append_diff_csv_row(std::string& csv,
                         std::string_view rel_path,
                         DiffKind decision,
                         std::uint64_t source_size,
                         std::uint64_t source_mtime,
                         std::uint64_t target_size,
                         std::uint64_t target_mtime) {
    csv += diff_csv_quote(rel_path) + "," + to_string(decision) + "," +
           std::to_string(source_size) + "," + std::to_string(source_mtime) + "," +
           std::to_string(target_size) + "," + std::to_string(target_mtime) + "\n";
}

FolderRecord make_done_folder_record(const FileSpec& folder,
                                      std::uint64_t flat_file_count,
                                      std::uint64_t flat_logical_size_bytes) {
    FolderRecord record;
    record.rel_path = normalize_path(folder.rel_path);
    record.files_discovered = flat_file_count;
    record.files_total = flat_file_count;
    record.files_completed = flat_file_count;
    record.flat_size_bytes = flat_logical_size_bytes;
    record.state = FolderState::done;
    record.remote_state = FolderState::done;
    return record;
}

struct DiffBatchResults {
    std::map<std::string, FileOutcome> files;
    std::map<std::string, FolderRecord> folders;
    std::string csv_rows;
    std::size_t files_total = 0;
    std::size_t folders_total = 0;
    std::size_t files_skipped = 0;
    std::size_t files_changed = 0;
    std::size_t files_new = 0;
    std::size_t files_target_only = 0;
    std::size_t files_failed = 0;
    std::uint64_t bytes_planned = 0;
};

void record_diff_file(DiffBatchResults& results, FileOutcome outcome, bool collect_detailed_records) {
    ++results.files_total;
    switch (outcome.diff) {
        case DiffKind::skip:
            ++results.files_skipped;
            break;
        case DiffKind::new_file:
            ++results.files_new;
            break;
        case DiffKind::changed:
            ++results.files_changed;
            break;
        case DiffKind::target_only:
            ++results.files_target_only;
            break;
        case DiffKind::failed:
            ++results.files_failed;
            break;
    }
    if (collect_detailed_records) {
        results.files[outcome.rel_path] = std::move(outcome);
    }
}

void record_diff_folder(DiffBatchResults& results, FolderRecord record, bool collect_detailed_records) {
    ++results.folders_total;
    if (collect_detailed_records) {
        results.folders[record.rel_path] = std::move(record);
    }
}

void merge_live_diff_results(TransferReport& report,
                             DiffBatchResults&& results,
                             bool collect_detailed_records,
                             std::mutex& report_mutex) {
    std::lock_guard<std::mutex> lock(report_mutex);
    report.files_total += results.files_total;
    report.folders_total += results.folders_total;
    report.files_skipped += results.files_skipped;
    report.files_changed += results.files_changed;
    report.files_new += results.files_new;
    report.files_target_only += results.files_target_only;
    report.files_failed += results.files_failed;
    report.bytes_planned += results.bytes_planned;
    if (collect_detailed_records) {
        report.files.insert(results.files.begin(), results.files.end());
        report.folders.insert(results.folders.begin(), results.folders.end());
        report.diff_csv += results.csv_rows;
    }
}

void add_target_only_tree(NfsBackend& target_backend,
                          const FileSpec& target_folder,
                          FlatMetadataWorkQueue& queue,
                          TransferReport& report,
                          std::mutex& report_mutex,
                          bool collect_detailed_records) {
    if (flat_metadata_scan_should_stop(queue)) {
        return;
    }

    DiffBatchResults results;
    FileSpec normalized_target_folder = target_folder;
    normalized_target_folder.rel_path = normalize_path(normalized_target_folder.rel_path);
    record_diff_folder(results, make_done_folder_record(normalized_target_folder, 0, 0), collect_detailed_records);

    try {
        target_backend.visit_metadata_at(
            target_folder.rel_path,
            true,
            [&](FileSpec file) {
                if (flat_metadata_scan_should_stop(queue)) {
                    return;
                }
                file.rel_path = normalize_path(file.rel_path);
                FileOutcome outcome;
                outcome.rel_path = file.rel_path;
                outcome.diff = DiffKind::target_only;
                outcome.size = file_spec_logical_size(file);
                if (collect_detailed_records) {
                    append_diff_csv_row(results.csv_rows,
                                        outcome.rel_path,
                                        DiffKind::target_only,
                                        0,
                                        0,
                                        outcome.size,
                                        file.mtime);
                }
                record_diff_file(results, std::move(outcome), collect_detailed_records);
            },
            [&](FileSpec directory) {
                if (flat_metadata_scan_should_stop(queue)) {
                    return;
                }
                directory.rel_path = normalize_path(directory.rel_path);
                record_diff_folder(results, make_done_folder_record(directory, 0, 0), collect_detailed_records);
            });
    } catch (const std::exception& ex) {
        FileOutcome outcome;
        outcome.rel_path = normalize_path(target_folder.rel_path);
        outcome.diff = DiffKind::failed;
        const std::string failed_path = outcome.rel_path;
        if (collect_detailed_records) {
            append_diff_csv_row(results.csv_rows, outcome.rel_path, DiffKind::failed, 0, 0, 0, 0);
        }
        record_diff_file(results, std::move(outcome), collect_detailed_records);
        std::cerr << "diff skipped target-only subtree '"
                  << (failed_path.empty() ? "/" : failed_path)
                  << "': " << ex.what() << '\n';
    }

    merge_live_diff_results(report, std::move(results), collect_detailed_records, report_mutex);
}

void record_live_diff_batch(bool recursive,
                            const std::string& compare_mode,
                            FlatMetadataWorkQueue& queue,
                            NfsBackend& target_backend,
                            TransferReport& report,
                            std::mutex& report_mutex,
                            bool collect_detailed_records,
                            FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "diff skipped source folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message = batch.error.empty() ? "failed to scan source root" : batch.error;
            fail_flat_folder_work(queue, std::make_exception_ptr(std::runtime_error(message)));
            return;
        }
        finish_flat_folder_work(queue);
        return;
    }

    const std::string folder_path = normalize_path(batch.folder.rel_path);
    std::uint64_t flat_logical_size = 0;
    std::vector<FileSpec> child_work;
    std::unordered_set<std::string> source_directories;
    if (recursive && !flat_metadata_scan_should_stop(queue)) {
        child_work.reserve(batch.directories.size());
    }
    for (auto& directory : batch.directories) {
        directory.rel_path = normalize_path(directory.rel_path);
        source_directories.insert(directory.rel_path);
        if (recursive && !flat_metadata_scan_should_stop(queue)) {
            child_work.push_back(directory);
        }
    }
    if (flat_metadata_scan_should_stop(queue)) {
        finish_flat_folder_work(queue);
        return;
    }

    std::vector<FileSpec> target_files;
    std::vector<FileSpec> target_directories;
    try {
        target_backend.visit_metadata_at(
            folder_path,
            false,
            [&target_files](FileSpec file) {
                file.rel_path = normalize_path(file.rel_path);
                target_files.push_back(std::move(file));
            },
            [&target_directories](FileSpec directory) {
                directory.rel_path = normalize_path(directory.rel_path);
                target_directories.push_back(std::move(directory));
            });
    } catch (const std::exception& ex) {
        std::cerr << "diff treats target folder '"
                  << (folder_path.empty() ? "/" : folder_path)
                  << "' as empty: " << ex.what() << '\n';
    }

    std::unordered_map<std::string, FileSpec> target_by_path;
    target_by_path.reserve(target_files.size());
    for (auto& file : target_files) {
        target_by_path.emplace(file.rel_path, std::move(file));
    }

    DiffBatchResults results;

    for (auto& source : batch.files) {
        source.rel_path = normalize_path(source.rel_path);
        const std::uint64_t source_size = file_spec_logical_size(source);
        flat_logical_size += source_size;
        FileOutcome outcome;
        outcome.rel_path = source.rel_path;
        outcome.size = source_size;

        const auto target_it = target_by_path.find(source.rel_path);
        if (target_it == target_by_path.end()) {
            outcome.diff = DiffKind::new_file;
            results.bytes_planned += source_size;
            if (collect_detailed_records) {
                append_diff_csv_row(results.csv_rows, source.rel_path, outcome.diff, source_size, source.mtime, 0, 0);
            }
        } else if (diff_file_specs_match(source, target_it->second, compare_mode)) {
            outcome.diff = DiffKind::skip;
            if (collect_detailed_records) {
                append_diff_csv_row(results.csv_rows,
                                    source.rel_path,
                                    outcome.diff,
                                    source_size,
                                    source.mtime,
                                    file_spec_logical_size(target_it->second),
                                    target_it->second.mtime);
            }
        } else {
            outcome.diff = DiffKind::changed;
            results.bytes_planned += source_size;
            if (collect_detailed_records) {
                append_diff_csv_row(results.csv_rows,
                                    source.rel_path,
                                    outcome.diff,
                                    source_size,
                                    source.mtime,
                                    file_spec_logical_size(target_it->second),
                                    target_it->second.mtime);
            }
        }

        record_diff_file(results, std::move(outcome), collect_detailed_records);
        if (target_it != target_by_path.end()) {
            target_by_path.erase(target_it);
        }
    }

    for (const auto& [path, target] : target_by_path) {
        FileOutcome outcome;
        outcome.rel_path = path;
        outcome.diff = DiffKind::target_only;
        outcome.size = file_spec_logical_size(target);
        if (collect_detailed_records) {
            append_diff_csv_row(results.csv_rows, path, outcome.diff, 0, 0, outcome.size, target.mtime);
        }
        record_diff_file(results, std::move(outcome), collect_detailed_records);
    }

    FileSpec normalized_folder = batch.folder;
    normalized_folder.rel_path = folder_path;
    record_diff_folder(results,
                       make_done_folder_record(normalized_folder, batch.files.size(), flat_logical_size),
                       collect_detailed_records);

    merge_live_diff_results(report, std::move(results), collect_detailed_records, report_mutex);

    if (recursive && !flat_metadata_scan_should_stop(queue)) {
        for (const auto& target_directory : target_directories) {
            if (source_directories.find(target_directory.rel_path) == source_directories.end()) {
                add_target_only_tree(target_backend,
                                     target_directory,
                                     queue,
                                     report,
                                     report_mutex,
                                     collect_detailed_records);
            }
        }
        enqueue_flat_folder_work(queue, std::move(child_work));
    }
    finish_flat_folder_work(queue);
}

void live_diff_metadata_worker(const std::string& source_root,
                               const std::string& target_root,
                               bool recursive,
                               const std::string& compare_mode,
                               std::size_t async_directory_depth,
                               FlatMetadataWorkQueue& queue,
                               TransferReport& report,
                               std::mutex& report_mutex,
                               bool collect_detailed_records) {
    auto source_backend = make_nfs_backend(source_root);
    auto target_backend = make_nfs_backend(target_root);
    try {
        source_backend->scan_flat_folders(
            async_directory_depth,
            [&queue](bool wait_for_work) {
                return take_flat_folder_work(queue, wait_for_work);
            },
            [&queue] {
                return flat_metadata_scan_should_stop(queue);
            },
            [recursive,
             &compare_mode,
             &queue,
             &target_backend,
             &report,
             &report_mutex,
             collect_detailed_records](FlatFolderScanBatch batch) {
                record_live_diff_batch(recursive,
                                       compare_mode,
                                       queue,
                                       *target_backend,
                                       report,
                                       report_mutex,
                                       collect_detailed_records,
                                       std::move(batch));
            });
        if (flat_metadata_scan_should_stop(queue)) {
            request_flat_folder_stop(queue);
        }
    } catch (...) {
        fail_flat_folder_work(queue);
    }
}

struct PendingLiveDiffPair {
    std::optional<FlatFolderScanBatch> source;
    std::optional<FlatFolderScanBatch> target;
};

struct LiveDiffSummaryCoordinator {
    std::mutex mutex;
    std::unordered_map<std::string, PendingLiveDiffPair> pending;
};

[[nodiscard]] std::string batch_folder_path(const FlatFolderScanBatch& batch) {
    return normalize_path(batch.folder.rel_path);
}

DiffBatchResults compare_live_diff_summary_batches(const std::string& compare_mode,
                                                   bool recursive,
                                                   bool allow_target_only,
                                                   FlatFolderScanBatch source_batch,
                                                   FlatFolderScanBatch target_batch,
                                                   std::vector<FileSpec>& target_only_child_work) {
    DiffBatchResults results;
    const std::string folder_path = batch_folder_path(source_batch);
    std::uint64_t flat_logical_size = 0;

    std::unordered_set<std::string> source_directories;
    source_directories.reserve(source_batch.directories.size());
    for (auto& directory : source_batch.directories) {
        directory.rel_path = normalize_path(directory.rel_path);
        source_directories.insert(directory.rel_path);
    }

    std::unordered_map<std::string_view, std::size_t> target_by_path;
    std::vector<unsigned char> target_matched;
    if (!target_batch.failed) {
        target_by_path.reserve(target_batch.files.size());
        if (allow_target_only) {
            target_matched.assign(target_batch.files.size(), 0U);
        }
        for (std::size_t index = 0; index < target_batch.files.size(); ++index) {
            auto& file = target_batch.files[index];
            file.rel_path = normalize_path(file.rel_path);
            target_by_path.emplace(std::string_view(file.rel_path), index);
        }
    }

    for (auto& source : source_batch.files) {
        source.rel_path = normalize_path(source.rel_path);
        const std::uint64_t source_size = file_spec_logical_size(source);
        flat_logical_size += source_size;

        FileOutcome outcome;
        outcome.rel_path = source.rel_path;
        outcome.size = source_size;

        const auto target_it = target_by_path.find(std::string_view(source.rel_path));
        if (target_it == target_by_path.end()) {
            outcome.diff = DiffKind::new_file;
            results.bytes_planned += source_size;
        } else if (diff_file_specs_match(source, target_batch.files[target_it->second], compare_mode)) {
            outcome.diff = DiffKind::skip;
        } else {
            outcome.diff = DiffKind::changed;
            results.bytes_planned += source_size;
        }

        record_diff_file(results, std::move(outcome), false);
        if (allow_target_only && target_it != target_by_path.end()) {
            target_matched[target_it->second] = 1U;
        }
    }

    if (allow_target_only) {
        for (std::size_t index = 0; index < target_batch.files.size(); ++index) {
            if (!target_matched.empty() && target_matched[index] != 0U) {
                continue;
            }
            const FileSpec& target = target_batch.files[index];
            FileOutcome outcome;
            outcome.rel_path = target.rel_path;
            outcome.diff = DiffKind::target_only;
            outcome.size = file_spec_logical_size(target);
            record_diff_file(results, std::move(outcome), false);
        }
    }

    FileSpec normalized_folder = source_batch.folder;
    normalized_folder.rel_path = folder_path;
    record_diff_folder(results,
                       make_done_folder_record(normalized_folder, source_batch.files.size(), flat_logical_size),
                       false);

    if (recursive && allow_target_only && !target_batch.failed) {
        for (auto& target_directory : target_batch.directories) {
            target_directory.rel_path = normalize_path(target_directory.rel_path);
            if (source_directories.find(target_directory.rel_path) == source_directories.end()) {
                target_directory.need_check = false;
                target_only_child_work.push_back(std::move(target_directory));
            }
        }
    }

    return results;
}

DiffBatchResults count_target_only_summary_batch(bool recursive,
                                                 FlatFolderScanBatch batch,
                                                 std::vector<FileSpec>& child_work) {
    DiffBatchResults results;
    const std::string folder_path = batch_folder_path(batch);
    if (batch.failed) {
        FileOutcome outcome;
        outcome.rel_path = folder_path;
        outcome.diff = DiffKind::failed;
        record_diff_file(results, std::move(outcome), false);
        return results;
    }

    FileSpec normalized_folder = batch.folder;
    normalized_folder.rel_path = folder_path;
    record_diff_folder(results, make_done_folder_record(normalized_folder, 0, 0), false);

    for (auto& file : batch.files) {
        file.rel_path = normalize_path(file.rel_path);
        FileOutcome outcome;
        outcome.rel_path = file.rel_path;
        outcome.diff = DiffKind::target_only;
        outcome.size = file_spec_logical_size(file);
        record_diff_file(results, std::move(outcome), false);
    }

    if (recursive) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            directory.need_check = false;
            child_work.push_back(std::move(directory));
        }
    }

    return results;
}

void merge_summary_results(TransferReport& report,
                           std::mutex& report_mutex,
                           DiffBatchResults&& results) {
    merge_live_diff_results(report, std::move(results), false, report_mutex);
}

void handle_ready_summary_pair(const std::string& compare_mode,
                               bool recursive,
                               bool allow_target_only,
                               DiffTargetFolderQueue& target_queue,
                               TransferReport& report,
                               std::mutex& report_mutex,
                               FlatFolderScanBatch source_batch,
                               FlatFolderScanBatch target_batch) {
    std::vector<FileSpec> target_only_child_work;
    DiffBatchResults results = compare_live_diff_summary_batches(compare_mode,
                                                                 recursive,
                                                                 allow_target_only,
                                                                 std::move(source_batch),
                                                                 std::move(target_batch),
                                                                 target_only_child_work);
    if (!target_only_child_work.empty()) {
        enqueue_diff_target_folders(target_queue, std::move(target_only_child_work));
    }
    merge_summary_results(report, report_mutex, std::move(results));
}

void store_summary_source_batch(LiveDiffSummaryCoordinator& coordinator,
                                const std::string& compare_mode,
                                bool recursive,
                                bool allow_target_only,
                                DiffTargetFolderQueue& target_queue,
                                TransferReport& report,
                                std::mutex& report_mutex,
                                FlatFolderScanBatch batch) {
    const std::string folder_path = batch_folder_path(batch);
    std::optional<FlatFolderScanBatch> target_batch;
    {
        std::lock_guard<std::mutex> lock(coordinator.mutex);
        PendingLiveDiffPair& pair = coordinator.pending[folder_path];
        pair.source = std::move(batch);
        if (pair.target.has_value()) {
            target_batch = std::move(pair.target);
            batch = std::move(*pair.source);
            coordinator.pending.erase(folder_path);
        }
    }
    if (target_batch.has_value()) {
        handle_ready_summary_pair(compare_mode,
                                  recursive,
                                  allow_target_only,
                                  target_queue,
                                  report,
                                  report_mutex,
                                  std::move(batch),
                                  std::move(*target_batch));
    }
}

void store_summary_target_batch(LiveDiffSummaryCoordinator& coordinator,
                                const std::string& compare_mode,
                                bool recursive,
                                bool allow_target_only,
                                DiffTargetFolderQueue& target_queue,
                                TransferReport& report,
                                std::mutex& report_mutex,
                                FlatFolderScanBatch batch) {
    if (!batch.folder.need_check) {
        std::vector<FileSpec> target_only_child_work;
        DiffBatchResults results = count_target_only_summary_batch(recursive,
                                                                   std::move(batch),
                                                                   target_only_child_work);
        if (!target_only_child_work.empty()) {
            enqueue_diff_target_folders(target_queue, std::move(target_only_child_work));
        }
        merge_summary_results(report, report_mutex, std::move(results));
        return;
    }

    const std::string folder_path = batch_folder_path(batch);
    std::optional<FlatFolderScanBatch> source_batch;
    {
        std::lock_guard<std::mutex> lock(coordinator.mutex);
        PendingLiveDiffPair& pair = coordinator.pending[folder_path];
        pair.target = std::move(batch);
        if (pair.source.has_value()) {
            source_batch = std::move(pair.source);
            batch = std::move(*pair.target);
            coordinator.pending.erase(folder_path);
        }
    }
    if (source_batch.has_value()) {
        handle_ready_summary_pair(compare_mode,
                                  recursive,
                                  allow_target_only,
                                  target_queue,
                                  report,
                                  report_mutex,
                                  std::move(*source_batch),
                                  std::move(batch));
    }
}

void record_source_summary_diff_batch(bool recursive,
                                      FlatMetadataWorkQueue& source_queue,
                                      DiffTargetFolderQueue& target_queue,
                                      LiveDiffSummaryCoordinator& coordinator,
                                      const std::string& compare_mode,
                                      bool allow_target_only,
                                      TransferReport& report,
                                      std::mutex& report_mutex,
                                      FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "diff skipped source folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message = batch.error.empty() ? "failed to scan source root" : batch.error;
            fail_flat_folder_work(source_queue, std::make_exception_ptr(std::runtime_error(message)));
            fail_diff_target_folder_work(target_queue, std::make_exception_ptr(std::runtime_error(message)));
            return;
        }
        finish_flat_folder_work(source_queue);
        return;
    }

    const std::string folder_path = batch_folder_path(batch);
    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(source_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    } else {
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
        }
    }

    FileSpec target_folder = batch.folder;
    target_folder.rel_path = folder_path;
    target_folder.need_check = true;
    if (!enqueue_diff_target_folder(target_queue, std::move(target_folder))) {
        finish_flat_folder_work(source_queue);
        return;
    }

    store_summary_source_batch(coordinator,
                               compare_mode,
                               recursive,
                               allow_target_only,
                               target_queue,
                               report,
                               report_mutex,
                               std::move(batch));
    enqueue_flat_folder_work(source_queue, std::move(child_work));
    finish_flat_folder_work(source_queue);
}

void source_summary_diff_worker(const std::string& source_root,
                                bool recursive,
                                const std::string& compare_mode,
                                bool allow_target_only,
                                std::size_t async_directory_depth,
                                FlatMetadataWorkQueue& source_queue,
                                DiffTargetFolderQueue& target_queue,
                                LiveDiffSummaryCoordinator& coordinator,
                                TransferReport& report,
                                std::mutex& report_mutex) {
    auto source_backend = make_nfs_backend(source_root);
    try {
        source_backend->scan_flat_folders(
            async_directory_depth,
            [&source_queue](bool wait_for_work) {
                return take_flat_folder_work(source_queue, wait_for_work);
            },
            [&source_queue] {
                return flat_metadata_scan_should_stop(source_queue);
            },
            [recursive,
             &source_queue,
             &target_queue,
             &coordinator,
             &compare_mode,
             allow_target_only,
             &report,
             &report_mutex](FlatFolderScanBatch batch) {
                record_source_summary_diff_batch(recursive,
                                                 source_queue,
                                                 target_queue,
                                                 coordinator,
                                                 compare_mode,
                                                 allow_target_only,
                                                 report,
                                                 report_mutex,
                                                 std::move(batch));
            });
        if (flat_metadata_scan_should_stop(source_queue)) {
            request_flat_folder_stop(source_queue);
        }
    } catch (...) {
        fail_flat_folder_work(source_queue);
        fail_diff_target_folder_work(target_queue);
    }
}

bool diff_target_queue_should_stop(DiffTargetFolderQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return queue.done || queue.error != nullptr;
}

void target_summary_diff_worker(const std::string& target_root,
                                bool recursive,
                                const std::string& compare_mode,
                                bool allow_target_only,
                                std::size_t async_directory_depth,
                                DiffTargetFolderQueue& target_queue,
                                LiveDiffSummaryCoordinator& coordinator,
                                TransferReport& report,
                                std::mutex& report_mutex) {
    auto target_backend = make_nfs_backend(target_root);
    try {
        target_backend->scan_flat_folders(
            async_directory_depth,
            [&target_queue](bool wait_for_work) {
                return take_diff_target_folder_work(target_queue, wait_for_work);
            },
            [&target_queue] {
                return diff_target_queue_should_stop(target_queue);
            },
            [recursive,
             &target_queue,
             &coordinator,
             &compare_mode,
             allow_target_only,
             &report,
             &report_mutex](FlatFolderScanBatch batch) {
                if (batch.failed) {
                    std::cerr << "diff treats target folder '"
                              << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                              << "' as empty: "
                              << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
                }
                store_summary_target_batch(coordinator,
                                           compare_mode,
                                           recursive,
                                           allow_target_only,
                                           target_queue,
                                           report,
                                           report_mutex,
                                           std::move(batch));
                finish_diff_target_folder_work(target_queue);
            });
    } catch (...) {
        fail_diff_target_folder_work(target_queue);
    }
}

void flush_unmatched_summary_batches(LiveDiffSummaryCoordinator& coordinator,
                                     const std::string& compare_mode,
                                     bool recursive,
                                     bool allow_target_only,
                                     DiffTargetFolderQueue& target_queue,
                                     TransferReport& report,
                                     std::mutex& report_mutex) {
    std::vector<PendingLiveDiffPair> pending;
    {
        std::lock_guard<std::mutex> lock(coordinator.mutex);
        pending.reserve(coordinator.pending.size());
        for (auto& [_, pair] : coordinator.pending) {
            pending.push_back(std::move(pair));
        }
        coordinator.pending.clear();
    }

    for (auto& pair : pending) {
        if (pair.source.has_value()) {
            FlatFolderScanBatch empty_target;
            empty_target.folder = pair.source->folder;
            handle_ready_summary_pair(compare_mode,
                                      recursive,
                                      allow_target_only,
                                      target_queue,
                                      report,
                                      report_mutex,
                                      std::move(*pair.source),
                                      std::move(empty_target));
        } else if (allow_target_only && pair.target.has_value()) {
            std::vector<FileSpec> target_only_child_work;
            DiffBatchResults results = count_target_only_summary_batch(recursive,
                                                                       std::move(*pair.target),
                                                                       target_only_child_work);
            merge_summary_results(report, report_mutex, std::move(results));
        }
    }
}

TransferReport run_summary_live_diff(const std::string& source_root,
                                     const std::string& target_root,
                                     const std::string& compare_mode,
                                     bool recursive,
                                     const NfsMetaReaderConfig& reader_config,
                                     double max_duration_seconds,
                                     std::uint32_t stats_interval_seconds) {
    TransferReport report;
    report.mode = Mode::dry_run;
    const auto started_at = std::chrono::steady_clock::now();

    FlatMetadataWorkQueue source_queue;
    source_queue.folders.push_back(FileSpec{});
    if (max_duration_seconds > 0.0) {
        source_queue.stop_at = std::chrono::steady_clock::now() +
                               std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                   std::chrono::duration<double>(max_duration_seconds));
    }

    DiffTargetFolderQueue target_queue;
    target_queue.max_entries = std::max<std::size_t>(
        16384,
        std::max<std::size_t>(1, reader_config.worker_count) *
            std::max<std::size_t>(1, reader_config.async_directory_depth) * 2U);
    LiveDiffSummaryCoordinator coordinator;
    std::mutex report_mutex;
    std::atomic<bool> stats_done{false};
    std::thread stats_thread;
    if (stats_interval_seconds != 0U) {
        stats_thread = std::thread([&report, &report_mutex, &stats_done, stats_interval_seconds, started_at]() {
            std::size_t last_records = 0;
            auto last_at = started_at;
            while (!stats_done.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::seconds(stats_interval_seconds));
                if (stats_done.load(std::memory_order_relaxed)) {
                    break;
                }
                std::size_t records = 0;
                std::size_t same = 0;
                std::size_t changed = 0;
                std::size_t created = 0;
                std::size_t target_only = 0;
                std::uint64_t bytes_planned = 0;
                {
                    std::lock_guard<std::mutex> lock(report_mutex);
                    records = report.files_total;
                    same = report.files_skipped;
                    changed = report.files_changed;
                    created = report.files_new;
                    target_only = report.files_target_only;
                    bytes_planned = report.bytes_planned;
                }
                const auto now = std::chrono::steady_clock::now();
                const double elapsed = std::chrono::duration<double>(now - started_at).count();
                const double interval_elapsed = std::chrono::duration<double>(now - last_at).count();
                const std::size_t interval_records = records >= last_records ? records - last_records : 0;
                const double average_rate = elapsed > 0.0 ? static_cast<double>(records) / elapsed : 0.0;
                const double interval_rate =
                    interval_elapsed > 0.0 ? static_cast<double>(interval_records) / interval_elapsed : 0.0;
                std::cout << "diff_stats records_per_second=" << average_rate
                          << " interval_records_per_second=" << interval_rate
                          << " diff_records=" << records
                          << " same=" << same
                          << " changed=" << changed
                          << " new=" << created
                          << " target_only=" << target_only
                          << " bytes_planned=" << bytes_planned
                          << " elapsed_seconds=" << elapsed << std::endl;
                last_records = records;
                last_at = now;
            }
        });
    }

    const std::size_t thread_count = std::max<std::size_t>(1, reader_config.worker_count);
    const std::size_t async_depth = std::max<std::size_t>(1, reader_config.async_directory_depth);
    const bool allow_target_only = max_duration_seconds <= 0.0;
    std::vector<std::thread> target_workers;
    std::vector<std::thread> source_workers;
    target_workers.reserve(thread_count);
    source_workers.reserve(thread_count);
    for (std::size_t index = 0; index < thread_count; ++index) {
        target_workers.emplace_back(target_summary_diff_worker,
                                    target_root,
                                    recursive,
                                    compare_mode,
                                    allow_target_only,
                                    async_depth,
                                    std::ref(target_queue),
                                    std::ref(coordinator),
                                    std::ref(report),
                                    std::ref(report_mutex));
    }
    for (std::size_t index = 0; index < thread_count; ++index) {
        source_workers.emplace_back(source_summary_diff_worker,
                                    source_root,
                                    recursive,
                                    compare_mode,
                                    allow_target_only,
                                    async_depth,
                                    std::ref(source_queue),
                                    std::ref(target_queue),
                                    std::ref(coordinator),
                                    std::ref(report),
                                    std::ref(report_mutex));
    }

    for (auto& worker : source_workers) {
        worker.join();
    }
    mark_diff_target_input_done(target_queue);
    for (auto& worker : target_workers) {
        worker.join();
    }
    flush_unmatched_summary_batches(coordinator,
                                    compare_mode,
                                    recursive,
                                    allow_target_only,
                                    target_queue,
                                    report,
                                    report_mutex);
    stats_done.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }

    if (source_queue.error) {
        std::rethrow_exception(source_queue.error);
    }
    if (target_queue.error) {
        std::rethrow_exception(target_queue.error);
    }
    return report;
}

std::size_t queued_data_read_files(DataReadFileQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return queue.files.size();
}

void record_data_read_metadata(DataReadBenchmarkStats& stats,
                               std::size_t files_found,
                               std::size_t folders_found,
                               std::uint64_t logical_size_bytes) {
    stats.files_found.fetch_add(files_found, std::memory_order_relaxed);
    stats.folders_found.fetch_add(folders_found, std::memory_order_relaxed);
    stats.logical_size_bytes.fetch_add(logical_size_bytes, std::memory_order_relaxed);
}

void record_data_read_bytes(DataReadBenchmarkStats& stats, std::uint64_t bytes_read) {
    stats.bytes_read.fetch_add(bytes_read, std::memory_order_relaxed);
}

void record_data_read_file(DataReadBenchmarkStats& stats) {
    stats.files_read.fetch_add(1, std::memory_order_relaxed);
}

void print_data_buffer_read_stats(const DataReadBenchmarkStats& stats,
                                  DataReadFileQueue& file_queue,
                                  const BufQueue& data_queue) {
    const DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    std::cerr << "data_read_stats bytes_per_second=" << snapshot.bytes_per_second
              << " gigabits_per_second=" << snapshot.gigabits_per_second
              << " bytes_read=" << snapshot.bytes_read
              << " files_read=" << snapshot.files_read
              << " files_found=" << snapshot.files_found
              << " folders_found=" << snapshot.folders_found
              << " logical_size_bytes=" << snapshot.logical_size_bytes
              << " queued_files=" << queued_data_read_files(file_queue)
              << " data_queue_depth=" << data_queue.size()
              << " elapsed_seconds=" << snapshot.elapsed_seconds << '\n';
}

void run_data_buffer_read_stats_printer(DataReadBenchmarkStats& stats,
                                        DataReadFileQueue& file_queue,
                                        const BufQueue& data_queue) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_data_buffer_read_stats(stats, file_queue, data_queue);
    }
}

void print_data_hash_stats(const DataReadBenchmarkStats& stats,
                           DataReadFileQueue& file_queue,
                           const DataHasherJob& hasher,
                           const BufQueue& data_queue,
                           const BufQueue& hashed_queue) {
    const DataReadBenchmarkSnapshot read = snapshot_data_read_stats(stats);
    const DataHasherStats hash = hasher.stats();
    const double hash_bytes_per_second =
        read.elapsed_seconds > 0.0 ? static_cast<double>(hash.bytes_hashed) / read.elapsed_seconds : 0.0;
    const double hash_gigabits_per_second = hash_bytes_per_second * 8.0 / 1'000'000'000.0;
    std::cerr << "data_hash_stats read_bytes_per_second=" << read.bytes_per_second
              << " read_gigabits_per_second=" << read.gigabits_per_second
              << " hash_bytes_per_second=" << hash_bytes_per_second
              << " hash_gigabits_per_second=" << hash_gigabits_per_second
              << " bytes_read=" << read.bytes_read
              << " bytes_hashed=" << hash.bytes_hashed
              << " files_read=" << read.files_read
              << " files_failed=" << read.files_failed
              << " files_found=" << read.files_found
              << " folders_found=" << read.folders_found
              << " queued_files=" << queued_data_read_files(file_queue)
              << " data_queue_depth=" << data_queue.size()
              << " hashed_queue_depth=" << hashed_queue.size()
              << " hash_work_factor=" << hash.work_factor
              << " elapsed_seconds=" << read.elapsed_seconds << '\n';
}

void run_data_hash_stats_printer(DataReadBenchmarkStats& stats,
                                 DataReadFileQueue& file_queue,
                                 const DataHasherJob& hasher,
                                 const BufQueue& data_queue,
                                 const BufQueue& hashed_queue) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_data_hash_stats(stats, file_queue, hasher, data_queue, hashed_queue);
    }
}

bool data_read_timer_expired(const DataReadFileQueue& queue) {
    return queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at;
}

void fail_data_file_work(DataReadFileQueue& queue, std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.stop = true;
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void request_data_file_stop(DataReadFileQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.stop = true;
        queue.files.clear();
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

bool enqueue_data_read_files(DataReadFileQueue& queue, std::vector<FileSpec>&& files) {
    for (auto& file : files) {
        std::unique_lock<std::mutex> lock(queue.mutex);
        const auto ready = [&queue]() {
            return queue.stop || queue.error || queue.files.size() < queue.max_entries || data_read_timer_expired(queue);
        };
        if (queue.stop_at.has_value()) {
            queue.cv_not_full.wait_until(lock, *queue.stop_at, ready);
        } else {
            queue.cv_not_full.wait(lock, ready);
        }

        if (data_read_timer_expired(queue)) {
            queue.stop = true;
            queue.files.clear();
        }
        if (queue.stop || queue.error) {
            queue.cv_not_empty.notify_all();
            queue.cv_not_full.notify_all();
            return false;
        }

        queue.files.push_back(std::move(file));
        lock.unlock();
        queue.cv_not_empty.notify_one();
    }
    return true;
}

std::optional<FileSpec> take_data_file_work(DataReadFileQueue& queue) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.stop || queue.error || !queue.files.empty() || queue.input_done || data_read_timer_expired(queue);
    };
    if (queue.stop_at.has_value()) {
        queue.cv_not_empty.wait_until(lock, *queue.stop_at, ready);
    } else {
        queue.cv_not_empty.wait(lock, ready);
    }

    if (data_read_timer_expired(queue)) {
        queue.stop = true;
        queue.files.clear();
    }
    if (queue.stop || queue.error || queue.files.empty()) {
        return std::nullopt;
    }

    FileSpec file = std::move(queue.files.front());
    queue.files.pop_front();
    lock.unlock();
    queue.cv_not_full.notify_one();
    return file;
}

void mark_data_file_input_done(DataReadFileQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.input_done = true;
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void record_flat_metadata_batch(bool recursive,
                                FlatMetadataWorkQueue& queue,
                                MetadataStatsDiscarder& stats_discarder,
                                MetadataRecordWriter* record_writer,
                                PartitionedMetadataWriter* partitioned_writer,
                                FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            fail_flat_folder_work(queue, std::make_exception_ptr(std::runtime_error(message)));
            return;
        }
        finish_flat_folder_work(queue);
        return;
    }

    std::uint64_t logical_size_bytes = 0;
    for (const auto& file : batch.files) {
        logical_size_bytes += file.declared_size != 0 ? file.declared_size : file.content.size();
    }

    std::vector<std::string> folders_found;
    folders_found.reserve(batch.directories.size());
    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(queue)) {
        child_work.reserve(batch.directories.size());
    }

    for (auto& directory : batch.directories) {
        directory.rel_path = normalize_path(directory.rel_path);
        folders_found.push_back(directory.rel_path);
        if (recursive && !flat_metadata_scan_should_stop(queue)) {
            child_work.push_back(directory);
        }
    }

    stats_discarder.record_batch(batch.files.size(), logical_size_bytes, folders_found);
    if (record_writer != nullptr) {
        MetadataFolderRecord folder_record;
        folder_record.spec = std::move(batch.folder);
        folder_record.flat_file_count = batch.files.size();
        folder_record.flat_logical_size_bytes = logical_size_bytes;
        record_writer->write_batch(batch.files, std::vector<MetadataFolderRecord>{std::move(folder_record)});
    } else if (partitioned_writer != nullptr) {
        MetadataFolderRecord folder_record;
        folder_record.spec = std::move(batch.folder);
        folder_record.flat_file_count = batch.files.size();
        folder_record.flat_logical_size_bytes = logical_size_bytes;
        partitioned_writer->write_batch(batch.files, folder_record);
    }
    enqueue_flat_folder_work(queue, std::move(child_work));
    finish_flat_folder_work(queue);
}

void scan_flat_metadata_worker(const std::string& source_root,
                               bool recursive,
                               std::size_t async_directory_depth,
                               FlatMetadataWorkQueue& queue,
                               MetadataStatsDiscarder& stats_discarder,
                               MetadataRecordWriter* record_writer,
                               PartitionedMetadataWriter* partitioned_writer) {
    auto backend = make_nfs_backend(source_root);
    try {
        backend->scan_flat_folders(
            async_directory_depth,
            [&queue](bool wait_for_work) {
                return take_flat_folder_work(queue, wait_for_work);
            },
            [&queue] {
                return flat_metadata_scan_should_stop(queue);
            },
            [recursive, &queue, &stats_discarder, record_writer, partitioned_writer](FlatFolderScanBatch batch) {
                record_flat_metadata_batch(recursive,
                                           queue,
                                           stats_discarder,
                                           record_writer,
                                           partitioned_writer,
                                           std::move(batch));
            });
        if (flat_metadata_scan_should_stop(queue)) {
            request_flat_folder_stop(queue);
        }
    } catch (...) {
        fail_flat_folder_work(queue);
    }
}

void run_parallel_flat_metadata_scan(const NfsMetaReaderConfig& reader_config,
                                     MetadataStatsDiscarder& stats_discarder,
                                     MetadataRecordWriter* record_writer,
                                     PartitionedMetadataWriter* partitioned_writer,
                                     double max_duration_seconds,
                                     const std::filesystem::path& status_socket_path) {
    FlatMetadataWorkQueue queue;
    queue.folders.push_back(FileSpec{});
    if (max_duration_seconds > 0.0) {
        queue.stop_at = std::chrono::steady_clock::now() +
                        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                            std::chrono::duration<double>(max_duration_seconds));
    }
    stats_discarder.record_folder("");

    const std::size_t thread_count = std::max<std::size_t>(1, reader_config.worker_count);
    StatusRegistry status_registry;
    std::unique_ptr<StatusServer> status_server;
    if (!status_socket_path.empty()) {
        status_registry.register_job("nfs_meta_reader", [&stats_discarder, thread_count]() {
            const MetadataStatsSnapshot stats = stats_discarder.snapshot();
            MonitorJobSnapshot snapshot;
            snapshot.name = "nfs_meta_reader";
            snapshot.running = true;
            snapshot.worker_count = thread_count;
            snapshot.processed_count = stats.files_found + stats.folders_found;
            snapshot.byte_count = stats.logical_size_bytes;
            snapshot.count_unit = "records";
            snapshot.detail = "files=" + std::to_string(stats.files_found) +
                              " folders=" + std::to_string(stats.folders_found);
            return snapshot;
        });
        status_registry.register_job("metadata_stats_discarder", [&stats_discarder]() {
            const MetadataStatsSnapshot stats = stats_discarder.snapshot();
            const JobStats job = stats_discarder.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "metadata_stats_discarder";
            snapshot.running = job.running;
            snapshot.worker_count = 1;
            snapshot.processed_count = stats.files_found + stats.folders_found;
            snapshot.byte_count = stats.logical_size_bytes;
            snapshot.count_unit = "records";
            snapshot.detail = "files=" + std::to_string(stats.files_found) +
                              " folders=" + std::to_string(stats.folders_found);
            return snapshot;
        });
        if (record_writer != nullptr) {
            status_registry.register_job("metadata_record_writer", [record_writer]() {
                MonitorJobSnapshot snapshot;
                snapshot.name = "metadata_record_writer";
                snapshot.running = true;
                snapshot.worker_count = 1;
                snapshot.processed_count = record_writer->files_written() + record_writer->folders_written();
                snapshot.count_unit = "records";
                snapshot.detail = "files_written=" + std::to_string(record_writer->files_written()) +
                                  " folders_written=" + std::to_string(record_writer->folders_written());
                return snapshot;
            });
        }
        status_registry.register_queue("folder_work_queue", [&queue]() {
            return monitor_flat_folder_queue("folder_work_queue", queue);
        });
        status_server = std::make_unique<StatusServer>(status_socket_path, status_registry);
        status_server->start();
    }

    std::vector<std::thread> workers;
    workers.reserve(thread_count);
    for (std::size_t index = 0; index < thread_count; ++index) {
        workers.emplace_back(scan_flat_metadata_worker,
                             reader_config.source_root,
                             reader_config.recursive,
                             std::max<std::size_t>(1, reader_config.async_directory_depth),
                             std::ref(queue),
                             std::ref(stats_discarder),
                             record_writer,
                             partitioned_writer);
    }

    for (auto& worker : workers) {
        worker.join();
    }
    if (queue.error) {
        std::rethrow_exception(queue.error);
    }
    if (status_server) {
        status_server->stop();
    }
}

void record_data_read_metadata_batch(bool recursive,
                                     FlatMetadataWorkQueue& folder_queue,
                                     DataReadFileQueue& file_queue,
    DataReadBenchmarkStats& stats,
    FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            const auto error = std::make_exception_ptr(std::runtime_error(message));
            fail_flat_folder_work(folder_queue, error);
            fail_data_file_work(file_queue, error);
            return;
        }
        finish_flat_folder_work(folder_queue);
        return;
    }

    std::uint64_t logical_size_bytes = 0;
    for (const auto& file : batch.files) {
        logical_size_bytes += file.declared_size != 0 ? file.declared_size : file.content.size();
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    record_data_read_metadata(stats, batch.files.size(), batch.directories.size(), logical_size_bytes);
    if (!enqueue_data_read_files(file_queue, std::move(batch.files))) {
        finish_flat_folder_work(folder_queue);
        return;
    }
    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    finish_flat_folder_work(folder_queue);
}

void scan_data_read_metadata_worker(const std::string& source_root,
                                    bool recursive,
                                    std::size_t async_directory_depth,
                                    FlatMetadataWorkQueue& folder_queue,
                                    DataReadFileQueue& file_queue,
                                    DataReadBenchmarkStats& stats) {
    auto backend = make_nfs_backend(source_root);
    try {
        backend->scan_flat_folders(
            async_directory_depth,
            [&folder_queue](bool wait_for_work) {
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue, &file_queue] {
                return flat_metadata_scan_should_stop(folder_queue) || data_read_timer_expired(file_queue);
            },
            [recursive, &folder_queue, &file_queue, &stats](FlatFolderScanBatch batch) {
                record_data_read_metadata_batch(recursive,
                                                folder_queue,
                                                file_queue,
                                                stats,
                                                std::move(batch));
            });
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_flat_folder_work(folder_queue, error);
        fail_data_file_work(file_queue, error);
    }
}

DataReadBenchmarkSnapshot run_parallel_data_read_scan(const NfsMetaReaderConfig& meta_config,
                                                      const NfsDataReaderConfig& data_config,
                                                      std::size_t max_files_queued,
                                                      std::size_t data_buffer_slots,
                                                      std::size_t data_queue_depth,
                                                      double max_duration_seconds,
                                                      std::uint32_t stats_interval_seconds,
                                                      const std::filesystem::path& status_socket_path) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(1, max_files_queued);

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    stats.folders_found.store(1, std::memory_order_relaxed);

    const std::size_t data_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    const std::size_t outstanding = std::max<std::size_t>(1, data_config.outstanding_requests);
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);
    const std::size_t queue_depth =
        std::max<std::size_t>(1, data_queue_depth == 0 ? data_threads * outstanding * 2U : data_queue_depth);
    const std::size_t pool_slots =
        std::max<std::size_t>(data_threads * outstanding + queue_depth + 1U,
                              data_buffer_slots == 0 ? data_threads * outstanding * 3U + 1U
                                                     : data_buffer_slots);

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    BufferPoolRegistry registry;
    registry.register_pool(data_pool);
    BufQueue reader_to_discard(queue_depth);

    NfsDataBufferReaderJob data_reader_job(
        data_config,
        data_pool,
        reader_to_discard,
        [&file_queue]() {
            return take_data_file_work(file_queue);
        },
        [&file_queue]() {
            return data_read_timer_expired(file_queue);
        });
    data_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_data_read_bytes(stats, bytes_read);
    });
    data_reader_job.set_file_read_callback([&stats]() {
        record_data_read_file(stats);
    });
    data_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1, std::memory_order_relaxed);
    });

    BufferDiscarderJob discarder(BufferDiscarderConfig(1), reader_to_discard, registry);

    StatusRegistry status_registry;
    std::unique_ptr<StatusServer> status_server;
    if (!status_socket_path.empty()) {
        status_registry.register_job("nfs_meta_reader", [&stats, metadata_threads]() {
            const DataReadBenchmarkSnapshot stats_snapshot = snapshot_data_read_stats(stats);
            MonitorJobSnapshot snapshot;
            snapshot.name = "nfs_meta_reader";
            snapshot.running = true;
            snapshot.worker_count = metadata_threads;
            snapshot.processed_count = stats_snapshot.files_found + stats_snapshot.folders_found;
            snapshot.byte_count = stats_snapshot.logical_size_bytes;
            snapshot.count_unit = "records";
            snapshot.detail = "files_found=" + std::to_string(stats_snapshot.files_found) +
                              " folders_found=" + std::to_string(stats_snapshot.folders_found);
            return snapshot;
        });
        status_registry.register_job("nfs_data_reader", [&data_reader_job]() {
            const NfsDataBufferReaderStats stats_snapshot = data_reader_job.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "nfs_data_reader";
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_read;
            snapshot.byte_count = stats_snapshot.bytes_read;
            snapshot.count_unit = "buffers";
            snapshot.detail = "files_read=" + std::to_string(stats_snapshot.files_read) +
                              " files_failed=" + std::to_string(stats_snapshot.files_failed);
            return snapshot;
        });
        status_registry.register_job("buffer_discarder", [&discarder]() {
            const BufferDiscarderStats stats_snapshot = discarder.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "buffer_discarder";
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_discarded;
            snapshot.byte_count = stats_snapshot.bytes_discarded;
            snapshot.count_unit = "buffers";
            return snapshot;
        });
        status_registry.register_queue("folder_work_queue", [&folder_queue]() {
            return monitor_flat_folder_queue("folder_work_queue", folder_queue);
        });
        status_registry.register_queue("file_work_queue", [&file_queue]() {
            return monitor_data_file_queue("file_work_queue", file_queue);
        });
        status_registry.register_queue("reader_to_discard", [&reader_to_discard]() {
            return monitor_buf_queue("reader_to_discard", reader_to_discard);
        });
        status_server = std::make_unique<StatusServer>(status_socket_path, status_registry);
        status_server->start();
    }

    const auto benchmark_started_at = std::chrono::steady_clock::now();
    stats.started_at = benchmark_started_at;
    stats.last_print_at = benchmark_started_at;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = benchmark_started_at +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (folder_queue.stop_at.has_value()) {
        const auto stop_at = *folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(folder_queue);
                request_data_file_stop(file_queue);
            }
        });
    }

    std::thread stats_printer(run_data_buffer_read_stats_printer,
                              std::ref(stats),
                              std::ref(file_queue),
                              std::cref(reader_to_discard));

    discarder.start();
    data_reader_job.start();

    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_data_read_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1, meta_config.async_directory_depth),
                                      std::ref(folder_queue),
                                      std::ref(file_queue),
                                      std::ref(stats));
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);

    data_reader_job.wait();
    discarder.wait();

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (status_server) {
        status_server->stop();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }

    DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    snapshot.data_buffer_slots = pool_slots;
    snapshot.data_queue_depth = queue_depth;
    return snapshot;
}

DataHashBenchmarkReport run_parallel_data_hash_scan(const NfsMetaReaderConfig& meta_config,
                                                    const NfsDataReaderConfig& data_config,
                                                    ContentHashAlgorithm algorithm,
                                                    std::size_t hash_worker_threads,
                                                    std::size_t hash_work_factor,
                                                    std::size_t max_files_queued,
                                                    std::size_t data_buffer_slots,
                                                    std::size_t data_queue_depth,
                                                    double max_duration_seconds,
                                                    std::uint32_t stats_interval_seconds,
                                                    const std::filesystem::path& status_socket_path) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(1, max_files_queued);

    const std::size_t data_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    const std::size_t outstanding = std::max<std::size_t>(1, data_config.outstanding_requests);
    const std::size_t hash_threads = std::max<std::size_t>(1, hash_worker_threads);
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);
    const std::size_t effective_hash_work_factor = std::max<std::size_t>(1, hash_work_factor);
    const std::size_t queue_depth =
        std::max<std::size_t>(1, data_queue_depth == 0 ? data_threads * outstanding * 2U : data_queue_depth);
    const std::size_t pool_slots =
        std::max<std::size_t>(data_threads * outstanding + queue_depth + hash_threads + 1U,
                              data_buffer_slots == 0 ? data_threads * outstanding * 3U + hash_threads + 1U
                                                     : data_buffer_slots);

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    BufferPoolRegistry registry;
    registry.register_pool(data_pool);
    BufQueue reader_to_hasher(queue_depth);
    BufQueue hasher_to_discard(queue_depth);

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    stats.folders_found.store(1, std::memory_order_relaxed);

    NfsDataBufferReaderJob data_reader_job(
        data_config,
        data_pool,
        reader_to_hasher,
        [&file_queue]() {
            return take_data_file_work(file_queue);
        },
        [&file_queue]() {
            return data_read_timer_expired(file_queue);
        });
    data_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_data_read_bytes(stats, bytes_read);
    });
    data_reader_job.set_file_read_callback([&stats]() {
        record_data_read_file(stats);
    });
    data_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1, std::memory_order_relaxed);
    });

    DataHasherJob hasher(DataHasherConfig(hash_threads, algorithm, effective_hash_work_factor),
                         reader_to_hasher,
                         hasher_to_discard,
                         registry);
    BufferDiscarderJob discarder(BufferDiscarderConfig(1), hasher_to_discard, registry);

    StatusRegistry status_registry;
    std::unique_ptr<StatusServer> status_server;
    if (!status_socket_path.empty()) {
        status_registry.register_job("nfs_meta_reader", [&stats, metadata_threads]() {
            const DataReadBenchmarkSnapshot stats_snapshot = snapshot_data_read_stats(stats);
            MonitorJobSnapshot snapshot;
            snapshot.name = "nfs_meta_reader";
            snapshot.running = true;
            snapshot.worker_count = metadata_threads;
            snapshot.processed_count = stats_snapshot.files_found + stats_snapshot.folders_found;
            snapshot.byte_count = stats_snapshot.logical_size_bytes;
            snapshot.count_unit = "records";
            snapshot.detail = "files_found=" + std::to_string(stats_snapshot.files_found) +
                              " folders_found=" + std::to_string(stats_snapshot.folders_found);
            return snapshot;
        });
        status_registry.register_job("nfs_data_reader", [&data_reader_job]() {
            const NfsDataBufferReaderStats stats_snapshot = data_reader_job.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "nfs_data_reader";
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_read;
            snapshot.byte_count = stats_snapshot.bytes_read;
            snapshot.count_unit = "buffers";
            snapshot.detail = "files_read=" + std::to_string(stats_snapshot.files_read) +
                              " files_failed=" + std::to_string(stats_snapshot.files_failed);
            return snapshot;
        });
        status_registry.register_job("data_hasher", [&hasher]() {
            const DataHasherStats stats_snapshot = hasher.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "data_hasher";
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_hashed;
            snapshot.byte_count = stats_snapshot.bytes_hashed;
            snapshot.count_unit = "buffers";
            snapshot.detail = "hash=" + stats_snapshot.algorithm +
                              " work_factor=" + std::to_string(stats_snapshot.work_factor);
            return snapshot;
        });
        status_registry.register_job("buffer_discarder", [&discarder]() {
            const BufferDiscarderStats stats_snapshot = discarder.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "buffer_discarder";
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_discarded;
            snapshot.byte_count = stats_snapshot.bytes_discarded;
            snapshot.count_unit = "buffers";
            return snapshot;
        });
        status_registry.register_queue("folder_work_queue", [&folder_queue]() {
            return monitor_flat_folder_queue("folder_work_queue", folder_queue);
        });
        status_registry.register_queue("file_work_queue", [&file_queue]() {
            return monitor_data_file_queue("file_work_queue", file_queue);
        });
        status_registry.register_queue("reader_to_hasher", [&reader_to_hasher]() {
            return monitor_buf_queue("reader_to_hasher", reader_to_hasher);
        });
        status_registry.register_queue("hasher_to_discard", [&hasher_to_discard]() {
            return monitor_buf_queue("hasher_to_discard", hasher_to_discard);
        });
        status_server = std::make_unique<StatusServer>(status_socket_path, status_registry);
        status_server->start();
    }

    const auto benchmark_started_at = std::chrono::steady_clock::now();
    stats.started_at = benchmark_started_at;
    stats.last_print_at = benchmark_started_at;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = benchmark_started_at +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (folder_queue.stop_at.has_value()) {
        const auto stop_at = *folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(folder_queue);
                request_data_file_stop(file_queue);
            }
        });
    }

    std::thread stats_printer(run_data_hash_stats_printer,
                              std::ref(stats),
                              std::ref(file_queue),
                              std::cref(hasher),
                              std::cref(reader_to_hasher),
                              std::cref(hasher_to_discard));

    discarder.start();
    hasher.start();
    data_reader_job.start();

    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_data_read_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1, meta_config.async_directory_depth),
                                      std::ref(folder_queue),
                                      std::ref(file_queue),
                                      std::ref(stats));
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);

    data_reader_job.wait();
    hasher.wait();
    discarder.wait();

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (status_server) {
        status_server->stop();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }

    const DataReadBenchmarkSnapshot read = snapshot_data_read_stats(stats);
    const DataHasherStats hash = hasher.stats();

    DataHashBenchmarkReport report;
    report.files_found = read.files_found;
    report.folders_found = read.folders_found;
    report.files_read = read.files_read;
    report.files_failed = read.files_failed;
    report.logical_size_bytes = read.logical_size_bytes;
    report.bytes_read = read.bytes_read;
    report.bytes_hashed = hash.bytes_hashed;
    report.read_bytes_per_second = read.bytes_per_second;
    report.read_gigabits_per_second = read.gigabits_per_second;
    report.hash_bytes_per_second =
        read.elapsed_seconds > 0.0 ? static_cast<double>(hash.bytes_hashed) / read.elapsed_seconds : 0.0;
    report.hash_gigabits_per_second = report.hash_bytes_per_second * 8.0 / 1'000'000'000.0;
    report.elapsed_seconds = read.elapsed_seconds;
    report.data_buffer_slots = pool_slots;
    report.data_queue_depth = queue_depth;
    report.hash_work_factor = hash.work_factor;
    report.hash_algorithm = to_string(algorithm);
    return report;
}

HashInventorySnapshot snapshot_hash_inventory_stats(const HashInventoryStats& stats) {
    const auto now = std::chrono::steady_clock::now();
    const double elapsed = stats.started_at == std::chrono::steady_clock::time_point{}
                               ? 0.0
                               : std::chrono::duration<double>(now - stats.started_at).count();
    const std::uint64_t read_elapsed_nanoseconds =
        stats.read_elapsed_nanoseconds.load(std::memory_order_relaxed);
    const double read_elapsed = read_elapsed_nanoseconds != 0
                                    ? static_cast<double>(read_elapsed_nanoseconds) / 1'000'000'000.0
                                    : elapsed;
    HashInventorySnapshot snapshot;
    snapshot.files_found = stats.files_found.load(std::memory_order_relaxed);
    snapshot.folders_found = stats.folders_found.load(std::memory_order_relaxed);
    snapshot.files_hashed = stats.files_hashed.load(std::memory_order_relaxed);
    snapshot.files_failed = stats.files_failed.load(std::memory_order_relaxed);
    snapshot.logical_size_bytes = stats.logical_size_bytes.load(std::memory_order_relaxed);
    snapshot.bytes_read = stats.bytes_read.load(std::memory_order_relaxed);
    snapshot.bytes_hashed = stats.bytes_hashed.load(std::memory_order_relaxed);
    snapshot.elapsed_seconds = elapsed;
    snapshot.read_elapsed_seconds = read_elapsed;
    snapshot.read_bytes_per_second =
        read_elapsed > 0.0 ? static_cast<double>(snapshot.bytes_read) / read_elapsed : 0.0;
    snapshot.read_gigabits_per_second = snapshot.read_bytes_per_second * 8.0 / 1'000'000'000.0;
    snapshot.bytes_per_second = elapsed > 0.0 ? static_cast<double>(snapshot.bytes_hashed) / elapsed : 0.0;
    return snapshot;
}

HashMode parse_hash_mode(const std::string& value) {
    if (value.empty() || value == "file" || value == "full-file" || value == "full") {
        return HashMode::file;
    }
    if (value == "blocks" || value == "block" || value == "block-list") {
        return HashMode::blocks;
    }
    throw std::invalid_argument("hash mode must be file or blocks");
}

std::string to_string(HashMode mode) {
    switch (mode) {
        case HashMode::file:
            return "file";
        case HashMode::blocks:
            return "blocks";
    }
    return "unknown";
}

void record_hash_read_bytes(HashInventoryStats& stats, std::uint64_t bytes_read) {
    stats.bytes_read.fetch_add(bytes_read, std::memory_order_relaxed);
    const auto elapsed = std::chrono::steady_clock::now() - stats.started_at;
    const std::uint64_t elapsed_nanoseconds =
        static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
    std::uint64_t current = stats.read_elapsed_nanoseconds.load(std::memory_order_relaxed);
    while (elapsed_nanoseconds > current &&
           !stats.read_elapsed_nanoseconds.compare_exchange_weak(current,
                                                                 elapsed_nanoseconds,
                                                                 std::memory_order_relaxed,
                                                                 std::memory_order_relaxed)) {
    }
}

void record_hash_metadata(HashInventoryStats& stats,
                          std::size_t files_found,
                          std::size_t folders_found,
                          std::uint64_t logical_size_bytes) {
    stats.files_found.fetch_add(files_found, std::memory_order_relaxed);
    stats.folders_found.fetch_add(folders_found, std::memory_order_relaxed);
    stats.logical_size_bytes.fetch_add(logical_size_bytes, std::memory_order_relaxed);
}

void record_hash_metadata_batch(bool recursive,
                                FlatMetadataWorkQueue& folder_queue,
                                DataReadFileQueue& file_queue,
                                HashInventoryStats& stats,
    MetadataRecordWriter* record_writer,
    FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            const auto error = std::make_exception_ptr(std::runtime_error(message));
            fail_flat_folder_work(folder_queue, error);
            fail_data_file_work(file_queue, error);
            return;
        }
        finish_flat_folder_work(folder_queue);
        return;
    }

    std::uint64_t logical_size_bytes = 0;
    for (const auto& file : batch.files) {
        logical_size_bytes += file.declared_size != 0 ? file.declared_size : file.content.size();
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    record_hash_metadata(stats, batch.files.size(), batch.directories.size(), logical_size_bytes);
    if (record_writer != nullptr) {
        MetadataFolderRecord folder_record;
        folder_record.spec = std::move(batch.folder);
        folder_record.flat_file_count = batch.files.size();
        folder_record.flat_logical_size_bytes = logical_size_bytes;
        record_writer->write_batch({}, std::vector<MetadataFolderRecord>{std::move(folder_record)});
    }
    if (!enqueue_data_read_files(file_queue, std::move(batch.files))) {
        finish_flat_folder_work(folder_queue);
        return;
    }
    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    finish_flat_folder_work(folder_queue);
}

void scan_hash_metadata_worker(const std::string& source_root,
                               bool recursive,
                               std::size_t async_directory_depth,
                               FlatMetadataWorkQueue& folder_queue,
                               DataReadFileQueue& file_queue,
                               HashInventoryStats& stats,
                               MetadataRecordWriter* record_writer) {
    auto backend = make_nfs_backend(source_root);
    try {
        backend->scan_flat_folders(
            async_directory_depth,
            [&folder_queue](bool wait_for_work) {
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue, &file_queue] {
                return flat_metadata_scan_should_stop(folder_queue) || data_read_timer_expired(file_queue);
            },
            [recursive, &folder_queue, &file_queue, &stats, record_writer](FlatFolderScanBatch batch) {
                record_hash_metadata_batch(recursive,
                                           folder_queue,
                                           file_queue,
                                           stats,
                                           record_writer,
                                           std::move(batch));
            });
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_flat_folder_work(folder_queue, error);
        fail_data_file_work(file_queue, error);
    }
}

std::size_t hash_chunk_message_bytes(const HashChunkMessage& message) {
    return static_cast<std::size_t>(message.data_len);
}

void fail_hash_chunk_queue(HashChunkShardQueue& queue, std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.stop = true;
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void fail_hash_chunk_queues(std::vector<std::unique_ptr<HashChunkShardQueue>>& queues,
                            std::exception_ptr error = std::current_exception()) {
    for (auto& queue : queues) {
        fail_hash_chunk_queue(*queue, error);
    }
}

bool enqueue_hash_chunk_message(HashChunkShardQueue& queue, HashChunkMessage&& message) {
    const std::size_t message_bytes = hash_chunk_message_bytes(message);
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_full.wait(lock, [&queue]() {
        return queue.stop || queue.error || queue.messages.size() < queue.max_entries;
    });
    if (queue.stop || queue.error) {
        queue.cv_not_empty.notify_all();
        queue.cv_not_full.notify_all();
        return false;
    }

    queue.queued_bytes += message_bytes;
    queue.messages.push_back(std::move(message));
    lock.unlock();
    queue.cv_not_empty.notify_one();
    return true;
}

std::optional<HashChunkMessage> take_hash_chunk_message(HashChunkShardQueue& queue) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_empty.wait(lock, [&queue]() {
        return queue.stop || queue.error || !queue.messages.empty() || queue.input_done;
    });
    if (queue.messages.empty()) {
        return std::nullopt;
    }

    HashChunkMessage message = std::move(queue.messages.front());
    queue.messages.pop_front();
    queue.queued_bytes -= hash_chunk_message_bytes(message);
    lock.unlock();
    queue.cv_not_full.notify_one();
    return message;
}

void mark_hash_chunk_input_done(std::vector<std::unique_ptr<HashChunkShardQueue>>& queues) {
    for (auto& queue : queues) {
        {
            std::lock_guard<std::mutex> lock(queue->mutex);
            queue->input_done = true;
        }
        queue->cv_not_empty.notify_all();
        queue->cv_not_full.notify_all();
    }
}

HashChunkShardQueue& hash_queue_for_file(std::vector<std::unique_ptr<HashChunkShardQueue>>& queues,
                                         std::uint64_t file_id) {
    return *queues[static_cast<std::size_t>(file_id % queues.size())];
}

std::string join_block_hashes(const std::vector<std::string>& block_hashes) {
    std::string joined;
    for (const auto& hash : block_hashes) {
        if (!joined.empty()) {
            joined.push_back(';');
        }
        joined += hash;
    }
    return joined;
}

std::uint64_t fold_digest_prefix(const std::uint8_t* digest, std::size_t digest_size) {
    std::uint64_t folded = 0;
    const std::size_t bytes = std::min<std::size_t>(sizeof(folded), digest_size);
    std::memcpy(&folded, digest, bytes);
    return folded;
}

std::uint64_t benchmark_hash_once(ContentHashAlgorithm algorithm, std::string_view data) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5: {
            Md5State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            return fold_digest_prefix(digest.data(), digest.size());
        }
        case ContentHashAlgorithm::sha256: {
            Sha256State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            return fold_digest_prefix(digest.data(), digest.size());
        }
        case ContentHashAlgorithm::xxh64: {
            Hash64State hasher;
            hasher.update(data);
            return hasher.value();
        }
        case ContentHashAlgorithm::xxh3_64: {
            Xxh3_64State hasher;
            hasher.update(data);
            return hasher.value();
        }
        case ContentHashAlgorithm::xxh3_128: {
            Xxh3_128State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            return fold_digest_prefix(digest.data(), digest.size());
        }
    }
    throw std::invalid_argument("unsupported hash algorithm");
}

std::string make_hash_benchmark_payload(std::size_t block_size) {
    std::string payload(block_size, '\0');
    std::uint64_t state = 0x9e3779b97f4a7c15ULL;
    for (char& ch : payload) {
        state ^= state >> 12U;
        state ^= state << 25U;
        state ^= state >> 27U;
        ch = static_cast<char>((state * 0x2545f4914f6cdd1dULL) & 0xffU);
    }
    return payload;
}

std::string active_hash_hex(ContentHashAlgorithm algorithm, const ActiveHashFile& active) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5:
            return active.md5.hex_digest();
        case ContentHashAlgorithm::sha256:
            return active.sha256.hex_digest();
        case ContentHashAlgorithm::xxh64: {
            std::ostringstream out;
            out << std::hex << std::setfill('0') << std::setw(16) << active.xxh64.value();
            return out.str();
        }
        case ContentHashAlgorithm::xxh3_64:
            return active.xxh3_64.hex_digest();
        case ContentHashAlgorithm::xxh3_128:
            return active.xxh3_128.hex_digest();
    }
    throw std::invalid_argument("unsupported hash algorithm");
}

void reset_active_hash_state(ActiveHashFile& active) {
    active.md5 = Md5State {};
    active.sha256 = Sha256State {};
    active.xxh64 = Hash64State {};
    active.xxh3_64 = Xxh3_64State {};
    active.xxh3_128 = Xxh3_128State {};
}

void update_active_hash(ContentHashAlgorithm algorithm, ActiveHashFile& active, std::string_view data) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5:
            active.md5.update(data);
            break;
        case ContentHashAlgorithm::sha256:
            active.sha256.update(data);
            break;
        case ContentHashAlgorithm::xxh64:
            active.xxh64.update(data);
            break;
        case ContentHashAlgorithm::xxh3_64:
            active.xxh3_64.update(data);
            break;
        case ContentHashAlgorithm::xxh3_128:
            active.xxh3_128.update(data);
            break;
    }
}

void update_active_block_hash(ContentHashAlgorithm algorithm,
                              ActiveHashFile& active,
                              HashInventoryStats& stats,
                              std::uint64_t hash_block_size,
                              std::string_view data) {
    const std::uint64_t block_size = std::max<std::uint64_t>(1, hash_block_size);
    std::string_view remaining = data;
    while (!remaining.empty()) {
        if (active.current_block_expected == 0) {
            const std::uint64_t block_start = active.next_block_index * block_size;
            const std::uint64_t remaining_file_bytes =
                active.file.declared_size > block_start ? active.file.declared_size - block_start : 0;
            active.current_block_expected = std::min<std::uint64_t>(block_size, remaining_file_bytes);
            active.current_block_bytes = 0;
            if (active.current_block_expected == 0) {
                throw std::runtime_error("block hash data exceeds declared file size");
            }
            reset_active_hash_state(active);
        }

        const std::size_t bytes_to_take = static_cast<std::size_t>(
            std::min<std::uint64_t>(active.current_block_expected - active.current_block_bytes, remaining.size()));
        const std::string_view segment = remaining.substr(0, bytes_to_take);
        update_active_hash(algorithm, active, segment);
        active.current_block_bytes += bytes_to_take;
        active.bytes_hashed += bytes_to_take;
        stats.bytes_hashed.fetch_add(bytes_to_take, std::memory_order_relaxed);

        if (active.current_block_bytes == active.current_block_expected) {
            active.block_hashes.push_back(active_hash_hex(algorithm, active));
            ++active.next_block_index;
            active.current_block_bytes = 0;
            active.current_block_expected = 0;
        }

        remaining.remove_prefix(bytes_to_take);
    }
}

void update_active_hash_from_payload(ContentHashAlgorithm algorithm,
                                     ActiveHashFile& active,
                                     HashInventoryStats& stats,
                                     HashMode hash_mode,
                                     std::uint64_t hash_block_size,
                                     std::string_view payload) {
    if (hash_mode == HashMode::blocks) {
        update_active_block_hash(algorithm, active, stats, hash_block_size, payload);
        return;
    }

    update_active_hash(algorithm, active, payload);
    active.bytes_hashed += payload.size();
    stats.bytes_hashed.fetch_add(payload.size(), std::memory_order_relaxed);
}

void release_pending_hash_slots(ActiveHashFile& active, DataSlotPool& hash_data_pool) {
    for (const auto& [offset, handle] : active.pending_data_slots) {
        (void)offset;
        hash_data_pool.release(handle);
    }
    active.pending_data_slots.clear();
}

void drain_ready_hash_data(ContentHashAlgorithm algorithm,
                           ActiveHashFile& active,
                           DataSlotPool& hash_data_pool,
                           HashInventoryStats& stats,
                           HashMode hash_mode,
                           std::uint64_t hash_block_size) {
    while (true) {
        const auto found = active.pending_data_slots.find(active.next_data_offset);
        if (found == active.pending_data_slots.end()) {
            return;
        }

        const DataSlotHandle handle = found->second;
        active.pending_data_slots.erase(found);

        const DataBufTrailer& trailer = hash_data_pool.trailer(handle);
        if (trailer.data_offset != active.next_data_offset) {
            hash_data_pool.release(handle);
            throw std::runtime_error("hash data slot offset does not match expected file offset");
        }

        const std::string_view payload = hash_data_pool.data_view(handle);
        const std::size_t payload_size = payload.size();
        try {
            update_active_hash_from_payload(algorithm, active, stats, hash_mode, hash_block_size, payload);
        } catch (...) {
            hash_data_pool.release(handle);
            throw;
        }
        active.next_data_offset += payload_size;
        hash_data_pool.release(handle);
    }
}

void hash_data_reader_worker(NfsDataReaderConfig reader_config,
                             DataReadFileQueue& file_queue,
                             FlatMetadataWorkQueue& folder_queue,
                             std::vector<std::unique_ptr<HashChunkShardQueue>>& hash_queues,
                             DataSlotPool& hash_data_pool,
                             HashInventoryStats& stats,
                             std::atomic<std::uint64_t>& next_file_id) {
    NfsDataReader reader(std::move(reader_config));
    try {
        while (auto file = take_data_file_work(file_queue)) {
            const std::uint64_t file_id = next_file_id.fetch_add(1, std::memory_order_relaxed);
            HashChunkShardQueue& hash_queue = hash_queue_for_file(hash_queues, file_id);
            HashChunkMessage begin;
            begin.kind = HashChunkKind::begin;
            begin.file_id = file_id;
            begin.file = *file;
            if (!enqueue_hash_chunk_message(hash_queue, std::move(begin))) {
                throw std::runtime_error("hash queue stopped before file read");
            }

            try {
                (void)reader.stream_file_pooled_chunks(*file, hash_data_pool, [&](PooledFileChunk&& chunk) {
                    HashChunkMessage data;
                    data.kind = HashChunkKind::data;
                    data.file_id = file_id;
                    data.offset = chunk.offset;
                    data.handle = chunk.handle;
                    data.data_len = hash_data_pool.trailer(chunk.handle).data_len;
                    data.has_data_slot = true;
                    const std::uint64_t chunk_size = data.data_len;
                    bool enqueued = false;
                    try {
                        enqueued = enqueue_hash_chunk_message(hash_queue, std::move(data));
                    } catch (...) {
                        hash_data_pool.release(chunk.handle);
                        throw;
                    }
                    if (!enqueued) {
                        hash_data_pool.release(chunk.handle);
                        throw std::runtime_error("hash queue stopped while streaming file data");
                    }
                    record_hash_read_bytes(stats, chunk_size);
                });
            } catch (...) {
                HashChunkMessage failed;
                failed.kind = HashChunkKind::failed;
                failed.file_id = file_id;
                if (!enqueue_hash_chunk_message(hash_queue, std::move(failed))) {
                    throw;
                }
                stats.files_failed.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            HashChunkMessage end;
            end.kind = HashChunkKind::end;
            end.file_id = file_id;
            if (!enqueue_hash_chunk_message(hash_queue, std::move(end))) {
                throw std::runtime_error("hash queue stopped before file finalization");
            }
        }
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_data_file_work(file_queue, error);
        fail_flat_folder_work(folder_queue, error);
        fail_hash_chunk_queues(hash_queues, error);
    }
}

void hash_stage_worker(ContentHashAlgorithm algorithm,
                       HashChunkShardQueue& hash_queue,
                       DataSlotPool& hash_data_pool,
                       DataReadFileQueue& file_queue,
                       FlatMetadataWorkQueue& folder_queue,
                       HashInventoryStats& stats,
                       MetadataRecordWriter* record_writer,
                       HashMode hash_mode,
                       std::uint64_t hash_block_size) {
    const std::string algorithm_name = to_string(algorithm);
    std::unordered_map<std::uint64_t, ActiveHashFile> active_files;
    try {
        while (auto message = take_hash_chunk_message(hash_queue)) {
            if (message->kind == HashChunkKind::begin) {
                ActiveHashFile active;
                active.file = std::move(message->file);
                active_files.emplace(message->file_id, std::move(active));
                continue;
            }
            if (message->kind == HashChunkKind::failed) {
                if (const auto failed_file = active_files.find(message->file_id); failed_file != active_files.end()) {
                    release_pending_hash_slots(failed_file->second, hash_data_pool);
                }
                active_files.erase(message->file_id);
                continue;
            }

            auto found = active_files.find(message->file_id);
            if (found == active_files.end()) {
                throw std::runtime_error("hash chunk arrived for an unknown file");
            }
            ActiveHashFile& active = found->second;
            if (message->kind == HashChunkKind::data) {
                if (!message->has_data_slot) {
                    throw std::runtime_error("hash data chunk arrived without a data slot");
                }
                const DataBufTrailer& trailer = hash_data_pool.trailer(message->handle);
                if (message->offset != trailer.data_offset) {
                    hash_data_pool.release(message->handle);
                    throw std::runtime_error("hash data message offset does not match data slot offset");
                }
                if (message->offset < active.next_data_offset) {
                    hash_data_pool.release(message->handle);
                    throw std::runtime_error("hash data chunk arrived for an already hashed offset");
                }
                const auto [inserted_at, inserted] = active.pending_data_slots.emplace(message->offset, message->handle);
                if (!inserted) {
                    (void)inserted_at;
                    hash_data_pool.release(message->handle);
                    throw std::runtime_error("duplicate hash data chunk arrived for a file offset");
                }
                drain_ready_hash_data(algorithm, active, hash_data_pool, stats, hash_mode, hash_block_size);
                continue;
            }

            if (message->kind == HashChunkKind::end) {
                drain_ready_hash_data(algorithm, active, hash_data_pool, stats, hash_mode, hash_block_size);
                if (!active.pending_data_slots.empty()) {
                    release_pending_hash_slots(active, hash_data_pool);
                    throw std::runtime_error("file ended with out-of-order hash data still pending");
                }
                if (active.next_data_offset != active.file.declared_size) {
                    throw std::runtime_error("file ended before all data offsets were hashed");
                }
                if (hash_mode == HashMode::blocks) {
                    if (active.current_block_bytes != 0 || active.current_block_expected != 0) {
                        throw std::runtime_error("file ended with incomplete block hash state");
                    }
                    const std::uint64_t block_size = std::max<std::uint64_t>(1, hash_block_size);
                    const std::uint64_t block_count =
                        active.file.declared_size == 0 ? 0 : (active.file.declared_size + block_size - 1U) / block_size;
                    if (active.block_hashes.size() != static_cast<std::size_t>(block_count)) {
                        throw std::runtime_error("file ended with missing block hashes");
                    }
                    active.file.hash_algorithm.clear();
                    active.file.content_hash.clear();
                    active.file.hash_block_size = block_size;
                    active.file.hash_block_count = block_count;
                    active.file.block_hash_algorithm = algorithm_name;
                    active.file.block_hashes = join_block_hashes(active.block_hashes);
                } else {
                    active.file.hash_algorithm = algorithm_name;
                    active.file.content_hash = active_hash_hex(algorithm, active);
                }
                if (record_writer != nullptr) {
                    record_writer->write_batch(std::vector<FileSpec>{std::move(active.file)}, {});
                }
                stats.files_hashed.fetch_add(1, std::memory_order_relaxed);
                active_files.erase(found);
            }
        }
    } catch (...) {
        for (auto& [file_id, active] : active_files) {
            (void)file_id;
            release_pending_hash_slots(active, hash_data_pool);
        }
        const std::exception_ptr error = std::current_exception();
        fail_hash_chunk_queue(hash_queue, error);
        fail_data_file_work(file_queue, error);
        fail_flat_folder_work(folder_queue, error);
    }
}

HashInventorySnapshot run_parallel_hash_inventory_scan(const NfsMetaReaderConfig& meta_config,
                                                       const NfsDataReaderConfig& data_config,
                                                       ContentHashAlgorithm algorithm,
                                                       HashMode hash_mode,
                                                       std::size_t hash_worker_threads,
                                                       std::size_t max_files_queued,
                                                       std::size_t max_hash_chunks_queued,
                                                       std::uint64_t hash_block_size,
                                                       MetadataRecordWriter* record_writer,
                                                       double max_duration_seconds) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(1, max_files_queued);

    if (max_duration_seconds > 0.0) {
        const auto stop_at = std::chrono::steady_clock::now() +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    HashInventoryStats stats;
    stats.started_at = std::chrono::steady_clock::now();
    stats.folders_found.store(1, std::memory_order_relaxed);

    const std::size_t hash_threads = std::max<std::size_t>(1, hash_worker_threads);
    const std::size_t data_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    const std::size_t reader_in_flight_slots =
        data_threads * std::max<std::size_t>(1, data_config.outstanding_requests);
    const std::size_t queued_hash_slots = std::max<std::size_t>(1, max_hash_chunks_queued);
    const std::size_t hash_data_pool_slots =
        reader_in_flight_slots + queued_hash_slots + hash_threads + 1U;
    DataSlotPool hash_data_pool(0, hash_data_pool_slots);
    std::vector<std::unique_ptr<HashChunkShardQueue>> hash_queues;
    std::vector<std::thread> hash_workers;
    const std::size_t per_hash_queue_entries =
        std::max<std::size_t>(1,
                              (std::max<std::size_t>(1, max_hash_chunks_queued) + hash_threads - 1) /
                                  hash_threads);
    hash_queues.reserve(hash_threads);
    for (std::size_t index = 0; index < hash_threads; ++index) {
        auto queue = std::make_unique<HashChunkShardQueue>();
        queue->max_entries = per_hash_queue_entries;
        hash_queues.push_back(std::move(queue));
    }

    hash_workers.reserve(hash_threads);
    for (std::size_t index = 0; index < hash_threads; ++index) {
        hash_workers.emplace_back(hash_stage_worker,
                                  algorithm,
                                  std::ref(*hash_queues[index]),
                                  std::ref(hash_data_pool),
                                  std::ref(file_queue),
                                  std::ref(folder_queue),
                                  std::ref(stats),
                                  record_writer,
                                  hash_mode,
                                  hash_block_size);
    }

    std::atomic<std::uint64_t> next_hash_file_id {1};
    std::vector<std::thread> data_workers;
    data_workers.reserve(data_threads);
    for (std::size_t index = 0; index < data_threads; ++index) {
        data_workers.emplace_back(hash_data_reader_worker,
                                  data_config,
                                  std::ref(file_queue),
                                  std::ref(folder_queue),
                                  std::ref(hash_queues),
                                  std::ref(hash_data_pool),
                                  std::ref(stats),
                                  std::ref(next_hash_file_id));
    }

    std::vector<std::thread> metadata_workers;
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_hash_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1, meta_config.async_directory_depth),
                                      std::ref(folder_queue),
                                      std::ref(file_queue),
                                      std::ref(stats),
                                      record_writer);
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);
    for (auto& worker : data_workers) {
        worker.join();
    }
    mark_hash_chunk_input_done(hash_queues);
    for (auto& worker : hash_workers) {
        worker.join();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }
    for (auto& queue : hash_queues) {
        if (queue->error) {
            std::rethrow_exception(queue->error);
        }
    }

    return snapshot_hash_inventory_stats(stats);
}

bool receiver_queue_should_pause(std::uint64_t queued_bytes, std::uint64_t capacity_bytes) {
    const WatermarkStatus status = evaluate_watermarks(percentage_of(queued_bytes, capacity_bytes), 0.0);
    return status.soft_throttle || status.hard_stop;
}

bool receiver_queue_should_resume(std::uint64_t queued_bytes, std::uint64_t capacity_bytes) {
    return !receiver_queue_should_pause(queued_bytes, capacity_bytes);
}

void shutdown_receiver_sockets(const ReceiverSharedState& state) {
    if (state.priority_fd >= 0) {
        ::shutdown(state.priority_fd, SHUT_RDWR);
    }
    if (state.data_fd >= 0) {
        ::shutdown(state.data_fd, SHUT_RDWR);
    }
}

std::vector<FileSpec> scan_directories_for_transfer(const std::filesystem::path& source_root, bool recursive) {
    if (is_nfs_url(source_root.string())) {
        auto backend = make_nfs_backend(source_root.string());
        return backend->list_directories(recursive);
    }
    return collect_directory_specs(source_root, recursive);
}

void initialize_folder_report(TransferReport& report, const FileSpec& file) {
    const std::string folder_path = parent_path(file.rel_path);
    auto [it, inserted] = report.folders.emplace(folder_path, FolderRecord{});
    FolderRecord& folder = it->second;
    if (inserted) {
        folder.rel_path = folder_path;
        folder.parent_hash = folder_hash_for_path(parent_path(folder_path));
        folder.md_hash = path_hash(base_name(folder_path), folder.parent_hash);
        folder.state = FolderState::reading;
        folder.remote_state = FolderState::receiving;
    }
    ++folder.files_discovered;
    ++folder.files_total;
    folder.flat_size_bytes += file.declared_size;
}

void finalize_folder_report(TransferReport& report) {
    for (auto& [_, folder] : report.folders) {
        folder.state = FolderState::done;
        folder.remote_state = FolderState::done;
    }
}

void receiver_priority_loop(ReceiverSharedState& state) {
    auto target_backend = make_nfs_backend(state.runtime.target_root.string());
    auto target_writer = make_target_writer_backend(state.runtime.target_root.string());
    for (;;) {
        PriorityMessageType type = PriorityMessageType::session_end;
        std::string payload;
        if (!read_priority_payload(state.priority_fd, type, payload)) {
            break;
        }

        if (type == PriorityMessageType::session_start) {
            const SessionStartInfo info = decode_session_start(payload);
            state.session_large_chunk_bytes = info.large_chunk_bytes;
            state.skip_verify = info.skip_verify;
            continue;
        }
        if (type == PriorityMessageType::session_end) {
            std::lock_guard<std::mutex> lock(state.metadata_mutex);
            state.session_done = true;
            state.metadata_cv.notify_all();
            return;
        }
        if (type == PriorityMessageType::directory_record) {
            const FileSpec spec = make_directory_spec(decode_directory_record(payload));
            target_writer->ensure_directory(spec);
            state.directory_specs.push_back(spec);
            continue;
        }
        if (type != PriorityMessageType::file_record) {
            throw std::runtime_error("unexpected priority message type");
        }

        const FileRecordMessage record = decode_file_record(payload);
        const bool skip = target_matches_record(*target_backend, record);
        if (!skip) {
            std::lock_guard<std::mutex> lock(state.metadata_mutex);
            state.pending_records[record.file_id] = record;
            state.metadata_cv.notify_all();
        }

        const FileDecisionMessage decision{record.file_id, skip};
        std::lock_guard<std::mutex> write_lock(state.priority_write_mutex);
        send_file_decision(state.priority_fd, decision);
    }
}

bool receiver_has_all_pending_records(const ReceiverSharedState& state, const std::vector<std::uint64_t>& file_ids) {
    return std::all_of(file_ids.begin(), file_ids.end(), [&state](std::uint64_t file_id) {
        return state.pending_records.find(file_id) != state.pending_records.end();
    });
}

void send_receiver_file_ack(ReceiverSharedState& state,
                            const FileRecordMessage& record,
                            bool verified,
                            std::uint64_t bytes_written) {
    {
        std::lock_guard<std::mutex> write_lock(state.priority_write_mutex);
        send_file_ack(state.priority_fd, FileAckMessage{record.file_id, verified, bytes_written});
    }
    {
        std::lock_guard<std::mutex> lock(state.metadata_mutex);
        state.pending_records.erase(record.file_id);
    }
}

void process_packed_small_file_slot(ReceiverSharedState& state,
                                    TargetWriterBackend& target_writer,
                                    DataSlotPool& slot_pool,
                                    const DataSlotHandle& handle) {
    const auto entries = decode_packed_small_file_entries(slot_pool, handle);
    for (const auto& entry : entries) {
        FileRecordMessage record;
        {
            std::lock_guard<std::mutex> lock(state.metadata_mutex);
            const auto it = state.pending_records.find(entry.record.file_id);
            if (it == state.pending_records.end()) {
                throw std::runtime_error("packed small file arrived without matching file record");
            }
            record = it->second;
        }
        if (entry.data.size() != record.size) {
            throw std::runtime_error("packed small file size does not match file record");
        }

        FileSpec spec;
        spec.rel_path = record.rel_path;
        spec.mtime = record.mtime;
        spec.mode = record.mode;
        spec.uid = record.uid;
        spec.gid = record.gid;
        spec.declared_size = record.size;

        bool verified = false;
        try {
            target_writer.write_chunk(spec, entry.data, 0);
            target_writer.finish_file(spec);
            verified =
                state.skip_verify ||
                target_writer.file_hash(record.rel_path) == entry.data_hash;
        } catch (...) {
            target_writer.abort_file(record.rel_path);
        }

        send_receiver_file_ack(state,
                               record,
                               verified,
                               verified ? static_cast<std::uint64_t>(entry.data.size()) : 0U);
    }
}

void receiver_data_loop(ReceiverSharedState& state, int data_fd) {
    ReceiverChunkQueue queue;
    queue.capacity_bytes = receiver_queue_capacity_bytes(state);
    queue.queue.reset(std::max<std::size_t>(2,
                                            static_cast<std::size_t>(
                                                queue.capacity_bytes /
                                                    std::max<std::uint64_t>(1, state.session_large_chunk_bytes)) +
                                                2U));
    DataSlotPool slot_pool(state.small_pool_slots, state.large_pool_slots);

    std::thread writer_thread([&] {
        std::unordered_map<std::uint64_t, ReceiverFileContext> contexts;
        auto target_writer = make_target_writer_backend(state.runtime.target_root.string());

        try {
            for (;;) {
                ReceiverQueuedChunk work;
                bool should_exit = false;
                bool need_resume = false;
                {
                    std::unique_lock<std::mutex> lock(queue.mutex);
                    queue.cv_not_empty.wait(lock, [&queue] {
                        return queue.worker_error != nullptr || queue.input_done || !queue.queue.empty();
                    });
                    if (queue.worker_error != nullptr) {
                        return;
                    }
                    if (queue.queue.empty()) {
                        should_exit = queue.input_done;
                    } else {
                        work = queue.queue.pop();
                        queue.queued_bytes -= slot_pool.trailer(work.handle).data_len;
                        if (queue.paused && receiver_queue_should_resume(queue.queued_bytes, queue.capacity_bytes)) {
                            queue.paused = false;
                            need_resume = true;
                        }
                        queue.cv_not_full.notify_all();
                    }
                }

                if (need_resume) {
                    std::lock_guard<std::mutex> write_lock(state.priority_write_mutex);
                    send_resume(state.priority_fd);
                }
                if (should_exit) {
                    return;
                }

                if (is_packed_small_file_slot(slot_pool, work.handle)) {
                    try {
                        process_packed_small_file_slot(state, *target_writer, slot_pool, work.handle);
                    } catch (...) {
                        slot_pool.release(work.handle);
                        throw;
                    }
                    slot_pool.release(work.handle);
                    continue;
                }

                auto& context = contexts[slot_pool.trailer(work.handle).file_id];
                if (context.bytes_written == 0) {
                    context.record = work.record;
                }

                FileSpec spec;
                spec.rel_path = work.record.rel_path;
                spec.mtime = work.record.mtime;
                spec.mode = work.record.mode;
                spec.uid = work.record.uid;
                spec.gid = work.record.gid;
                spec.declared_size = work.record.size;

                try {
                    target_writer->write_chunk(spec,
                                               slot_pool.data_view(work.handle),
                                               slot_pool.trailer(work.handle).data_offset);
                } catch (...) {
                    target_writer->abort_file(work.record.rel_path);
                    send_receiver_file_ack(state, work.record, false, 0);
                    contexts.erase(slot_pool.trailer(work.handle).file_id);
                    slot_pool.release(work.handle);
                    continue;
                }
                context.bytes_written += slot_pool.trailer(work.handle).data_len;

                if ((slot_pool.trailer(work.handle).flags & kFlagLastChunk) == 0U) {
                    slot_pool.release(work.handle);
                    continue;
                }

                try {
                    target_writer->finish_file(spec);
                } catch (...) {
                    target_writer->abort_file(work.record.rel_path);
                    send_receiver_file_ack(state, work.record, false, 0);
                    contexts.erase(slot_pool.trailer(work.handle).file_id);
                    slot_pool.release(work.handle);
                    continue;
                }
                const bool verified =
                    state.skip_verify ||
                    target_writer->file_hash(work.record.rel_path) == slot_pool.trailer(work.handle).data_hash;
                send_receiver_file_ack(state, work.record, verified, verified ? context.bytes_written : 0U);
                contexts.erase(slot_pool.trailer(work.handle).file_id);
                slot_pool.release(work.handle);
            }
        } catch (...) {
            {
                std::lock_guard<std::mutex> lock(queue.mutex);
                queue.worker_error = std::current_exception();
                queue.cv_not_full.notify_all();
                queue.cv_not_empty.notify_all();
            }
            shutdown_receiver_sockets(state);
        }
    });

    try {
        for (;;) {
            DataSlotHandle handle;
            if (!read_data_slot(data_fd, slot_pool, handle)) {
                break;
            }

            FileRecordMessage record;
            {
                std::unique_lock<std::mutex> lock(state.metadata_mutex);
                if (is_packed_small_file_slot(slot_pool, handle)) {
                    const auto packed_ids = packed_small_file_ids(slot_pool, handle);
                    state.metadata_cv.wait(lock, [&state, &packed_ids] {
                        return state.session_done || receiver_has_all_pending_records(state, packed_ids);
                    });
                    if (!receiver_has_all_pending_records(state, packed_ids)) {
                        slot_pool.release(handle);
                        throw std::runtime_error("received packed data without matching file records");
                    }
                } else {
                    state.metadata_cv.wait(lock, [&state, &slot_pool, &handle] {
                        return state.session_done ||
                               state.pending_records.find(slot_pool.trailer(handle).file_id) !=
                                   state.pending_records.end();
                    });
                    const auto it = state.pending_records.find(slot_pool.trailer(handle).file_id);
                    if (it == state.pending_records.end()) {
                        slot_pool.release(handle);
                        throw std::runtime_error("received data chunk without matching file record");
                    }
                    record = it->second;
                }
            }

            const std::uint64_t chunk_bytes = slot_pool.trailer(handle).data_len;
            bool need_pause = false;
            {
                std::unique_lock<std::mutex> lock(queue.mutex);
                const std::uint64_t queue_limit = std::max(queue.capacity_bytes, chunk_bytes);
                queue.cv_not_full.wait(lock, [&queue, chunk_bytes, queue_limit] {
                    return queue.worker_error != nullptr || queue.queued_bytes + chunk_bytes <= queue_limit;
                });
                if (queue.worker_error != nullptr) {
                    slot_pool.release(handle);
                    std::rethrow_exception(queue.worker_error);
                }
                queue.queue.push(ReceiverQueuedChunk{handle, record});
                queue.queued_bytes += chunk_bytes;
                if (!queue.paused && receiver_queue_should_pause(queue.queued_bytes, queue.capacity_bytes)) {
                    queue.paused = true;
                    need_pause = true;
                }
                queue.cv_not_empty.notify_one();
            }

            if (need_pause) {
                std::lock_guard<std::mutex> write_lock(state.priority_write_mutex);
                send_pause(state.priority_fd);
            }
        }
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(queue.mutex);
            queue.input_done = true;
            queue.cv_not_empty.notify_all();
            queue.cv_not_full.notify_all();
        }
        writer_thread.join();
        throw;
    }

    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.input_done = true;
        queue.cv_not_empty.notify_all();
        queue.cv_not_full.notify_all();
    }
    writer_thread.join();

    if (queue.worker_error != nullptr) {
        std::rethrow_exception(queue.worker_error);
    }
}

bool poll_priority_message(int fd, int timeout_ms, PriorityMessageType& type, std::string& payload) {
    struct pollfd poll_state {
        fd, POLLIN, 0
    };
    const int ready = ::poll(&poll_state, 1, timeout_ms);
    if (ready < 0) {
        throw std::system_error(errno, std::generic_category(), "poll failed on priority socket");
    }
    if (ready == 0) {
        return false;
    }
    if ((poll_state.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        throw std::runtime_error("priority socket closed unexpectedly");
    }
    return read_priority_payload(fd, type, payload);
}

void consume_sender_priority_message(PriorityMessageType type,
                                     std::string_view payload,
                                     bool& paused,
                                     std::deque<FileAckMessage>& pending_acks) {
    switch (type) {
        case PriorityMessageType::pause:
            paused = true;
            return;
        case PriorityMessageType::resume:
            paused = false;
            return;
        case PriorityMessageType::file_ack:
            pending_acks.push_back(decode_file_ack(payload));
            paused = false;
            return;
        default:
            throw std::runtime_error("unexpected priority message while sending data");
    }
}

void drain_sender_priority_events(int fd,
                                  int initial_timeout_ms,
                                  bool& paused,
                                  std::deque<FileAckMessage>& pending_acks) {
    PriorityMessageType type = PriorityMessageType::session_end;
    std::string payload;
    if (!poll_priority_message(fd, initial_timeout_ms, type, payload)) {
        return;
    }
    consume_sender_priority_message(type, payload, paused, pending_acks);
    while (poll_priority_message(fd, 0, type, payload)) {
        consume_sender_priority_message(type, payload, paused, pending_acks);
    }
}

void wait_for_sender_resume(int fd, bool& paused, std::deque<FileAckMessage>& pending_acks) {
    while (paused && pending_acks.empty()) {
        drain_sender_priority_events(fd, -1, paused, pending_acks);
    }
}

FileAckMessage wait_for_sender_ack(int fd, bool& paused, std::deque<FileAckMessage>& pending_acks) {
    if (!pending_acks.empty()) {
        const FileAckMessage ack = pending_acks.front();
        pending_acks.pop_front();
        return ack;
    }
    for (;;) {
        drain_sender_priority_events(fd, -1, paused, pending_acks);
        if (!pending_acks.empty()) {
            const FileAckMessage ack = pending_acks.front();
            pending_acks.pop_front();
            return ack;
        }
    }
}

std::size_t count_chunks_for_size(std::uint64_t total_bytes, std::size_t chunk_bytes) {
    if (chunk_bytes == 0U) {
        throw std::invalid_argument("chunk bytes must be positive");
    }
    if (total_bytes == 0U) {
        return 1U;
    }
    return static_cast<std::size_t>((total_bytes + static_cast<std::uint64_t>(chunk_bytes) - 1U) /
                                    static_cast<std::uint64_t>(chunk_bytes));
}

void populate_slot_for_chunk(DataSlotPool& pool,
                             const DataSlotHandle& handle,
                             const RecBuf& record,
                             std::uint64_t offset,
                             std::string_view bytes,
                             bool is_small,
                             bool is_last,
                             std::uint64_t data_hash) {
    DataBufTrailer& trailer = pool.trailer(handle);
    trailer.file_id = record.own_hash;
    trailer.folder_hash = record.folder_hash;
    trailer.data_offset = offset;
    trailer.data_len = bytes.size();
    trailer.file_size = record.size;
    trailer.data_hash = is_last ? data_hash : 0;
    trailer.mtime = record.mtime;
    trailer.mode = record.mode;
    trailer.uid = record.uid;
    trailer.gid = record.gid;
    trailer.chunk_hash = chunk_hash32(bytes);
    trailer.flags = is_small ? kFlagSmallFile : 0U;
    if (is_last) {
        trailer.flags |= kFlagLastChunk | kFlagHashValid;
    }
    trailer.rel_path = record.rel_path;
    trailer.slot_valid = kSlotValid;
}

struct SlotStreamingResult {
    std::uint64_t data_hash = 0;
    std::size_t chunk_count = 0;
};

template <typename Consumer>
SlotStreamingResult stream_content_slots(DataSlotPool& pool,
                                         const FileSpec& file,
                                         const EngineConfig& config,
                                         Consumer&& consume_slot) {
    const RecBuf record = make_recbuf(file);
    const bool is_small = record.size <= config.small_file_threshold;
    const std::size_t chunk_bytes = is_small ? config.small_file_threshold : config.large_chunk_bytes;
    Hash64State hasher;
    std::size_t chunk_count = 0;

    for (std::uint64_t offset = 0; offset < record.size || (record.size == 0 && offset == 0); offset += chunk_bytes) {
        const std::size_t len = record.size == 0
                                    ? 0
                                    : std::min<std::uint64_t>(static_cast<std::uint64_t>(chunk_bytes), record.size - offset);
        const std::string_view bytes(file.content.data() + static_cast<std::size_t>(offset), len);
        if (!bytes.empty()) {
            hasher.update(bytes);
        }

        DataSlotHandle handle = pool.acquire_or_throw(is_small ? DataSlotClass::small : DataSlotClass::large, len);
        if (len != 0U) {
            std::memcpy(pool.data(handle), bytes.data(), len);
        }
        const bool is_last = offset + len >= record.size;
        populate_slot_for_chunk(pool, handle, record, offset, bytes, is_small, is_last, hasher.value());
        consume_slot(handle);
        ++chunk_count;
    }

    return SlotStreamingResult{hasher.value(), chunk_count};
}

template <typename Consumer>
SlotStreamingResult stream_local_file_slots(DataSlotPool& pool,
                                            const std::filesystem::path& absolute_path,
                                            const FileSpec& file,
                                            const EngineConfig& config,
                                            Consumer&& consume_slot) {
    std::ifstream input(absolute_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open source file: " + absolute_path.string());
    }

    const RecBuf record = make_recbuf(file);
    const bool is_small = record.size <= config.small_file_threshold;
    const std::size_t chunk_bytes = is_small ? config.small_file_threshold : config.large_chunk_bytes;
    Hash64State hasher;
    std::size_t chunk_count = 0;

    for (std::uint64_t offset = 0; offset < record.size || (record.size == 0 && offset == 0); offset += chunk_bytes) {
        const std::size_t len = record.size == 0
                                    ? 0
                                    : std::min<std::uint64_t>(static_cast<std::uint64_t>(chunk_bytes), record.size - offset);
        DataSlotHandle handle = pool.acquire_or_throw(is_small ? DataSlotClass::small : DataSlotClass::large, len);
        if (len != 0U) {
            input.read(pool.data(handle), static_cast<std::streamsize>(len));
            if (static_cast<std::size_t>(input.gcount()) != len) {
                pool.release(handle);
                throw std::runtime_error("failed to read source file chunk: " + absolute_path.string());
            }
            hasher.update(std::string_view(pool.data(handle), len));
        }
        const bool is_last = offset + len >= record.size;
        populate_slot_for_chunk(pool,
                                handle,
                                record,
                                offset,
                                std::string_view(pool.data(handle), len),
                                is_small,
                                is_last,
                                hasher.value());
        consume_slot(handle);
        ++chunk_count;
    }

    if (record.size != 0U && !input.eof()) {
        char extra = '\0';
        input.read(&extra, 1);
        if (input.gcount() != 0) {
            throw std::runtime_error("source file changed while streaming: " + absolute_path.string());
        }
    }

    return SlotStreamingResult{hasher.value(), chunk_count};
}

CachedFilePlan cache_file_for_transfer(DataCacher& cacher,
                                       DataSlotPool& slot_pool,
                                       const std::filesystem::path& absolute_path,
                                       const FileSpec& file,
                                       const EngineConfig& config) {
    const RecBuf record = make_recbuf(file);
    const SlotStreamingResult streamed =
        stream_local_file_slots(slot_pool, absolute_path, file, config, [&](const DataSlotHandle& handle) {
            cacher.cache_slot(slot_pool, handle);
            slot_pool.release(handle);
        });

    const std::uint64_t data_hash = streamed.data_hash;
    cacher.set_cached_file_hash(record.own_hash, data_hash);

    CachedFilePlan plan;
    plan.data_hash = data_hash;
    plan.chunk_count = streamed.chunk_count;
    plan.entry_ids = cacher.cached_entries_for(record.own_hash);
    return plan;
}

CachedFilePlan cache_file_for_transfer(DataCacher& cacher,
                                       DataSlotPool& slot_pool,
                                       const FileSpec& file,
                                       const EngineConfig& config) {
    const RecBuf record = make_recbuf(file);
    const SlotStreamingResult streamed = stream_content_slots(slot_pool, file, config, [&](const DataSlotHandle& handle) {
        cacher.cache_slot(slot_pool, handle);
        slot_pool.release(handle);
    });

    CachedFilePlan plan;
    plan.data_hash = streamed.data_hash;
    plan.chunk_count = streamed.chunk_count;
    plan.entry_ids = cacher.cached_entries_for(record.own_hash);
    return plan;
}

bool contains_path(const std::unordered_map<std::string, std::size_t>& map,
                   std::string_view path,
                   std::size_t attempt) {
    const auto it = map.find(normalize_path(path));
    if (it == map.end()) {
        return false;
    }
    return attempt <= it->second;
}

void send_content_file_slots(int data_fd,
                             DataSlotPool& slot_pool,
                             const FileSpec& file,
                             const EngineConfig& config,
                             int priority_fd,
                             bool& paused,
                             std::deque<FileAckMessage>& pending_acks,
                             TransferReport& report) {
    (void)stream_content_slots(slot_pool, file, config, [&](const DataSlotHandle& handle) {
        drain_sender_priority_events(priority_fd, 0, paused, pending_acks);
        wait_for_sender_resume(priority_fd, paused, pending_acks);
        send_data_slot(data_fd, slot_pool, handle);
        ++report.chunks_sent;
        slot_pool.release(handle);
    });
}

std::uint64_t send_local_file_slots(int data_fd,
                                    DataSlotPool& slot_pool,
                                    const std::filesystem::path& absolute_path,
                                    const FileSpec& file,
                                    const EngineConfig& config,
                                    int priority_fd,
                                    bool& paused,
                                    std::deque<FileAckMessage>& pending_acks,
                                    TransferReport& report) {
    return stream_local_file_slots(slot_pool, absolute_path, file, config, [&](const DataSlotHandle& handle) {
               drain_sender_priority_events(priority_fd, 0, paused, pending_acks);
               wait_for_sender_resume(priority_fd, paused, pending_acks);
               send_data_slot(data_fd, slot_pool, handle);
               ++report.chunks_sent;
               slot_pool.release(handle);
           })
        .data_hash;
}

std::uint64_t write_packed_small_file_entry(char* payload,
                                            std::size_t& offset,
                                            const PreparedTransfer& prepared) {
    const std::uint64_t data_len = prepared.record.size;
    const std::uint32_t path_len = static_cast<std::uint32_t>(prepared.record.rel_path.size());
    const std::size_t entry_start = offset;
    const std::size_t data_start = entry_start + kPackedSmallFileEntryFixedBytes + path_len;
    char* const data_begin = payload + data_start;

    if (data_len != 0U) {
        if (prepared.content_loaded) {
            if (prepared.file.content.size() != data_len) {
                throw std::runtime_error("prepared small file content size mismatch");
            }
            std::memcpy(data_begin, prepared.file.content.data(), static_cast<std::size_t>(data_len));
        } else {
            std::ifstream input(prepared.source_path, std::ios::binary);
            if (!input) {
                throw std::runtime_error("failed to open source file: " + prepared.source_path.string());
            }
            input.read(data_begin, static_cast<std::streamsize>(data_len));
            if (input.gcount() != static_cast<std::streamsize>(data_len)) {
                throw std::runtime_error("failed to read complete source file: " + prepared.source_path.string());
            }
        }
    }

    const std::string_view data_view(data_begin, static_cast<std::size_t>(data_len));
    const std::uint64_t data_hash = hash64(data_view);

    std::size_t header = entry_start;
    write_u64_be(payload, header, prepared.record.file_id);
    write_u64_be(payload, header, data_len);
    write_u64_be(payload, header, prepared.record.size);
    write_u64_be(payload, header, data_hash);
    write_u64_be(payload, header, prepared.record.mtime);
    write_u32_be(payload, header, prepared.record.mode);
    write_u32_be(payload, header, prepared.record.uid);
    write_u32_be(payload, header, prepared.record.gid);
    write_u32_be(payload, header, path_len);
    if (path_len != 0U) {
        std::memcpy(payload + header, prepared.record.rel_path.data(), path_len);
        header += path_len;
    }
    if (header != data_start) {
        throw std::runtime_error("packed small file header size mismatch");
    }
    offset = data_start + static_cast<std::size_t>(data_len);
    return data_hash;
}

std::vector<std::uint64_t> send_packed_small_file_batch(int data_fd,
                                                       DataSlotPool& slot_pool,
                                                       std::vector<PreparedTransfer>& batch,
                                                       const EngineConfig& config,
                                                       int priority_fd,
                                                       bool& paused,
                                                       std::deque<FileAckMessage>& pending_acks,
                                                       TransferReport& report) {
    if (batch.empty()) {
        return {};
    }
    std::size_t payload_bytes = kPackedSmallFileCountBytes;
    for (const auto& prepared : batch) {
        payload_bytes += packed_small_file_entry_bytes(prepared);
    }
    if (payload_bytes > config.large_chunk_bytes) {
        throw std::runtime_error("packed small file batch exceeds large buffer capacity");
    }

    DataSlotHandle handle = slot_pool.acquire_or_throw(DataSlotClass::large, payload_bytes);
    std::vector<std::uint64_t> hashes;
    hashes.reserve(batch.size());
    try {
        char* payload = slot_pool.data(handle);
        std::size_t offset = kPackedSmallFileCountBytes;
        for (const auto& prepared : batch) {
            hashes.push_back(write_packed_small_file_entry(payload, offset, prepared));
        }
        if (offset != payload_bytes) {
            throw std::runtime_error("packed small file batch size mismatch");
        }
        write_u32_be_at(payload, 0, static_cast<std::uint32_t>(batch.size()));

        DataBufTrailer& trailer = slot_pool.trailer(handle);
        trailer.file_id = 0;
        trailer.folder_hash = 0;
        trailer.data_offset = 0;
        trailer.data_len = payload_bytes;
        trailer.file_size = payload_bytes;
        trailer.data_hash = 0;
        trailer.mtime = 0;
        trailer.mode = 0;
        trailer.uid = 0;
        trailer.gid = 0;
        trailer.chunk_hash = chunk_hash32(std::string_view(payload, payload_bytes));
        trailer.flags = kFlagPackedSmallFiles | kFlagLastChunk;
        trailer.rel_path.clear();
        trailer.slot_valid = kSlotValid;

        drain_sender_priority_events(priority_fd, 0, paused, pending_acks);
        wait_for_sender_resume(priority_fd, paused, pending_acks);
        send_data_slot(data_fd, slot_pool, handle);
        ++report.chunks_sent;
    } catch (...) {
        slot_pool.release(handle);
        throw;
    }
    slot_pool.release(handle);
    return hashes;
}

void record_completed_prepared_transfer(const PreparedTransfer& prepared,
                                        std::uint64_t data_hash,
                                        std::size_t chunk_count,
                                        const FileAckMessage& ack,
                                        TransferReport& report) {
    FileOutcome outcome;
    outcome.rel_path = prepared.file.rel_path;
    outcome.size = prepared.file.declared_size;
    outcome.sender_state = ack.hash_verified ? FileState::done : FileState::failed;
    outcome.receiver_state = ack.hash_verified ? FileState::done : FileState::failed;
    outcome.chunk_count = chunk_count;
    outcome.hash_verified = ack.hash_verified;
    outcome.data_hash = data_hash;

    FolderRecord& folder = report.folders[parent_path(prepared.file.rel_path)];
    if (!ack.hash_verified) {
        outcome.diff = DiffKind::failed;
        ++report.files_failed;
    } else {
        outcome.diff = DiffKind::new_file;
        ++report.files_transferred;
        report.bytes_transferred += ack.bytes_written;
        folder.bytes_transferred += ack.bytes_written;
        ++folder.files_completed;
        ++folder.files_received;
        ++folder.files_written;
        const auto source_snapshot = make_snapshot(prepared.file, outcome.data_hash, 'S');
        const auto target_snapshot = make_snapshot(prepared.file, outcome.data_hash, 'T');
        report.source_scan_rows.push_back(source_snapshot);
        report.target_scan_rows.push_back(target_snapshot);
    }
    report.files[prepared.file.rel_path] = outcome;
}

void record_sender_ack(const FileAckMessage& ack,
                       std::unordered_map<std::uint64_t, AwaitingAckTransfer>& awaiting_acks,
                       TransferReport& report) {
    const auto it = awaiting_acks.find(ack.file_id);
    if (it == awaiting_acks.end()) {
        throw std::runtime_error("received ack for unknown file_id: " + std::to_string(ack.file_id));
    }
    record_completed_prepared_transfer(it->second.prepared,
                                       it->second.data_hash,
                                       it->second.chunk_count,
                                       ack,
                                       report);
    awaiting_acks.erase(it);
}

void consume_sender_acks(std::deque<FileAckMessage>& pending_acks,
                         std::unordered_map<std::uint64_t, AwaitingAckTransfer>& awaiting_acks,
                         TransferReport& report) {
    while (!pending_acks.empty()) {
        const FileAckMessage ack = pending_acks.front();
        pending_acks.pop_front();
        record_sender_ack(ack, awaiting_acks, report);
    }
}

PreparedTransfer prepare_transfer_file(const PendingTransferFile& pending,
                                       bool remote_source,
                                       const std::filesystem::path& source_root,
                                       NfsDataReader& data_reader,
                                       DataCacher& cacher,
                                       DataSlotPool& cache_slots,
                                       const EngineConfig& config,
                                       std::uint64_t cache_file_threshold_bytes,
                                       bool enable_cache) {
    PreparedTransfer prepared;
    prepared.file = pending.file;
    prepared.record = pending.record;

    if (enable_cache &&
        (cache_file_threshold_bytes == 0 || prepared.file.declared_size >= cache_file_threshold_bytes)) {
        if (remote_source) {
            FileSpec loaded = data_reader.load_file(prepared.file.rel_path);
            prepared.file.content = std::move(loaded.content);
            prepared.content_loaded = true;
            const CachedFilePlan plan = cache_file_for_transfer(cacher, cache_slots, prepared.file, config);
            prepared.cached = true;
            prepared.data_hash = plan.data_hash;
            prepared.chunk_count = plan.chunk_count;
            prepared.cached_entry_ids = plan.entry_ids;
            return prepared;
        }

        prepared.source_path = source_root / prepared.file.rel_path;
        const CachedFilePlan plan = cache_file_for_transfer(cacher, cache_slots, prepared.source_path, prepared.file, config);
        prepared.cached = true;
        prepared.data_hash = plan.data_hash;
        prepared.chunk_count = plan.chunk_count;
        prepared.cached_entry_ids = plan.entry_ids;
        return prepared;
    }

    if (remote_source) {
        prepared.file = data_reader.load_file(prepared.file.rel_path);
        prepared.content_loaded = true;
    } else {
        prepared.source_path = source_root / prepared.file.rel_path;
    }

    const bool is_small = prepared.record.size <= config.small_file_threshold;
    const std::size_t chunk_bytes = is_small ? config.small_file_threshold : config.large_chunk_bytes;
    prepared.chunk_count = count_chunks_for_size(prepared.record.size, chunk_bytes);
    if (prepared.content_loaded) {
        prepared.data_hash = hash64(prepared.file.content);
    }
    return prepared;
}

void produce_prepared_transfers(PreparedTransferQueue& queue,
                                const std::vector<PendingTransferFile>& pending_files,
                                bool remote_source,
                                const std::filesystem::path& source_root,
                                NfsDataReader& data_reader,
                                DataCacher& cacher,
                                DataSlotPool& cache_slots,
                                const EngineConfig& config,
                                std::uint64_t cache_file_threshold_bytes,
                                bool enable_cache) {
    try {
        for (const auto& pending : pending_files) {
            PreparedTransfer prepared = prepare_transfer_file(pending,
                                                             remote_source,
                                                             source_root,
                                                             data_reader,
                                                             cacher,
                                                             cache_slots,
                                                             config,
                                                             cache_file_threshold_bytes,
                                                             enable_cache);

            std::unique_lock<std::mutex> lock(queue.mutex);
            queue.cv_not_full.wait(lock, [&queue] {
                return queue.stop || !queue.queue.full();
            });
            if (queue.stop) {
                break;
            }
            queue.queue.push(std::move(prepared));
            queue.cv_not_empty.notify_one();
        }
    } catch (...) {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.producer_error = std::current_exception();
    }

    std::lock_guard<std::mutex> lock(queue.mutex);
    queue.input_done = true;
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

bool pop_prepared_transfer(PreparedTransferQueue& queue, PreparedTransfer& prepared) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_empty.wait(lock, [&queue] {
        return queue.producer_error != nullptr || queue.input_done || !queue.queue.empty();
    });
    if (queue.producer_error != nullptr) {
        std::rethrow_exception(queue.producer_error);
    }
    if (queue.queue.empty()) {
        return false;
    }

    prepared = queue.queue.pop();
    queue.cv_not_full.notify_one();
    return true;
}

bool try_pop_prepared_transfer(PreparedTransferQueue& queue, PreparedTransfer& prepared) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    if (queue.producer_error != nullptr) {
        std::rethrow_exception(queue.producer_error);
    }
    if (queue.queue.empty()) {
        return false;
    }
    prepared = queue.queue.pop();
    lock.unlock();
    queue.cv_not_full.notify_one();
    return true;
}

void write_text_file(const std::filesystem::path& path, const std::string& contents) {
    ensure_parent_directories(path);
    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("failed to open output file: " + path.string());
    }
    output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    if (!output) {
        throw std::runtime_error("failed to write output file: " + path.string());
    }
}

std::string read_text_file(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open input file: " + path.string());
    }
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

}  // namespace

SenderRuntimeConfig::SenderRuntimeConfig()
    : SenderRuntimeConfig(load_sender_runtime_config(ConfigStore{})) {}

SenderRuntimeConfig::SenderRuntimeConfig(std::filesystem::path source_root,
                                         std::string remote_host,
                                         std::uint16_t priority_port,
                                         std::uint16_t data_port,
                                         bool recursive,
                                         std::filesystem::path cache_root,
                                         std::uint64_t cache_file_threshold_bytes)
    : source_root(std::move(source_root)),
      remote_host(std::move(remote_host)),
      priority_port(priority_port),
      data_port(data_port),
      recursive(recursive),
      cache_root(std::move(cache_root)),
      cache_file_threshold_bytes(cache_file_threshold_bytes) {}

ReceiverRuntimeConfig::ReceiverRuntimeConfig()
    : ReceiverRuntimeConfig(load_receiver_runtime_config(ConfigStore{})) {}

ReceiverRuntimeConfig::ReceiverRuntimeConfig(std::filesystem::path target_root,
                                             std::string bind_host,
                                             std::uint16_t priority_port,
                                             std::uint16_t data_port,
                                             std::uint64_t backpressure_window_bytes,
                                             std::uint32_t backpressure_pause_ms)
    : target_root(std::move(target_root)),
      bind_host(std::move(bind_host)),
      priority_port(priority_port),
      data_port(data_port),
      backpressure_window_bytes(backpressure_window_bytes),
      backpressure_pause_ms(backpressure_pause_ms) {}

SenderRuntimeConfig load_sender_runtime_config(const ConfigStore& config) {
    const ConfigSection values = config.section("runtime.sender");
    return SenderRuntimeConfig({},
                               config_string(values, "remote_host"),
                               config_u16(values, "priority_port"),
                               config_u16(values, "data_port"),
                               config_bool(values, "recursive"),
                               {},
                               config_u64(values, "cache_file_threshold_bytes"));
}

ReceiverRuntimeConfig load_receiver_runtime_config(const ConfigStore& config) {
    const ConfigSection values = config.section("runtime.receiver");
    return ReceiverRuntimeConfig({},
                                 config_string(values, "bind_host"),
                                 config_u16(values, "priority_port"),
                                 config_u16(values, "data_port"),
                                 config_u64(values, "backpressure_window_bytes"),
                                 config_u32(values, "backpressure_pause_ms"));
}

TransferEngine::TransferEngine(EngineConfig config, ConfigStore config_store)
    : config_(std::move(config)),
      config_store_(std::move(config_store)) {
    if (config_.small_file_threshold == 0 || config_.large_chunk_bytes == 0) {
        throw std::invalid_argument("chunk sizes must be positive");
    }
    if (config_.small_file_threshold > kSmallFileThreshold) {
        throw std::invalid_argument("small file threshold exceeds fixed slot capacity");
    }
    if (config_.large_chunk_bytes > kLargeChunkBytes) {
        throw std::invalid_argument("large chunk bytes exceed fixed slot capacity");
    }
}

TransferReport TransferEngine::run(const std::vector<FileSpec>& source_files,
                                   const ScanIndex* source_scan,
                                   const ScanIndex* target_scan) const {
    DataSlotPool slot_pool(config_.small_pool_slots, config_.large_pool_slots);

    std::vector<FileSpec> normalized_files = source_files;
    for (auto& file : normalized_files) {
        file.rel_path = normalize_path(file.rel_path);
    }
    std::sort(normalized_files.begin(), normalized_files.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });

    std::map<std::string, std::vector<FileSpec>> files_by_folder;
    for (const auto& file : normalized_files) {
        files_by_folder[parent_path(file.rel_path)].push_back(file);
    }

    TransferReport report;
    report.mode = config_.mode;
    report.files_total = normalized_files.size();
    report.folders_total = files_by_folder.size();
    if (config_.mode == Mode::dry_run) {
        report.diff_csv = "rel_path,decision,size,mtime\n";
    }

    for (const auto& [folder_path, files] : files_by_folder) {
        FolderRecord folder;
        folder.rel_path = folder_path;
        folder.parent_hash = folder_hash_for_path(parent_path(folder_path));
        folder.md_hash = path_hash(base_name(folder_path), folder.parent_hash);
        folder.files_discovered = files.size();
        folder.files_total = files.size();
        folder.started_ts = files.empty() ? 0 : files.front().mtime;

        transition_folder(folder, EndpointRole::sender, FolderState::reading);
        if (config_.mode == Mode::transfer) {
            transition_folder(folder, EndpointRole::receiver, FolderState::receiving);
        }

        const std::uint64_t source_folder_hash = source_scan ? source_scan->folder_data_hash(folder_path) : 0;
        const std::uint64_t target_folder_hash = target_scan ? target_scan->folder_data_hash(folder_path) : 0;
        const bool folder_fast_skip =
            source_scan != nullptr && target_scan != nullptr && source_folder_hash != 0 && source_folder_hash == target_folder_hash;

        std::vector<FileSnapshot> folder_snapshots;
        bool any_transfer = false;
        bool any_receiver_write = false;

        for (const auto& file : files) {
            RecBuf record = make_recbuf(file);
            FileOutcome outcome;
            outcome.rel_path = file.rel_path;
            outcome.size = record.size;

            const auto known_source = source_scan ? source_scan->find(file.rel_path) : std::nullopt;
            const auto known_target = target_scan ? target_scan->find(file.rel_path) : std::nullopt;

            bool skip = false;
            if (config_.mode != Mode::scan) {
                if (folder_fast_skip) {
                    skip = true;
                } else if (known_source.has_value() && target_scan != nullptr && target_scan->file_matches(*known_source)) {
                    skip = true;
                } else if (!known_source.has_value() && target_scan != nullptr &&
                           target_scan->metadata_matches(file.rel_path, record.size, record.mtime)) {
                    skip = true;
                }
            }

            if (config_.mode == Mode::scan) {
                transition_file(record, EndpointRole::sender, FileState::reading);
                const std::uint64_t data_hash = hash64(file.content);
                record.data_hash = data_hash;
                outcome.data_hash = data_hash;
                outcome.hash_verified = true;
                transition_file(record, EndpointRole::sender, FileState::done);
                outcome.sender_state = record.state;
                outcome.receiver_state = FileState::pending;
                report.files[file.rel_path] = outcome;
                const auto snapshot = make_snapshot(file, data_hash, 'S');
                report.source_scan_rows.push_back(snapshot);
                folder_snapshots.push_back(snapshot);
                folder.files_completed++;
                folder.flat_size_bytes += record.size;
                continue;
            }

            transition_file(record, EndpointRole::sender, FileState::checking);
            if (skip) {
                transition_file(record, EndpointRole::sender, FileState::skipped);
                transition_file(record, EndpointRole::sender, FileState::done);
                outcome.diff = DiffKind::skip;
                outcome.sender_state = record.state;
                outcome.receiver_state = FileState::done;
                outcome.hash_verified = true;
                outcome.data_hash = known_source ? known_source->data_hash : (known_target ? known_target->data_hash : 0);
                report.files_skipped++;
                folder.files_skipped++;
                folder.files_completed++;
                folder.files_received++;
                if (known_source.has_value()) {
                    report.source_scan_rows.push_back(*known_source);
                    folder_snapshots.push_back(*known_source);
                }
                if (known_target.has_value()) {
                    report.target_scan_rows.push_back(*known_target);
                }
                if (config_.mode == Mode::dry_run) {
                    report.diff_csv += file.rel_path + "," + to_string(DiffKind::skip) + "," +
                                       std::to_string(record.size) + "," + std::to_string(record.mtime) + "\n";
                }
                report.files[file.rel_path] = outcome;
                folder.flat_size_bytes += record.size;
                continue;
            }

            if (config_.mode == Mode::dry_run) {
                outcome.diff = known_target.has_value() ? DiffKind::changed : DiffKind::new_file;
                outcome.sender_state = record.state;
                outcome.receiver_state = FileState::pending;
                report.bytes_planned += record.size;
                report.diff_csv += file.rel_path + "," + to_string(outcome.diff) + "," +
                                   std::to_string(record.size) + "," + std::to_string(record.mtime) + "\n";
                report.files[file.rel_path] = outcome;
                folder.flat_size_bytes += record.size;
                continue;
            }

            const bool is_small = record.size <= config_.small_file_threshold;
            const std::size_t chunk_size = is_small ? config_.small_file_threshold : config_.large_chunk_bytes;
            const std::uint64_t data_hash = known_source ? known_source->data_hash : hash64(file.content);
            const std::size_t total_attempts = config_.max_retries + 1;
            std::size_t chunks_for_success = 0;
            bool transferred = false;
            FileState receiver_state = FileState::pending;

            report.bytes_planned += record.size;
            any_transfer = true;

            for (std::size_t attempt = 1; attempt <= total_attempts; ++attempt) {
                if (record.state == FileState::checking || record.state == FileState::failed) {
                    transition_file(record, EndpointRole::sender, FileState::reading);
                }
                if (receiver_state == FileState::pending) {
                    if (can_transition_file(EndpointRole::receiver, receiver_state, FileState::receiving)) {
                        receiver_state = FileState::receiving;
                    }
                } else if (receiver_state == FileState::failed) {
                    receiver_state = FileState::receiving;
                }

                outcome.attempts = attempt;
                std::size_t chunk_count = 0;
                for (std::size_t offset = 0; offset < file.content.size() || (file.content.empty() && offset == 0); offset += chunk_size) {
                    const std::size_t remaining = file.content.size() > offset ? file.content.size() - offset : 0;
                    const std::size_t len = file.content.empty() ? 0 : std::min<std::size_t>(chunk_size, remaining);
                    const std::string_view bytes(file.content.data() + offset, len);
                    const DataSlotHandle handle =
                        slot_pool.acquire_or_throw(is_small ? DataSlotClass::small : DataSlotClass::large, len);
                    if (len != 0U) {
                        std::memcpy(slot_pool.data(handle), bytes.data(), len);
                    }
                    populate_slot_for_chunk(slot_pool,
                                            handle,
                                            record,
                                            offset,
                                            bytes,
                                            is_small,
                                            offset + len >= file.content.size(),
                                            data_hash);

                    record.bytes_sent += len;
                    ++chunk_count;
                    ++report.chunks_sent;
                    slot_pool.release(handle);
                }

                if (record.state == FileState::checking || record.state == FileState::failed) {
                    transition_file(record, EndpointRole::sender, FileState::reading);
                }
                transition_file(record, EndpointRole::sender, FileState::transferring);
                receiver_state = FileState::writing;
                const bool forced_failure = !config_.skip_verify &&
                                            contains_path(config_.forced_failures, file.rel_path, attempt);
                if (forced_failure) {
                    transition_file(record, EndpointRole::sender, FileState::failed);
                    receiver_state = FileState::failed;
                    if (attempt < total_attempts) {
                        ++report.retries;
                    }
                    continue;
                }

                transition_file(record, EndpointRole::sender, FileState::done);
                receiver_state = FileState::done;
                transferred = true;
                chunks_for_success = chunk_count;
                break;
            }

            if (!transferred) {
                outcome.diff = DiffKind::failed;
                outcome.sender_state = record.state;
                outcome.receiver_state = receiver_state;
                outcome.data_hash = data_hash;
                outcome.hash_verified = false;
                report.files_failed++;
                report.files[file.rel_path] = outcome;
                folder.flat_size_bytes += record.size;
                continue;
            }

            outcome.diff = known_target.has_value() ? DiffKind::changed : DiffKind::new_file;
            outcome.sender_state = record.state;
            outcome.receiver_state = receiver_state;
            outcome.data_hash = data_hash;
            outcome.hash_verified = true;
            outcome.chunk_count = chunks_for_success;
            report.files_transferred++;
            report.bytes_transferred += record.size;
            folder.bytes_transferred += record.size;
            folder.files_completed++;
            folder.files_received++;
            folder.files_written++;
            any_receiver_write = true;

            const auto source_snapshot = known_source ? *known_source : make_snapshot(file, data_hash, 'S');
            const auto target_snapshot = make_snapshot(file, data_hash, 'T');
            report.source_scan_rows.push_back(source_snapshot);
            report.target_scan_rows.push_back(target_snapshot);
            folder_snapshots.push_back(source_snapshot);
            report.files[file.rel_path] = outcome;
            folder.flat_size_bytes += record.size;
        }

        folder.folder_data_hash = compute_folder_data_hash(folder_snapshots);
        folder.last_scan_ts = files.empty() ? 0 : files.back().mtime;
        folder.completed_ts = folder.last_scan_ts;

        if (config_.mode == Mode::scan) {
            transition_folder(folder, EndpointRole::sender, FolderState::done);
        } else {
            if (any_transfer) {
                transition_folder(folder, EndpointRole::sender, FolderState::transferring);
                transition_folder(folder, EndpointRole::sender, FolderState::awaiting_ack);
            } else {
                transition_folder(folder, EndpointRole::sender, FolderState::awaiting_ack);
            }
            transition_folder(folder, EndpointRole::sender, FolderState::done);
        }

        if (config_.mode == Mode::transfer) {
            if (any_receiver_write) {
                transition_folder(folder, EndpointRole::receiver, FolderState::writing);
            }
            transition_folder(folder, EndpointRole::receiver, FolderState::done);
        }

        report.folders[folder_path] = folder;
    }

    std::sort(report.source_scan_rows.begin(), report.source_scan_rows.end(), [](const FileSnapshot& lhs, const FileSnapshot& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });
    std::sort(report.target_scan_rows.begin(), report.target_scan_rows.end(), [](const FileSnapshot& lhs, const FileSnapshot& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });

    return report;
}

std::vector<FileSpec> TransferEngine::scan_directory(const std::filesystem::path& source_root, bool recursive) {
    if (is_nfs_url(source_root.string())) {
        NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(ConfigStore{});
        reader_config.source_root = source_root.string();
        reader_config.recursive = recursive;
        NfsMetaReader reader(reader_config);
        return reader.scan_tree();
    }
    return collect_file_specs(source_root, recursive);
}

std::vector<FileSpec> TransferEngine::scan_directory_with_config(const std::filesystem::path& source_root, bool recursive) const {
    if (is_nfs_url(source_root.string())) {
        NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
        reader_config.source_root = source_root.string();
        reader_config.recursive = recursive;
        NfsMetaReader reader(reader_config);
        return reader.scan_tree();
    }
    return collect_file_specs(source_root, recursive);
}

TransferReport TransferEngine::scan_directory_report(const std::filesystem::path& source_root,
                                                     char scan_side,
                                                     bool recursive) const {
    auto files = scan_directory_with_config(source_root, recursive);
    if (is_nfs_url(source_root.string())) {
        NfsDataReaderConfig reader_config = load_nfs_data_reader_config(config_store_);
        reader_config.small_file_threshold = config_.small_file_threshold;
        reader_config.large_chunk_bytes = config_.large_chunk_bytes;
        reader_config.source_root = source_root.string();
        NfsDataReader reader(reader_config);
        for (auto& file : files) {
            file.content = reader.load_file(file.rel_path).content;
            if (file.declared_size == 0) {
                file.declared_size = file.content.size();
            }
        }
    } else {
        for (auto& file : files) {
            file.content = read_file_contents(source_root / file.rel_path);
            if (file.declared_size == 0) {
                file.declared_size = file.content.size();
            }
        }
    }

    EngineConfig scan_config = config_;
    scan_config.mode = Mode::scan;
    TransferEngine scan_engine(scan_config);
    TransferReport report = scan_engine.run(files);

    for (auto& snapshot : report.source_scan_rows) {
        snapshot.scan_side = scan_side;
    }
    if (scan_side == 'T') {
        report.target_scan_rows = report.source_scan_rows;
        report.source_scan_rows.clear();
    }
    return report;
}

TransferReport TransferEngine::dry_run_directory(const std::filesystem::path& source_root,
                                                 const ScanIndex* source_scan,
                                                 const ScanIndex* target_scan,
                                                 bool recursive) const {
    auto files = scan_directory_with_config(source_root, recursive);

    std::optional<ScanIndex> computed_source_scan;
    const ScanIndex* effective_source_scan = source_scan;
    if (effective_source_scan == nullptr && target_scan != nullptr) {
        computed_source_scan.emplace(build_scan_index(source_root, 'S', recursive));
        effective_source_scan = &*computed_source_scan;
    }

    EngineConfig dry_run_config = config_;
    dry_run_config.mode = Mode::dry_run;
    TransferEngine dry_run_engine(dry_run_config);
    return dry_run_engine.run(files, effective_source_scan, target_scan);
}

TransferReport TransferEngine::diff_scan_indexes(const ScanIndex& source_scan,
                                                 const ScanIndex& target_scan,
                                                 const std::string& compare_mode) const {
    if (compare_mode != "size" && compare_mode != "time" && compare_mode != "content") {
        throw std::invalid_argument("diff compare mode must be size, time, or content");
    }

    const auto source_rows = source_scan.rows();
    const auto target_rows = target_scan.rows();
    std::map<std::string, FileSnapshot> target_by_path;
    for (const auto& row : target_rows) {
        target_by_path.emplace(row.rel_path, row);
    }

    const auto rows_match = [&](const FileSnapshot& source, const FileSnapshot& target) {
        if (compare_mode == "size") {
            return source.size == target.size;
        }
        if (compare_mode == "time") {
            return source.size == target.size && source.mtime == target.mtime;
        }
        if (source.data_hash != 0U && target.data_hash != 0U) {
            return source.size == target.size && source.data_hash == target.data_hash;
        }
        return source.size == target.size && source.mtime == target.mtime;
    };

    TransferReport report;
    report.mode = Mode::dry_run;
    report.source_scan_rows = source_rows;
    report.target_scan_rows = target_rows;
    report.diff_csv = "rel_path,decision,source_size,source_mtime,target_size,target_mtime\n";

    for (const auto& source : source_rows) {
        FileOutcome outcome;
        outcome.rel_path = source.rel_path;
        outcome.size = source.size;
        outcome.data_hash = source.data_hash;
        const auto target_it = target_by_path.find(source.rel_path);
        if (target_it == target_by_path.end()) {
            outcome.diff = DiffKind::new_file;
            ++report.files_new;
            report.bytes_planned += source.size;
        } else if (rows_match(source, target_it->second)) {
            outcome.diff = DiffKind::skip;
            ++report.files_skipped;
        } else {
            outcome.diff = DiffKind::changed;
            ++report.files_changed;
            report.bytes_planned += source.size;
        }
        const std::uint64_t target_size = target_it == target_by_path.end() ? 0U : target_it->second.size;
        const std::uint64_t target_mtime = target_it == target_by_path.end() ? 0U : target_it->second.mtime;
        report.diff_csv += source.rel_path + "," + to_string(outcome.diff) + "," +
                           std::to_string(source.size) + "," + std::to_string(source.mtime) + "," +
                           std::to_string(target_size) + "," + std::to_string(target_mtime) + "\n";
        report.files[source.rel_path] = outcome;
        if (target_it != target_by_path.end()) {
            target_by_path.erase(target_it);
        }
    }

    for (const auto& [path, target] : target_by_path) {
        FileOutcome outcome;
        outcome.rel_path = path;
        outcome.diff = DiffKind::target_only;
        outcome.size = target.size;
        outcome.data_hash = target.data_hash;
        ++report.files_target_only;
        report.diff_csv += path + "," + to_string(DiffKind::target_only) + ",0,0," +
                           std::to_string(target.size) + "," + std::to_string(target.mtime) + "\n";
        report.files[path] = outcome;
    }

    report.files_total = report.files.size();
    return report;
}

TransferReport TransferEngine::diff_metadata_trees(const std::filesystem::path& source_root,
                                                   const std::filesystem::path& target_root,
                                                   const std::string& compare_mode,
                                                   bool recursive,
                                                   std::size_t meta_reader_threads,
                                                   std::size_t metadata_async_depth,
                                                   double max_duration_seconds,
                                                   bool collect_detailed_records,
                                                   std::uint32_t stats_interval_seconds) const {
    if (compare_mode != "size" && compare_mode != "time" && compare_mode != "content") {
        throw std::invalid_argument("diff compare mode must be size, time, or content");
    }
    if (source_root.empty()) {
        throw std::invalid_argument("source root is required");
    }
    if (target_root.empty()) {
        throw std::invalid_argument("target root is required");
    }

    NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
    reader_config.source_root = source_root.string();
    reader_config.recursive = recursive;
    if (meta_reader_threads != 0) {
        reader_config.worker_count = meta_reader_threads;
        reader_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0) {
        reader_config.async_directory_depth = metadata_async_depth;
    }

    if (!collect_detailed_records) {
        return run_summary_live_diff(reader_config.source_root,
                                     target_root.string(),
                                     compare_mode,
                                     recursive,
                                     reader_config,
                                     max_duration_seconds,
                                     stats_interval_seconds);
    }

    TransferReport report;
    report.mode = Mode::dry_run;
    if (collect_detailed_records) {
        report.diff_csv = "rel_path,decision,source_size,source_mtime,target_size,target_mtime\n";
    }

    FlatMetadataWorkQueue queue;
    queue.folders.push_back(FileSpec{});
    if (max_duration_seconds > 0.0) {
        queue.stop_at = std::chrono::steady_clock::now() +
                        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                            std::chrono::duration<double>(max_duration_seconds));
    }

    std::mutex report_mutex;
    const std::size_t thread_count = std::max<std::size_t>(1, reader_config.worker_count);
    std::vector<std::thread> workers;
    workers.reserve(thread_count);
    for (std::size_t index = 0; index < thread_count; ++index) {
        workers.emplace_back(live_diff_metadata_worker,
                             reader_config.source_root,
                             target_root.string(),
                             recursive,
                             compare_mode,
                             std::max<std::size_t>(1, reader_config.async_directory_depth),
                             std::ref(queue),
                             std::ref(report),
                             std::ref(report_mutex),
                             collect_detailed_records);
    }
    for (auto& worker : workers) {
        worker.join();
    }
    if (queue.error) {
        std::rethrow_exception(queue.error);
    }

    if (collect_detailed_records) {
        report.files_total = report.files.size();
        report.folders_total = report.folders.size();
    }
    return report;
}

ScanIndex TransferEngine::build_scan_index(const std::filesystem::path& source_root,
                                           char scan_side,
                                           bool recursive) const {
    const TransferReport report = scan_directory_report(source_root, scan_side, recursive);
    ScanIndex index;
    const auto& rows = scan_side == 'T' ? report.target_scan_rows : report.source_scan_rows;
    for (const auto& row : rows) {
        index.add(row);
    }
    return index;
}

MetadataBenchmarkReport TransferEngine::benchmark_metadata_pipeline(const std::filesystem::path& source_root,
                                                                    bool recursive,
                                                                    bool discard_after_checker,
                                                                    bool use_stats_discarder,
                                                                    std::size_t meta_reader_threads,
                                                                    std::size_t metadata_async_depth,
                                                                    const std::filesystem::path& metadata_output_path,
                                                                    const std::string& metadata_output_format,
                                                                    const std::string& metadata_records,
                                                                    double max_duration_seconds,
                                                                    std::uint32_t stats_interval_seconds,
                                                                    std::size_t record_buffer_slots,
                                                                    std::size_t metadata_output_partitions,
                                                                    const std::string& metadata_output_partition_mode,
                                                                    const std::filesystem::path& status_socket_path) const {
    NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
    reader_config.source_root = source_root.string();
    reader_config.recursive = recursive;
    if (meta_reader_threads != 0) {
        reader_config.worker_count = meta_reader_threads;
        reader_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0) {
        reader_config.async_directory_depth = metadata_async_depth;
    }
    if (record_buffer_slots != 0) {
        reader_config.recbuf_window = record_buffer_slots;
    }
    NfsMetaReader reader(reader_config);

    CheckerConfig checker_config = load_checker_config(config_store_);
    checker_config.discard_checked_records = discard_after_checker;
    Checker checker(checker_config);

    MetadataBenchmarkReport report;
    report.meta_reader_async = reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, reader_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, reader_config.async_directory_depth);
    report.record_buffer_slots = reader_config.recbuf_window;
    report.stats_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    report.metadata_output_partitions = std::max<std::size_t>(1U, metadata_output_partitions);
    if (metadata_output_partition_mode != "single" && metadata_output_partition_mode != "processes") {
        throw std::invalid_argument("metadata output partition mode must be single or processes");
    }

    const auto started = std::chrono::steady_clock::now();

    MetadataStatsDiscarderConfig stats_config = load_metadata_stats_discarder_config(config_store_);
    stats_config.enabled = stats_config.enabled || use_stats_discarder || !status_socket_path.empty();
    stats_config.print_interval_seconds = report.stats_interval_seconds;
    MetadataStatsDiscarder stats_discarder(stats_config);
    if (stats_config.enabled) {
        MetadataRecordWriterConfig writer_config = load_metadata_record_writer_config(config_store_);
        if (!metadata_output_path.empty()) {
            writer_config.enabled = true;
            writer_config.output_path = metadata_output_path;
            if (metadata_output_format.empty()) {
                writer_config.format = infer_metadata_record_format(metadata_output_path);
            }
        }
        if (!metadata_output_format.empty()) {
            writer_config.format = parse_metadata_record_format(metadata_output_format);
        }
        if (!metadata_records.empty()) {
            if (metadata_records == "all") {
                writer_config.write_files = true;
                writer_config.write_folders = true;
            } else if (metadata_records == "files") {
                writer_config.write_files = true;
                writer_config.write_folders = false;
            } else if (metadata_records == "folders") {
                writer_config.write_files = false;
                writer_config.write_folders = true;
            } else {
                throw std::invalid_argument("metadata records must be all, files, or folders");
            }
        }
        const std::string effective_records = writer_config.write_files && writer_config.write_folders
                                                  ? "all"
                                                  : (writer_config.write_files ? "files" : "folders");
        if (writer_config.enabled) {
            writer_config.run_info =
                make_metadata_scan_run_info(source_root,
                                            recursive,
                                            to_string(writer_config.format),
                                            effective_records,
                                            report.meta_reader_threads,
                                            report.metadata_async_depth,
                                            report.record_buffer_slots,
                                            max_duration_seconds,
                                            report.stats_interval_seconds);
            report.scan_run_id = writer_config.run_info->run_id;
        }
        std::optional<MetadataRecordWriter> record_writer;
        std::unique_ptr<PartitionedMetadataWriter> partitioned_writer;
        if (writer_config.enabled && report.metadata_output_partitions > 1U) {
            if (metadata_output_partition_mode != "processes") {
                throw std::invalid_argument("partitioned metadata output requires --metadata-output-partition-mode processes");
            }
            partitioned_writer = std::make_unique<PartitionedMetadataWriter>(
                writer_config,
                writer_config.output_path,
                report.metadata_output_partitions);
        } else if (writer_config.enabled) {
            record_writer.emplace(writer_config);
        }
        stats_discarder.start();
        run_parallel_flat_metadata_scan(reader_config,
                                        stats_discarder,
                                        record_writer.has_value() ? &*record_writer : nullptr,
                                        partitioned_writer.get(),
                                        max_duration_seconds,
                                        status_socket_path);
        stats_discarder.stop();
        const MetadataStatsSnapshot stats = stats_discarder.snapshot();
        report.files_seen = stats.files_found;
        report.checker_discarded = stats.records_discarded;
        report.folders_found = stats.folders_found;
        report.logical_size_bytes = stats.logical_size_bytes;
        report.records_per_second = stats.records_per_second;
        if (record_writer.has_value()) {
            record_writer->close();
            report.metadata_files_written = record_writer->files_written();
            report.metadata_folders_written = record_writer->folders_written();
        }
        if (partitioned_writer) {
            partitioned_writer->close();
            report.metadata_files_written = partitioned_writer->files_written();
            report.metadata_folders_written = partitioned_writer->folders_written();
        }
    } else {
        reader.publish_tree();
        JobMessage message;
        while (reader.pull(message)) {
            RecBuf record = message_as<RecBuf>(message);
            ++report.files_seen;
            (void)checker.should_skip(record);
            checker.queue_checked_record(std::move(record));
        }

        while (checker.pull(message)) {
            ++report.checker_emitted;
        }
        report.checker_discarded = checker.stats().deferred;
    }

    report.elapsed_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    if (!stats_config.enabled && report.elapsed_seconds > 0.0) {
        report.records_per_second = static_cast<double>(report.files_seen) / report.elapsed_seconds;
    }
    return report;
}

DataReadBenchmarkReport TransferEngine::benchmark_data_read_pipeline(const std::filesystem::path& source_root,
                                                                     bool recursive,
                                                                     std::size_t meta_reader_threads,
                                                                     std::size_t metadata_async_depth,
                                                                     std::size_t data_reader_threads,
                                                                     std::size_t data_outstanding_requests,
                                                                     std::size_t max_files_queued,
                                                                     std::size_t data_buffer_slots,
                                                                     std::size_t data_queue_depth,
                                                                     const std::string& data_copy_mode,
                                                                     double max_duration_seconds,
                                                                     std::uint32_t stats_interval_seconds,
                                                                     const std::filesystem::path& status_socket_path) const {
    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    meta_config.worker_count = std::max<std::size_t>(1, meta_reader_threads);
    meta_config.thread_count = meta_config.worker_count;
    meta_config.async_directory_depth = std::max<std::size_t>(1, metadata_async_depth);

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    if (data_reader_threads != 0) {
        data_config.data_reader_worker_count = data_reader_threads;
    }
    if (data_outstanding_requests != 0) {
        data_config.outstanding_requests = data_outstanding_requests;
    }
    if (!data_copy_mode.empty()) {
        data_config.copy_data_from_nfs = parse_data_copy_mode(data_copy_mode);
    }

    NfsMetaReader meta_reader(meta_config);
    NfsDataReader data_reader(data_config);

    DataReadBenchmarkReport report;
    report.meta_reader_async = meta_reader.using_async_backend();
    report.data_reader_async = data_reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, meta_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, meta_config.async_directory_depth);
    report.data_reader_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    report.data_outstanding_requests = std::max<std::size_t>(1, data_config.outstanding_requests);
    report.max_files_queued = std::max<std::size_t>(1, max_files_queued);
    report.data_copy_mode = data_copy_mode_name(data_config.copy_data_from_nfs);

    const DataReadBenchmarkSnapshot snapshot =
        run_parallel_data_read_scan(meta_config,
                                    data_config,
                                    report.max_files_queued,
                                    data_buffer_slots,
                                    data_queue_depth,
                                    max_duration_seconds,
                                    stats_interval_seconds,
                                    status_socket_path);
    report.files_found = snapshot.files_found;
    report.folders_found = snapshot.folders_found;
    report.files_read = snapshot.files_read;
    report.files_failed = snapshot.files_failed;
    report.logical_size_bytes = snapshot.logical_size_bytes;
    report.bytes_read = snapshot.bytes_read;
    report.bytes_per_second = snapshot.bytes_per_second;
    report.gigabits_per_second = snapshot.gigabits_per_second;
    report.elapsed_seconds = snapshot.elapsed_seconds;
    report.data_buffer_slots = snapshot.data_buffer_slots;
    report.data_queue_depth = snapshot.data_queue_depth;
    return report;
}

DataHashBenchmarkReport TransferEngine::benchmark_data_hash_pipeline(const std::filesystem::path& source_root,
                                                                     bool recursive,
                                                                     const std::string& hash_algorithm,
                                                                     std::size_t meta_reader_threads,
                                                                     std::size_t metadata_async_depth,
                                                                     std::size_t data_reader_threads,
                                                                     std::size_t data_outstanding_requests,
                                                                     std::size_t hash_worker_threads,
                                                                     std::size_t max_files_queued,
                                                                     std::size_t data_buffer_slots,
                                                                     std::size_t data_queue_depth,
                                                                     std::size_t hash_work_factor,
                                                                     double max_duration_seconds,
                                                                     std::uint32_t stats_interval_seconds,
                                                                     const std::filesystem::path& status_socket_path) const {
    const ContentHashAlgorithm algorithm = parse_content_hash_algorithm(hash_algorithm);

    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    meta_config.worker_count = std::max<std::size_t>(1, meta_reader_threads);
    meta_config.thread_count = meta_config.worker_count;
    meta_config.async_directory_depth = std::max<std::size_t>(1, metadata_async_depth);

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    if (data_reader_threads != 0) {
        data_config.data_reader_worker_count = data_reader_threads;
    }
    if (data_outstanding_requests != 0) {
        data_config.outstanding_requests = data_outstanding_requests;
    }
    data_config.copy_data_from_nfs = true;

    NfsMetaReader meta_reader(meta_config);
    NfsDataReader data_reader(data_config);

    DataHashBenchmarkReport report =
        run_parallel_data_hash_scan(meta_config,
                                    data_config,
                                    algorithm,
                                    hash_worker_threads == 0 ? data_config.data_reader_worker_count
                                                             : hash_worker_threads,
                                    hash_work_factor,
                                    max_files_queued,
                                    data_buffer_slots,
                                    data_queue_depth,
                                    max_duration_seconds,
                                    stats_interval_seconds,
                                    status_socket_path);
    report.meta_reader_async = meta_reader.using_async_backend();
    report.data_reader_async = data_reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, meta_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, meta_config.async_directory_depth);
    report.data_reader_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    report.data_outstanding_requests = std::max<std::size_t>(1, data_config.outstanding_requests);
    report.hash_worker_threads = hash_worker_threads == 0
                                     ? std::max<std::size_t>(1, data_config.data_reader_worker_count)
                                     : std::max<std::size_t>(1, hash_worker_threads);
    report.max_files_queued = std::max<std::size_t>(1, max_files_queued);
    report.hash_algorithm = to_string(algorithm);
    report.hash_work_factor = std::max<std::size_t>(1, hash_work_factor);
    return report;
}

HashInventoryReport TransferEngine::hash_inventory_pipeline(const std::filesystem::path& source_root,
                                                            bool recursive,
                                                            const std::string& hash_algorithm,
                                                            std::size_t meta_reader_threads,
                                                            std::size_t metadata_async_depth,
                                                            std::size_t data_reader_threads,
                                                            std::size_t data_outstanding_requests,
                                                            std::size_t hash_worker_threads,
                                                            std::size_t max_files_queued,
                                                            std::size_t max_hash_chunks_queued,
                                                            const std::string& hash_mode_value,
                                                            std::uint64_t hash_block_size,
                                                            const std::filesystem::path& metadata_output_path,
                                                            const std::string& metadata_output_format,
                                                            const std::string& metadata_records,
                                                            double max_duration_seconds) const {
    const ContentHashAlgorithm algorithm = parse_content_hash_algorithm(hash_algorithm);
    const HashMode hash_mode = parse_hash_mode(hash_mode_value);

    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    meta_config.worker_count = std::max<std::size_t>(1, meta_reader_threads);
    meta_config.thread_count = meta_config.worker_count;
    meta_config.async_directory_depth = std::max<std::size_t>(1, metadata_async_depth);

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    if (data_reader_threads != 0) {
        data_config.data_reader_worker_count = data_reader_threads;
    }
    if (data_outstanding_requests != 0) {
        data_config.outstanding_requests = data_outstanding_requests;
    }

    MetadataRecordWriterConfig writer_config = load_metadata_record_writer_config(config_store_);
    writer_config.enabled = true;
    if (!metadata_output_path.empty()) {
        writer_config.output_path = metadata_output_path;
        if (metadata_output_format.empty()) {
            writer_config.format = infer_metadata_record_format(metadata_output_path);
        }
    }
    if (!metadata_output_format.empty()) {
        writer_config.format = parse_metadata_record_format(metadata_output_format);
    }
    if (metadata_records == "all" || metadata_records.empty()) {
        writer_config.write_files = true;
        writer_config.write_folders = true;
    } else if (metadata_records == "files") {
        writer_config.write_files = true;
        writer_config.write_folders = false;
    } else if (metadata_records == "folders") {
        writer_config.write_files = false;
        writer_config.write_folders = true;
    } else {
        throw std::invalid_argument("metadata records must be all, files, or folders");
    }

    NfsMetaReader meta_reader(meta_config);
    NfsDataReader data_reader(data_config);

    HashInventoryReport report;
    report.meta_reader_async = meta_reader.using_async_backend();
    report.data_reader_async = data_reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, meta_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, meta_config.async_directory_depth);
    report.data_reader_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    report.data_outstanding_requests = std::max<std::size_t>(1, data_config.outstanding_requests);
    report.hash_worker_threads = hash_worker_threads == 0
                                     ? std::max<std::size_t>(1, data_config.data_reader_worker_count)
                                     : std::max<std::size_t>(1, hash_worker_threads);
    report.max_files_queued = std::max<std::size_t>(1, max_files_queued);
    report.max_hash_chunks_queued = std::max<std::size_t>(1, max_hash_chunks_queued);
    report.hash_block_size = std::max<std::uint64_t>(1, hash_block_size);
    report.hash_mode = to_string(hash_mode);
    report.hash_algorithm = to_string(algorithm);

    MetadataRecordWriter record_writer(writer_config);
    const HashInventorySnapshot snapshot =
        run_parallel_hash_inventory_scan(meta_config,
                                         data_config,
                                         algorithm,
                                         hash_mode,
                                         report.hash_worker_threads,
                                         report.max_files_queued,
                                         report.max_hash_chunks_queued,
                                         report.hash_block_size,
                                         &record_writer,
                                         max_duration_seconds);
    record_writer.close();

    report.files_found = snapshot.files_found;
    report.folders_found = snapshot.folders_found;
    report.files_hashed = snapshot.files_hashed;
    report.files_failed = snapshot.files_failed;
    report.logical_size_bytes = snapshot.logical_size_bytes;
    report.bytes_read = snapshot.bytes_read;
    report.bytes_hashed = snapshot.bytes_hashed;
    report.read_bytes_per_second = snapshot.read_bytes_per_second;
    report.read_gigabits_per_second = snapshot.read_gigabits_per_second;
    report.bytes_per_second = snapshot.bytes_per_second;
    report.read_elapsed_seconds = snapshot.read_elapsed_seconds;
    report.elapsed_seconds = snapshot.elapsed_seconds;
    report.metadata_files_written = record_writer.files_written();
    report.metadata_folders_written = record_writer.folders_written();
    return report;
}

HashSpeedBenchmarkReport TransferEngine::benchmark_hash_speed(const std::string& hash_algorithm,
                                                              std::size_t worker_threads,
                                                              std::uint64_t block_size,
                                                              double duration_seconds,
                                                              double min_gigabits_per_core_second) const {
    const ContentHashAlgorithm algorithm = parse_content_hash_algorithm(hash_algorithm);
    const std::size_t threads = std::max<std::size_t>(1, worker_threads);
    const std::size_t payload_size = static_cast<std::size_t>(std::max<std::uint64_t>(1, block_size));
    const std::string payload = make_hash_benchmark_payload(payload_size);

    std::atomic<std::size_t> ready {0};
    std::atomic<bool> start {false};
    std::atomic<bool> stop {false};
    std::vector<HashSpeedWorkerResult> results(threads);
    std::vector<std::thread> workers;
    workers.reserve(threads);

    for (std::size_t index = 0; index < threads; ++index) {
        workers.emplace_back([&, index] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            HashSpeedWorkerResult local;
            std::uint64_t mix = 0x9e3779b97f4a7c15ULL ^ static_cast<std::uint64_t>(index);
            while (!stop.load(std::memory_order_relaxed)) {
                const std::uint64_t digest = benchmark_hash_once(algorithm, payload);
                mix ^= digest + 0x9e3779b97f4a7c15ULL + (mix << 6U) + (mix >> 2U);
                local.bytes_hashed += payload.size();
                ++local.iterations;
            }
            local.digest_mix = mix;
            results[index] = local;
        });
    }

    while (ready.load(std::memory_order_acquire) != threads) {
        std::this_thread::yield();
    }
    const auto started_at = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::duration<double>(duration_seconds));
    stop.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto ended_at = std::chrono::steady_clock::now();

    HashSpeedBenchmarkReport report;
    report.worker_threads = threads;
    report.hardware_threads = std::max<unsigned>(1U, std::thread::hardware_concurrency());
    report.block_size = payload.size();
    report.duration_seconds = duration_seconds;
    report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
    report.hash_algorithm = to_string(algorithm);
    report.min_gigabits_per_core_second = std::max(0.0, min_gigabits_per_core_second);
    for (const auto& result : results) {
        report.bytes_hashed += result.bytes_hashed;
        report.iterations += result.iterations;
        report.digest_mix ^= result.digest_mix;
    }
    report.bytes_per_second =
        report.elapsed_seconds > 0.0 ? static_cast<double>(report.bytes_hashed) / report.elapsed_seconds : 0.0;
    report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
    report.bytes_per_core_second =
        threads != 0 ? report.bytes_per_second / static_cast<double>(threads) : 0.0;
    report.gigabits_per_core_second = report.gigabits_per_second / static_cast<double>(threads);
    report.gigabits_per_hardware_core_second =
        report.gigabits_per_second / static_cast<double>(report.hardware_threads);
    report.passed = report.min_gigabits_per_core_second <= 0.0 ||
                    report.gigabits_per_core_second >= report.min_gigabits_per_core_second;
    return report;
}

MetadataWriterBenchmarkReport TransferEngine::benchmark_metadata_writer(
    const std::filesystem::path& output_path,
    const std::string& output_format,
    std::uint64_t file_count,
    std::uint64_t folder_count,
    std::size_t batch_size,
    std::uint64_t average_file_size,
    const std::string& duckdb_memory_limit,
    std::size_t duckdb_threads,
    const std::string& duckdb_checkpoint_threshold,
    const std::string& parquet_compression,
    std::size_t partitions,
    const std::string& partition_mode) const {
    if (output_path.empty()) {
        throw std::invalid_argument("metadata writer benchmark output path is required");
    }
    if (partition_mode != "threads" &&
        partition_mode != "processes" &&
        partition_mode != "transport-processes" &&
        partition_mode != "generate-discard" &&
        partition_mode != "generate-hash-discard" &&
        partition_mode != "pack-discard" &&
        partition_mode != "folder-pack-discard" &&
        partition_mode != "transport-discard") {
        throw std::invalid_argument("metadata writer benchmark partition mode must be threads, processes, transport-processes, generate-discard, generate-hash-discard, pack-discard, folder-pack-discard, or transport-discard");
    }
    partitions = std::max<std::size_t>(1U, partitions);

    MetadataRecordWriterConfig writer_config = load_metadata_record_writer_config(config_store_);
    writer_config.enabled = true;
    writer_config.output_path = output_path;
    writer_config.format = output_format.empty() || output_format == "auto"
                               ? infer_metadata_record_format(output_path)
                               : parse_metadata_record_format(output_format);
    writer_config.write_files = true;
    writer_config.write_folders = true;
    writer_config.flush_per_batch = false;
    if (!duckdb_memory_limit.empty()) {
        writer_config.duckdb_memory_limit = duckdb_memory_limit;
    }
    if (duckdb_threads != 0U) {
        writer_config.duckdb_threads = duckdb_threads;
    }
    if (!duckdb_checkpoint_threshold.empty()) {
        writer_config.duckdb_checkpoint_threshold = duckdb_checkpoint_threshold;
    }
    if (!parquet_compression.empty()) {
        writer_config.parquet_compression = parquet_compression;
    }

    if (partitions > 1U) {
        std::filesystem::create_directories(output_path);
    }

    const std::size_t effective_batch_size = std::max<std::size_t>(1U, batch_size);
    const auto count_for_partition = [partitions](std::uint64_t total, std::size_t index) {
        const std::uint64_t base = total / partitions;
        const std::uint64_t remainder = total % partitions;
        return base + (index < remainder ? 1U : 0U);
    };
    const auto part_suffix = [](std::size_t index) {
        std::ostringstream out;
        out << std::setw(5) << std::setfill('0') << index;
        return out.str();
    };
    const auto extension_for_format = [](MetadataRecordFormat format) {
        switch (format) {
            case MetadataRecordFormat::text:
                return std::string("txt");
            case MetadataRecordFormat::csv:
                return std::string("csv");
            case MetadataRecordFormat::parquet:
                return std::string("parquet");
        }
        return std::string("txt");
    };
    const auto output_for_partition = [&](std::size_t index) {
        if (partitions == 1U) {
            return output_path;
        }
        return output_path / ("part-" + part_suffix(index) + "." + extension_for_format(writer_config.format));
    };
    const auto report_for_partition = [&](std::size_t index) {
        return output_path / ("part-" + part_suffix(index) + ".report");
    };
    const auto socket_for_partition = [&](std::size_t index) {
        return output_path / ("part-" + part_suffix(index) + ".sock");
    };
    const auto metadata_partition_for_path = [partitions](std::string_view path) {
        return static_cast<std::size_t>(hash64(path) % partitions);
    };

    const auto run_partition = [&](std::size_t partition_index) {
        MetadataRecordWriterConfig partition_writer_config = writer_config;
        partition_writer_config.output_path = output_for_partition(partition_index);

        MetadataScanRunInfo run_info;
        run_info.run_id = partitions == 1U ? "metadata-writer-benchmark"
                                           : "metadata-writer-benchmark-part-" + part_suffix(partition_index);
        run_info.started_at_utc = "benchmark";
        run_info.source_root = "synthetic";
        run_info.settings_json = "{\"mode\":\"metadata_writer_benchmark\",\"partitions\":" +
                                 std::to_string(partitions) + ",\"partition\":" +
                                 std::to_string(partition_index) + "}";
        partition_writer_config.run_info = run_info;

        FileMetadataGenerator generator(FileMetadataGeneratorConfig(
            count_for_partition(file_count, partition_index),
            count_for_partition(folder_count, partition_index),
            effective_batch_size,
            average_file_size,
            0x9e3779b97f4a7c15ULL ^ (static_cast<std::uint64_t>(partition_index) * 0x94d049bb133111ebULL),
            partitions == 1U ? "generated" : "generated/part_" + part_suffix(partition_index)));
        MetadataRecordWriter writer(partition_writer_config);
        FileMetadataGeneratorBatch batch;

        MetadataWriterBenchmarkReport partition_report;
        while (generator.next_batch(batch)) {
            for (const auto& file : batch.files) {
                partition_report.logical_size_bytes += file.declared_size;
            }
            writer.write_batch(batch.files, batch.folders);
        }
        writer.close();

        partition_report.files_generated = generator.files_generated();
        partition_report.folders_generated = generator.folders_generated();
        partition_report.records_written = writer.files_written() + writer.folders_written();
        partition_report.batch_size = effective_batch_size;
        partition_report.partitions = partitions;
        partition_report.partition_mode = partition_mode;
        partition_report.output_format = to_string(writer_config.format);
        partition_report.output_path = partition_writer_config.output_path;
        return partition_report;
    };
    const auto write_partition_report = [](const std::filesystem::path& path,
                                           const MetadataWriterBenchmarkReport& report) {
        std::ofstream output(path, std::ios::out | std::ios::trunc);
        if (!output) {
            throw std::runtime_error("failed to open metadata writer partition report: " + path.string());
        }
        output << report.files_generated << ' '
               << report.folders_generated << ' '
               << report.records_written << ' '
               << report.logical_size_bytes << '\n';
    };
    const auto read_partition_report = [&](std::size_t index) {
        const std::filesystem::path path = report_for_partition(index);
        std::ifstream input(path);
        if (!input) {
            throw std::runtime_error("failed to read metadata writer partition report: " + path.string());
        }
        MetadataWriterBenchmarkReport report;
        input >> report.files_generated
              >> report.folders_generated
              >> report.records_written
              >> report.logical_size_bytes;
        if (!input) {
            throw std::runtime_error("malformed metadata writer partition report: " + path.string());
        }
        report.batch_size = effective_batch_size;
        report.partitions = partitions;
        report.partition_mode = partition_mode;
        report.output_format = to_string(writer_config.format);
        report.output_path = output_for_partition(index);
        std::filesystem::remove(path);
        return report;
    };
    const auto run_transport_writer_process = [&](std::size_t partition_index) {
        MetadataRecordWriterConfig partition_writer_config = writer_config;
        partition_writer_config.output_path = output_for_partition(partition_index);

        MetadataScanRunInfo run_info;
        run_info.run_id = "metadata-writer-transport-part-" + part_suffix(partition_index);
        run_info.started_at_utc = "benchmark";
        run_info.source_root = "synthetic";
        run_info.settings_json = "{\"mode\":\"metadata_writer_transport_benchmark\",\"partitions\":" +
                                 std::to_string(partitions) + ",\"partition\":" +
                                 std::to_string(partition_index) + "}";
        partition_writer_config.run_info = run_info;

        RawBufferPool receive_pool(kMetadataBatchBufferPoolId,
                                   16U,
                                   sizeof(MetadataBatchBuffer),
                                   alignof(MetadataBatchBuffer));
        BufferPoolRegistry receive_registry;
        receive_registry.register_pool(receive_pool);
        BufQueue writer_queue(receive_pool.capacity());
        BufferReceiverJob receiver(1U,
                                   receive_pool,
                                   writer_queue,
                                   BufferTransportEndpoint::unix_socket(socket_for_partition(partition_index)));
        MetadataRecordWriterJob writer(1U, writer_queue, receive_registry, partition_writer_config);

        receiver.start();
        writer.start();
        receiver.wait();
        writer.wait();

        MetadataWriterBenchmarkReport partition_report;
        const auto stats = writer.writer_stats();
        partition_report.files_generated = stats.files_written;
        partition_report.folders_generated = stats.folders_written;
        partition_report.records_written = stats.files_written + stats.folders_written;
        partition_report.batch_size = effective_batch_size;
        partition_report.partitions = partitions;
        partition_report.partition_mode = partition_mode;
        partition_report.output_format = to_string(writer_config.format);
        partition_report.output_path = partition_writer_config.output_path;
        return partition_report;
    };
    const auto run_transport_discard_process = [&](std::size_t partition_index) {
        RawBufferPool receive_pool(kMetadataBatchBufferPoolId,
                                   16U,
                                   sizeof(MetadataBatchBuffer),
                                   alignof(MetadataBatchBuffer));
        BufferPoolRegistry receive_registry;
        receive_registry.register_pool(receive_pool);
        BufQueue discard_queue(receive_pool.capacity());
        BufferReceiverJob receiver(1U,
                                   receive_pool,
                                   discard_queue,
                                   BufferTransportEndpoint::unix_socket(socket_for_partition(partition_index)));
        BufferDiscarderJob discarder(BufferDiscarderConfig(1U), discard_queue, receive_registry);

        receiver.start();
        discarder.start();
        receiver.wait();
        discarder.wait();

        MetadataWriterBenchmarkReport partition_report;
        const auto stats = discarder.stats();
        partition_report.records_written = stats.buffers_discarded;
        partition_report.logical_size_bytes = stats.bytes_discarded;
        partition_report.batch_size = effective_batch_size;
        partition_report.partitions = partitions;
        partition_report.partition_mode = partition_mode;
        partition_report.output_format = "discard";
        partition_report.output_path = output_for_partition(partition_index);
        return partition_report;
    };

    std::vector<MetadataWriterBenchmarkReport> partition_reports(partitions);
    std::exception_ptr worker_error;
    std::mutex worker_error_mutex;
    std::uint64_t transport_logical_size_bytes = 0;
    std::uint64_t transport_files_generated = 0;
    std::uint64_t transport_folders_generated = 0;

    const auto run_parent_generator_probe = [&](bool hash_paths) {
        FileMetadataGenerator generator(FileMetadataGeneratorConfig(file_count,
                                                                    folder_count,
                                                                    effective_batch_size,
                                                                    average_file_size,
                                                                    0x9e3779b97f4a7c15ULL,
                                                                    "generated"));
        std::uint64_t shard_mix = 0;
        for (std::uint64_t first = 0; first < file_count; first += effective_batch_size) {
            const std::uint64_t count = std::min<std::uint64_t>(effective_batch_size, file_count - first);
            generator.for_each_file_view(first, count, [&](const GeneratedFileMetadataView& file) {
                transport_logical_size_bytes += file.declared_size;
                ++transport_files_generated;
                if (hash_paths) {
                    shard_mix += metadata_partition_for_path(file.rel_path);
                }
            });
        }
        for (std::uint64_t first = 0; first < folder_count; first += effective_batch_size) {
            const std::uint64_t count = std::min<std::uint64_t>(effective_batch_size, folder_count - first);
            generator.for_each_folder_view(first, count, [&](const GeneratedFolderMetadataView& folder) {
                ++transport_folders_generated;
                if (hash_paths) {
                    shard_mix += metadata_partition_for_path(folder.rel_path);
                }
            });
        }
        if (hash_paths && shard_mix == UINT64_MAX) {
            throw std::runtime_error("unexpected metadata shard checksum");
        }
    };

    const auto run_parent_batch_producer = [&](bool send_to_children) {
        RawBufferPool send_pool(kMetadataBatchBufferPoolId,
                                std::max<std::size_t>(64U, partitions * 4U),
                                sizeof(MetadataBatchBuffer),
                                alignof(MetadataBatchBuffer));
        BufferPoolRegistry send_registry;
        send_registry.register_pool(send_pool);
        std::vector<std::unique_ptr<BufQueue>> sender_queues;
        std::vector<std::unique_ptr<BufferSenderJob>> senders;
        if (send_to_children) {
            sender_queues.reserve(partitions);
            senders.reserve(partitions);
            for (std::size_t index = 0; index < partitions; ++index) {
                sender_queues.push_back(std::make_unique<BufQueue>(std::max<std::size_t>(4U, send_pool.capacity() / partitions)));
                senders.push_back(std::make_unique<BufferSenderJob>(
                    1U,
                    *sender_queues.back(),
                    send_registry,
                    BufferTransportEndpoint::unix_socket(socket_for_partition(index)),
                    metadata_transport_payload_bytes));
                senders.back()->start();
            }
        }

        std::vector<BufferHandle> open_batches(partitions);
        std::vector<bool> has_open_batch(partitions, false);
        const auto acquire_batch = [&](std::size_t partition_index) -> MetadataBatchBuffer& {
            if (!has_open_batch[partition_index]) {
                open_batches[partition_index] = send_pool.acquire_spin();
                reset_metadata_batch(metadata_batch_buffer(send_pool, open_batches[partition_index]));
                has_open_batch[partition_index] = true;
            }
            return metadata_batch_buffer(send_pool, open_batches[partition_index]);
        };
        const auto flush_batch = [&](std::size_t partition_index) {
            if (!has_open_batch[partition_index]) {
                return;
            }
            BufferHandle handle = open_batches[partition_index];
            if (metadata_batch_buffer(send_pool, handle).record_count == 0U) {
                send_pool.release(handle);
            } else if (send_to_children) {
                if (!sender_queues[partition_index]->push_wait(handle)) {
                    send_pool.release(handle);
                    throw std::runtime_error("metadata sender queue closed while pushing batch");
                }
            } else {
                send_pool.release(handle);
            }
            has_open_batch[partition_index] = false;
        };

        FileMetadataGenerator generator(FileMetadataGeneratorConfig(file_count,
                                                                    folder_count,
                                                                    effective_batch_size,
                                                                    average_file_size,
                                                                    0x9e3779b97f4a7c15ULL,
                                                                    "generated"));
        std::uint64_t next_folder = 0;
        const std::uint64_t folder_budget = std::max<std::size_t>(1U, effective_batch_size / 1024U);
        const auto emit_folders = [&](std::uint64_t requested) {
            if (next_folder >= folder_count) {
                return;
            }
            const std::uint64_t count = std::min<std::uint64_t>(requested, folder_count - next_folder);
            generator.for_each_folder_view(next_folder, count, [&](const GeneratedFolderMetadataView& folder) {
                ++transport_folders_generated;
                const std::size_t partition_index = metadata_partition_for_path(folder.rel_path);
                if (!append_metadata_batch_folder(acquire_batch(partition_index), folder)) {
                    flush_batch(partition_index);
                    if (!append_metadata_batch_folder(acquire_batch(partition_index), folder)) {
                        throw std::runtime_error("metadata folder record does not fit metadata batch buffer");
                    }
                }
            });
            next_folder += count;
        };
        for (std::uint64_t first = 0; first < file_count; first += effective_batch_size) {
            const std::uint64_t count = std::min<std::uint64_t>(effective_batch_size, file_count - first);
            generator.for_each_file_view(first, count, [&](const GeneratedFileMetadataView& file) {
                transport_logical_size_bytes += file.declared_size;
                ++transport_files_generated;
                const std::size_t partition_index = metadata_partition_for_path(file.rel_path);
                if (!append_metadata_batch_file(acquire_batch(partition_index), file)) {
                    flush_batch(partition_index);
                    if (!append_metadata_batch_file(acquire_batch(partition_index), file)) {
                        throw std::runtime_error("metadata file record does not fit metadata batch buffer");
                    }
                }
            });
            emit_folders(folder_budget);
        }
        while (next_folder < folder_count) {
            emit_folders(folder_budget);
        }
        for (std::size_t index = 0; index < partitions; ++index) {
            flush_batch(index);
        }
        if (send_to_children) {
            for (auto& queue : sender_queues) {
                queue->close();
            }
            for (auto& sender : senders) {
                sender->wait();
            }
        }
    };

    const auto run_parent_folder_batch_producer = [&]() {
        RawBufferPool send_pool(kMetadataBatchBufferPoolId,
                                std::max<std::size_t>(64U, partitions * 4U),
                                sizeof(MetadataBatchBuffer),
                                alignof(MetadataBatchBuffer));
        const auto make_folder_record = [](const GeneratedFolderMetadataView& view) {
            MetadataFolderRecord folder;
            folder.spec.rel_path.assign(view.rel_path);
            folder.spec.mtime = view.mtime;
            folder.spec.mode = view.mode;
            folder.spec.uid = view.uid;
            folder.spec.gid = view.gid;
            folder.flat_file_count = static_cast<std::size_t>(view.flat_file_count);
            folder.flat_logical_size_bytes = view.flat_logical_size_bytes;
            return folder;
        };

        FileMetadataGenerator generator(FileMetadataGeneratorConfig(file_count,
                                                                    folder_count,
                                                                    effective_batch_size,
                                                                    average_file_size,
                                                                    0x9e3779b97f4a7c15ULL,
                                                                    "generated"));
        const std::uint64_t effective_folder_count = std::max<std::uint64_t>(1U, folder_count);
        for (std::uint64_t folder_index = 0; folder_index < effective_folder_count; ++folder_index) {
            MetadataFolderRecord folder_record;
            generator.for_each_folder_view(folder_index, 1U, [&](const GeneratedFolderMetadataView& folder) {
                folder_record = make_folder_record(folder);
            });
            const bool include_folder_record = folder_index < folder_count;
            if (include_folder_record) {
                ++transport_folders_generated;
            }

            BufferHandle batch_handle = send_pool.acquire_spin();
            if (!reset_folder_metadata_batch(metadata_batch_buffer(send_pool, batch_handle),
                                             folder_record,
                                             include_folder_record)) {
                send_pool.release(batch_handle);
                throw std::runtime_error("metadata folder path does not fit metadata batch buffer");
            }

            for (std::uint64_t file_index = folder_index;
                 file_index < file_count;
                 file_index += effective_folder_count) {
                bool appended = false;
                generator.for_each_file_view(file_index, 1U, [&](const GeneratedFileMetadataView& file) {
                    transport_logical_size_bytes += file.declared_size;
                    ++transport_files_generated;
                    appended = append_folder_metadata_batch_file(metadata_batch_buffer(send_pool, batch_handle),
                                                                 file);
                });
                if (!appended) {
                    send_pool.release(batch_handle);
                    batch_handle = send_pool.acquire_spin();
                    if (!reset_folder_metadata_batch(metadata_batch_buffer(send_pool, batch_handle),
                                                     folder_record,
                                                     false)) {
                        send_pool.release(batch_handle);
                        throw std::runtime_error("metadata folder path does not fit metadata batch buffer");
                    }
                    generator.for_each_file_view(file_index, 1U, [&](const GeneratedFileMetadataView& file) {
                        if (!append_folder_metadata_batch_file(metadata_batch_buffer(send_pool, batch_handle),
                                                               file)) {
                            throw std::runtime_error("metadata file record does not fit folder metadata batch buffer");
                        }
                    });
                }
            }
            send_pool.release(batch_handle);
        }
    };

    const auto started_at = std::chrono::steady_clock::now();
    if (partition_mode == "generate-discard") {
        run_parent_generator_probe(false);
    } else if (partition_mode == "generate-hash-discard") {
        run_parent_generator_probe(true);
    } else if (partition_mode == "pack-discard") {
        run_parent_batch_producer(false);
    } else if (partition_mode == "folder-pack-discard") {
        run_parent_folder_batch_producer();
    } else if (partitions == 1U) {
        partition_reports.front() = run_partition(0);
    } else if (partition_mode == "processes" || partition_mode == "transport-processes" || partition_mode == "transport-discard") {
        std::vector<pid_t> children;
        children.reserve(partitions);
        for (std::size_t index = 0; index < partitions; ++index) {
            std::filesystem::remove(report_for_partition(index));
            std::filesystem::remove(socket_for_partition(index));
            const pid_t child = ::fork();
            if (child < 0) {
                throw std::runtime_error("failed to fork metadata writer partition process");
            }
            if (child == 0) {
                try {
                    write_partition_report(report_for_partition(index),
                                           partition_mode == "transport-processes"
                                               ? run_transport_writer_process(index)
                                               : partition_mode == "transport-discard"
                                                     ? run_transport_discard_process(index)
                                                     : run_partition(index));
                    _Exit(0);
                } catch (const std::exception& error) {
                    std::cerr << "metadata writer partition " << index << " failed: "
                              << error.what() << '\n';
                    _Exit(101);
                } catch (...) {
                    std::cerr << "metadata writer partition " << index << " failed\n";
                    _Exit(101);
                }
            }
            children.push_back(child);
        }

        if (partition_mode == "transport-processes" || partition_mode == "transport-discard") {
            run_parent_batch_producer(true);
        }

        bool child_failed = false;
        for (const pid_t child : children) {
            int status = 0;
            if (::waitpid(child, &status, 0) < 0) {
                throw std::runtime_error("failed waiting for metadata writer partition process");
            }
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                child_failed = true;
            }
        }
        if (child_failed) {
            throw std::runtime_error("one or more metadata writer partition processes failed");
        }
        for (std::size_t index = 0; index < partitions; ++index) {
            partition_reports[index] = read_partition_report(index);
            std::filesystem::remove(socket_for_partition(index));
        }
    } else {
        std::vector<std::thread> workers;
        workers.reserve(partitions);
        for (std::size_t index = 0; index < partitions; ++index) {
            workers.emplace_back([&, index]() {
                try {
                    partition_reports[index] = run_partition(index);
                } catch (...) {
                    std::lock_guard<std::mutex> lock(worker_error_mutex);
                    if (!worker_error) {
                        worker_error = std::current_exception();
                    }
                }
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
        if (worker_error) {
            std::rethrow_exception(worker_error);
        }
    }
    const auto ended_at = std::chrono::steady_clock::now();

    MetadataWriterBenchmarkReport report;
    for (const auto& partition_report : partition_reports) {
        report.files_generated += partition_report.files_generated;
        report.folders_generated += partition_report.folders_generated;
        report.records_written += partition_report.records_written;
        report.logical_size_bytes += partition_report.logical_size_bytes;
    }
    if (partition_mode == "generate-discard" ||
        partition_mode == "generate-hash-discard" ||
        partition_mode == "pack-discard" ||
        partition_mode == "folder-pack-discard" ||
        partition_mode == "transport-processes" ||
        partition_mode == "transport-discard") {
        report.files_generated = transport_files_generated;
        report.folders_generated = transport_folders_generated;
        report.records_written = transport_files_generated + transport_folders_generated;
        report.logical_size_bytes = transport_logical_size_bytes;
    }
    report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
    report.records_per_second = report.elapsed_seconds > 0.0
                                    ? static_cast<double>(report.records_written) / report.elapsed_seconds
                                    : 0.0;
    report.files_per_second = report.elapsed_seconds > 0.0
                                  ? static_cast<double>(report.files_generated) / report.elapsed_seconds
                                  : 0.0;
    report.batch_size = effective_batch_size;
    report.partitions = partitions;
    report.partition_mode = partition_mode;
    report.output_format = to_string(writer_config.format);
    report.output_path = output_path;
    return report;
}

BufferTransportBenchmarkReport TransferEngine::benchmark_buffer_transport(
    std::size_t transports,
    std::uint64_t buffers_per_transport,
    std::size_t buffer_size,
    std::size_t pool_slots_per_transport,
    std::size_t generator_threads,
    std::size_t sender_threads,
    std::size_t receiver_threads,
    std::size_t discarder_threads,
    const std::string& pattern,
    const std::string& transport_kind,
    std::uint16_t base_port,
    const std::filesystem::path& socket_dir,
    bool shared_input_queue) const {
    if (transports == 0U ||
        buffers_per_transport == 0U ||
        buffer_size == 0U ||
        pool_slots_per_transport == 0U ||
        generator_threads == 0U ||
        sender_threads == 0U ||
        receiver_threads == 0U ||
        discarder_threads == 0U) {
        throw std::invalid_argument("buffer transport benchmark numeric parameters must be positive");
    }
    if (transport_kind != "none" && transport_kind != "unix" && transport_kind != "tcp") {
        throw std::invalid_argument("buffer transport benchmark transport must be none, unix, or tcp");
    }
    if (transport_kind == "tcp" &&
        static_cast<unsigned>(base_port) + transports - 1U > 65535U) {
        throw std::invalid_argument("buffer transport benchmark tcp port range exceeds 65535");
    }

    const BufferGeneratorPattern generator_pattern = parse_buffer_generator_pattern(pattern);
    const std::filesystem::path effective_socket_dir =
        socket_dir.empty()
            ? std::filesystem::temp_directory_path() /
                  ("wsync-transport-bench-" + std::to_string(static_cast<unsigned long long>(::getpid())))
            : socket_dir;
    if (transport_kind == "unix") {
        std::filesystem::create_directories(effective_socket_dir);
    }

    if (transport_kind == "none") {
        if (shared_input_queue) {
            const std::size_t total_pool_slots = pool_slots_per_transport * transports;
            const std::uint64_t total_buffers = buffers_per_transport * transports;
            const std::size_t shard_count = std::max<std::size_t>(generator_threads, discarder_threads);
            const std::size_t queue_depth_per_shard =
                std::max<std::size_t>(1U, (total_pool_slots + shard_count - 1U) / shard_count);
            RawBufferPool pool(kDataBufferPoolId, total_pool_slots, buffer_size);
            BufferPoolRegistry registry;
            registry.register_pool(pool);
            ShardedBufQueue queue(shard_count, queue_depth_per_shard);
            BufferDiscarderJob discarder(BufferDiscarderConfig(discarder_threads), queue, registry);
            BufferGeneratorJob generator(
                BufferGeneratorConfig(generator_threads,
                                      total_buffers,
                                      generator_pattern,
                                      0x9e3779b97f4a7c15ULL,
                                      1.0),
                pool,
                queue);

            const auto started_at = std::chrono::steady_clock::now();
            discarder.start();
            generator.start();
            generator.wait();
            discarder.wait();
            const auto ended_at = std::chrono::steady_clock::now();

            const auto generator_stats = generator.stats();
            const auto discarder_stats = discarder.stats();
            BufferTransportBenchmarkReport report;
            report.buffers_generated = generator_stats.buffers_generated;
            report.buffers_discarded = discarder_stats.buffers_discarded;
            report.payload_bytes_sent = generator_stats.bytes_generated;
            report.payload_bytes_received = discarder_stats.bytes_discarded;
            report.buffers_sent = report.buffers_generated;
            report.buffers_received = report.buffers_discarded;
            report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
            report.bytes_per_second = report.elapsed_seconds > 0.0
                                          ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                          : 0.0;
            report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
            report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
            report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
            report.transports = shard_count;
            report.generator_threads = generator_threads;
            report.sender_threads = 0;
            report.receiver_threads = 0;
            report.discarder_threads = discarder_threads;
            report.buffer_size = buffer_size;
            report.pool_slots_per_transport = total_pool_slots;
            report.buffers_per_transport = total_buffers;
            report.pattern = to_string(generator_pattern);
            report.transport_kind = "none-sharded";
            return report;
        }

        struct LocalLane {
            RawBufferPool pool;
            BufferPoolRegistry registry;
            ShardedBufQueue queue;
            std::unique_ptr<BufferDiscarderJob> discarder;
            std::unique_ptr<BufferGeneratorJob> generator;

            LocalLane(BufferPoolId pool_id,
                      std::size_t pool_slots,
                      std::size_t bytes_per_buffer,
                      std::size_t queue_depth)
                : pool(pool_id, pool_slots, bytes_per_buffer),
                  queue(1U, queue_depth) {
                registry.register_pool(pool);
            }
        };

        std::vector<std::unique_ptr<LocalLane>> lanes;
        lanes.reserve(transports);
        const std::size_t queue_depth = std::max<std::size_t>(1U, pool_slots_per_transport);
        for (std::size_t index = 0; index < transports; ++index) {
            auto lane = std::make_unique<LocalLane>(kDataBufferPoolId,
                                                    pool_slots_per_transport,
                                                    buffer_size,
                                                    queue_depth);
            lane->discarder = std::make_unique<BufferDiscarderJob>(BufferDiscarderConfig(discarder_threads),
                                                                   lane->queue,
                                                                   lane->registry);
            lane->generator = std::make_unique<BufferGeneratorJob>(
                BufferGeneratorConfig(generator_threads,
                                      buffers_per_transport,
                                      generator_pattern,
                                      0x9e3779b97f4a7c15ULL ^ static_cast<std::uint64_t>(index),
                                      1.0),
                lane->pool,
                lane->queue);
            lanes.push_back(std::move(lane));
        }

        const auto started_at = std::chrono::steady_clock::now();
        for (auto& lane : lanes) {
            lane->discarder->start();
        }
        for (auto& lane : lanes) {
            lane->generator->start();
        }
        BufferTransportBenchmarkReport report;
        for (auto& lane : lanes) {
            lane->generator->wait();
        }
        for (auto& lane : lanes) {
            lane->discarder->wait();
        }
        const auto ended_at = std::chrono::steady_clock::now();

        for (const auto& lane : lanes) {
            const auto generator_stats = lane->generator->stats();
            const auto discarder_stats = lane->discarder->stats();
            report.buffers_generated += generator_stats.buffers_generated;
            report.buffers_discarded += discarder_stats.buffers_discarded;
            report.payload_bytes_sent += generator_stats.bytes_generated;
            report.payload_bytes_received += discarder_stats.bytes_discarded;
        }
        report.buffers_sent = report.buffers_generated;
        report.buffers_received = report.buffers_discarded;
        report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
        report.bytes_per_second = report.elapsed_seconds > 0.0
                                      ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                      : 0.0;
        report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
        report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
        report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
        report.transports = transports;
        report.generator_threads = generator_threads;
        report.sender_threads = 0;
        report.receiver_threads = 0;
        report.discarder_threads = discarder_threads;
        report.buffer_size = buffer_size;
        report.pool_slots_per_transport = pool_slots_per_transport;
        report.buffers_per_transport = buffers_per_transport;
        report.pattern = to_string(generator_pattern);
        report.transport_kind = transport_kind;
        return report;
    }

    if (shared_input_queue) {
        const std::size_t total_pool_slots = pool_slots_per_transport * transports;
        const std::uint64_t total_buffers = buffers_per_transport * transports;
        RawBufferPool send_pool(kDataBufferPoolId, total_pool_slots, buffer_size);
        BufferPoolRegistry send_registry;
        send_registry.register_pool(send_pool);
        BufQueue sender_input(std::max<std::size_t>(1U, total_pool_slots));
        BufferGeneratorJob generator(BufferGeneratorConfig(generator_threads,
                                                           total_buffers,
                                                           generator_pattern,
                                                           0x9e3779b97f4a7c15ULL,
                                                           1.0),
                                     send_pool,
                                     sender_input);

        std::vector<std::unique_ptr<RawBufferPool>> receive_pools;
        std::vector<std::unique_ptr<BufferPoolRegistry>> receive_registries;
        std::vector<std::unique_ptr<BufQueue>> receiver_outputs;
        std::vector<std::unique_ptr<BufferReceiverJob>> receivers;
        std::vector<std::unique_ptr<BufferDiscarderJob>> discarders;
        std::vector<std::unique_ptr<BufferSenderJob>> senders;
        receive_pools.reserve(transports);
        receive_registries.reserve(transports);
        receiver_outputs.reserve(transports);
        receivers.reserve(transports);
        discarders.reserve(transports);
        senders.reserve(transports);

        for (std::size_t index = 0; index < transports; ++index) {
            BufferTransportEndpoint endpoint =
                transport_kind == "unix"
                    ? BufferTransportEndpoint::unix_socket(effective_socket_dir /
                                                           ("transport-" + std::to_string(index) + ".sock"))
                    : BufferTransportEndpoint::tcp("127.0.0.1", static_cast<std::uint16_t>(base_port + index));
            receive_pools.push_back(std::make_unique<RawBufferPool>(kDataBufferPoolId,
                                                                     pool_slots_per_transport,
                                                                     buffer_size));
            receive_registries.push_back(std::make_unique<BufferPoolRegistry>());
            receive_registries.back()->register_pool(*receive_pools.back());
            receiver_outputs.push_back(std::make_unique<BufQueue>(std::max<std::size_t>(1U, pool_slots_per_transport)));
            receivers.push_back(std::make_unique<BufferReceiverJob>(receiver_threads,
                                                                    *receive_pools.back(),
                                                                    *receiver_outputs.back(),
                                                                    endpoint));
            discarders.push_back(std::make_unique<BufferDiscarderJob>(BufferDiscarderConfig(discarder_threads),
                                                                      *receiver_outputs.back(),
                                                                      *receive_registries.back()));
            senders.push_back(std::make_unique<BufferSenderJob>(sender_threads,
                                                                sender_input,
                                                                send_registry,
                                                                std::move(endpoint)));
        }

        const auto started_at = std::chrono::steady_clock::now();
        for (std::size_t index = 0; index < transports; ++index) {
            receivers[index]->start();
            discarders[index]->start();
        }
        for (auto& sender : senders) {
            sender->start();
        }
        generator.start();

        generator.wait();
        for (auto& sender : senders) {
            sender->wait();
        }
        for (auto& receiver : receivers) {
            receiver->wait();
        }
        for (auto& discarder : discarders) {
            discarder->wait();
        }
        const auto ended_at = std::chrono::steady_clock::now();

        BufferTransportBenchmarkReport report;
        const auto generator_stats = generator.stats();
        report.buffers_generated = generator_stats.buffers_generated;
        report.payload_bytes_sent = generator_stats.bytes_generated;
        for (std::size_t index = 0; index < transports; ++index) {
            const auto sender_stats = senders[index]->stats();
            const auto receiver_stats = receivers[index]->stats();
            const auto discarder_stats = discarders[index]->stats();
            report.buffers_sent += sender_stats.buffers;
            report.buffers_received += receiver_stats.buffers;
            report.buffers_discarded += discarder_stats.buffers_discarded;
            report.payload_bytes_received += receiver_stats.payload_bytes;
        }
        report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
        report.bytes_per_second = report.elapsed_seconds > 0.0
                                      ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                      : 0.0;
        report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
        report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
        report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
        report.transports = transports;
        report.generator_threads = generator_threads;
        report.sender_threads = sender_threads;
        report.receiver_threads = receiver_threads;
        report.discarder_threads = discarder_threads;
        report.buffer_size = buffer_size;
        report.pool_slots_per_transport = pool_slots_per_transport;
        report.buffers_per_transport = buffers_per_transport;
        report.pattern = to_string(generator_pattern);
        report.transport_kind = transport_kind + "-shared";
        if (transport_kind == "unix" && socket_dir.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(effective_socket_dir, ignored);
        }
        return report;
    }

    struct TransportLane {
        RawBufferPool send_pool;
        RawBufferPool receive_pool;
        BufferPoolRegistry send_registry;
        BufferPoolRegistry receive_registry;
        BufQueue generator_to_sender;
        BufQueue receiver_to_discarder;
        BufferTransportEndpoint endpoint;
        std::unique_ptr<BufferReceiverJob> receiver;
        std::unique_ptr<BufferDiscarderJob> discarder;
        std::unique_ptr<BufferSenderJob> sender;
        std::unique_ptr<BufferGeneratorJob> generator;

        TransportLane(BufferPoolId pool_id,
                      std::size_t pool_slots,
                      std::size_t bytes_per_buffer,
                      std::size_t queue_depth,
                      BufferTransportEndpoint lane_endpoint)
            : send_pool(pool_id, pool_slots, bytes_per_buffer),
              receive_pool(pool_id, pool_slots, bytes_per_buffer),
              generator_to_sender(queue_depth),
              receiver_to_discarder(queue_depth),
              endpoint(std::move(lane_endpoint)) {
            send_registry.register_pool(send_pool);
            receive_registry.register_pool(receive_pool);
        }
    };

    std::vector<std::unique_ptr<TransportLane>> lanes;
    lanes.reserve(transports);
    const std::size_t queue_depth = std::max<std::size_t>(1U, pool_slots_per_transport);
    for (std::size_t index = 0; index < transports; ++index) {
        BufferTransportEndpoint endpoint =
            transport_kind == "unix"
                ? BufferTransportEndpoint::unix_socket(effective_socket_dir /
                                                       ("transport-" + std::to_string(index) + ".sock"))
                : BufferTransportEndpoint::tcp("127.0.0.1", static_cast<std::uint16_t>(base_port + index));
        auto lane = std::make_unique<TransportLane>(kDataBufferPoolId,
                                                    pool_slots_per_transport,
                                                    buffer_size,
                                                    queue_depth,
                                                    std::move(endpoint));
        lane->receiver = std::make_unique<BufferReceiverJob>(receiver_threads,
                                                             lane->receive_pool,
                                                             lane->receiver_to_discarder,
                                                             lane->endpoint);
        lane->discarder = std::make_unique<BufferDiscarderJob>(
            BufferDiscarderConfig(discarder_threads),
            lane->receiver_to_discarder,
            lane->receive_registry);
        lane->sender = std::make_unique<BufferSenderJob>(sender_threads,
                                                         lane->generator_to_sender,
                                                         lane->send_registry,
                                                         lane->endpoint);
        lane->generator = std::make_unique<BufferGeneratorJob>(
            BufferGeneratorConfig(generator_threads,
                                  buffers_per_transport,
                                  generator_pattern,
                                  0x9e3779b97f4a7c15ULL ^ static_cast<std::uint64_t>(index),
                                  1.0),
            lane->send_pool,
            lane->generator_to_sender);
        lanes.push_back(std::move(lane));
    }

    const auto started_at = std::chrono::steady_clock::now();
    for (auto& lane : lanes) {
        lane->receiver->start();
        lane->discarder->start();
    }
    for (auto& lane : lanes) {
        lane->sender->start();
    }
    for (auto& lane : lanes) {
        lane->generator->start();
    }

    BufferTransportBenchmarkReport report;
    for (auto& lane : lanes) {
        lane->generator->wait();
    }
    for (auto& lane : lanes) {
        lane->sender->wait();
    }
    for (auto& lane : lanes) {
        lane->receiver->wait();
    }
    for (auto& lane : lanes) {
        lane->discarder->wait();
    }
    const auto ended_at = std::chrono::steady_clock::now();

    for (const auto& lane : lanes) {
        const auto generator_stats = lane->generator->stats();
        const auto sender_stats = lane->sender->stats();
        const auto receiver_stats = lane->receiver->stats();
        const auto discarder_stats = lane->discarder->stats();
        report.buffers_generated += generator_stats.buffers_generated;
        report.buffers_sent += sender_stats.buffers;
        report.buffers_received += receiver_stats.buffers;
        report.buffers_discarded += discarder_stats.buffers_discarded;
        report.payload_bytes_sent += sender_stats.payload_bytes;
        report.payload_bytes_received += receiver_stats.payload_bytes;
    }
    report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
    report.bytes_per_second = report.elapsed_seconds > 0.0
                                  ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                  : 0.0;
    report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
    report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
    report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
    report.transports = transports;
    report.generator_threads = generator_threads;
    report.sender_threads = sender_threads;
    report.receiver_threads = receiver_threads;
    report.discarder_threads = discarder_threads;
    report.buffer_size = buffer_size;
    report.pool_slots_per_transport = pool_slots_per_transport;
    report.buffers_per_transport = buffers_per_transport;
    report.pattern = to_string(generator_pattern);
    report.transport_kind = transport_kind;

    if (transport_kind == "unix" && socket_dir.empty()) {
        std::error_code ignored;
        std::filesystem::remove_all(effective_socket_dir, ignored);
    }
    return report;
}

TransferReport TransferEngine::transfer_directory(const SenderRuntimeConfig& runtime) const {
    auto files = scan_directory_with_config(runtime.source_root, runtime.recursive);
    auto directories = scan_directories_for_transfer(runtime.source_root, runtime.recursive);
    std::sort(files.begin(), files.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });
    std::sort(directories.begin(), directories.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
        if (path_depth(lhs.rel_path) != path_depth(rhs.rel_path)) {
            return path_depth(lhs.rel_path) < path_depth(rhs.rel_path);
        }
        return lhs.rel_path < rhs.rel_path;
    });

    TransferReport report;
    report.mode = Mode::transfer;
    report.files_total = files.size();
    report.folders_total = 0;
    for (const auto& file : files) {
        initialize_folder_report(report, file);
        report.bytes_planned += file.declared_size;
    }
    report.folders_total = report.folders.size();

    ScopedFd priority_fd = connect_tcp(runtime.remote_host, runtime.priority_port);
    ScopedFd data_fd = connect_tcp(runtime.remote_host, runtime.data_port);
    send_session_start(priority_fd.get(), config_);
    for (const auto& directory : directories) {
        send_directory_record(priority_fd.get(), make_directory_record_message(directory));
    }

    const bool remote_source = is_nfs_url(runtime.source_root.string());
    NfsDataReaderConfig data_reader_config = load_nfs_data_reader_config(config_store_);
    data_reader_config.small_file_threshold = config_.small_file_threshold;
    data_reader_config.large_chunk_bytes = config_.large_chunk_bytes;
    data_reader_config.source_root = runtime.source_root.string();
    NfsDataReader data_reader(data_reader_config);
    const bool enable_cache = !runtime.cache_root.empty();
    DataCacherConfig cache_config = load_data_cacher_config(config_store_);
    cache_config.role = EndpointRole::sender;
    if (!runtime.cache_root.empty()) {
        cache_config.cache_path = runtime.cache_root.string();
    }
    DataCacher cacher(cache_config);
    DataSlotPool sender_slots(config_.small_pool_slots, config_.large_pool_slots);
    DataSlotPool cache_slots(config_.small_pool_slots, config_.large_pool_slots);
    bool paused = false;
    std::deque<FileAckMessage> pending_acks;
    std::vector<PendingTransferFile> decision_files;
    decision_files.reserve(files.size());
    std::vector<PendingTransferFile> pending_files;
    pending_files.reserve(files.size());

    for (const auto& file : files) {
        const FileRecordMessage record = make_file_record_message(file);
        send_file_record(priority_fd.get(), record);
        decision_files.push_back(PendingTransferFile{file, record});
    }

    for (const auto& pending : decision_files) {
        PriorityMessageType decision_type = PriorityMessageType::session_end;
        std::string decision_payload;
        if (!read_priority_payload(priority_fd.get(), decision_type, decision_payload) ||
            decision_type != PriorityMessageType::file_decision) {
            throw std::runtime_error("expected file decision from receiver");
        }

        const FileDecisionMessage decision = decode_file_decision(decision_payload);
        if (decision.file_id != pending.record.file_id) {
            throw std::runtime_error("receiver decision file_id mismatch");
        }

        FileOutcome outcome;
        outcome.rel_path = pending.file.rel_path;
        outcome.size = pending.file.declared_size;
        outcome.sender_state = FileState::checking;
        outcome.receiver_state = FileState::pending;

        FolderRecord& folder = report.folders[parent_path(pending.file.rel_path)];
        if (decision.skip) {
            outcome.diff = DiffKind::skip;
            outcome.sender_state = FileState::done;
            outcome.receiver_state = FileState::done;
            outcome.hash_verified = true;
            ++report.files_skipped;
            ++folder.files_skipped;
            ++folder.files_completed;
            ++folder.files_received;
            report.files[pending.file.rel_path] = outcome;
            continue;
        }
        pending_files.push_back(pending);
    }

    PreparedTransferQueue prepared_queue;
    prepared_queue.max_entries = std::max<std::size_t>(1, data_reader.config().large_file_parallelism);
    prepared_queue.queue.reset(prepared_queue.max_entries);
    std::thread prepare_thread(produce_prepared_transfers,
                               std::ref(prepared_queue),
                               std::cref(pending_files),
                               remote_source,
                               std::cref(runtime.source_root),
                               std::ref(data_reader),
                               std::ref(cacher),
                               std::ref(cache_slots),
                               std::cref(config_),
                               runtime.cache_file_threshold_bytes,
                               enable_cache);

    std::unordered_map<std::uint64_t, AwaitingAckTransfer> awaiting_acks;
    awaiting_acks.reserve(pending_files.size());

    try {
        PreparedTransfer prepared;
        std::optional<PreparedTransfer> carried_prepared;
        while (carried_prepared.has_value() || pop_prepared_transfer(prepared_queue, prepared)) {
            if (carried_prepared.has_value()) {
                prepared = std::move(*carried_prepared);
                carried_prepared.reset();
            }

            if (can_pack_small_file(prepared, config_)) {
                std::vector<PreparedTransfer> batch;
                batch.push_back(std::move(prepared));
                std::size_t packed_bytes = kPackedSmallFileCountBytes + packed_small_file_entry_bytes(batch.back());

                PreparedTransfer next;
                while (try_pop_prepared_transfer(prepared_queue, next)) {
                    if (!can_pack_small_file(next, config_)) {
                        carried_prepared = std::move(next);
                        break;
                    }
                    const std::size_t next_bytes = packed_small_file_entry_bytes(next);
                    if (packed_bytes + next_bytes > config_.large_chunk_bytes) {
                        carried_prepared = std::move(next);
                        break;
                    }
                    packed_bytes += next_bytes;
                    batch.push_back(std::move(next));
                }

                const std::vector<std::uint64_t> hashes =
                    send_packed_small_file_batch(data_fd.get(),
                                                 sender_slots,
                                                 batch,
                                                 config_,
                                                 priority_fd.get(),
                                                 paused,
                                                 pending_acks,
                                                 report);
                for (std::size_t index = 0; index < batch.size(); ++index) {
                    const std::uint64_t file_id = batch[index].record.file_id;
                    awaiting_acks.emplace(file_id,
                                          AwaitingAckTransfer{std::move(batch[index]), hashes[index], 1U});
                }
                consume_sender_acks(pending_acks, awaiting_acks, report);
                continue;
            }

            if (prepared.cached) {
                for (const std::uint64_t entry_id : prepared.cached_entry_ids) {
                    drain_sender_priority_events(priority_fd.get(), 0, paused, pending_acks);
                    wait_for_sender_resume(priority_fd.get(), paused, pending_acks);

                    const DataSlotHandle handle = cacher.take_slot(entry_id, sender_slots);
                    send_data_slot(data_fd.get(), sender_slots, handle);
                    sender_slots.release(handle);
                    ++report.chunks_sent;
                }
            } else if (prepared.content_loaded) {
                send_content_file_slots(data_fd.get(),
                                        sender_slots,
                                        prepared.file,
                                        config_,
                                        priority_fd.get(),
                                        paused,
                                        pending_acks,
                                        report);
            } else {
                prepared.data_hash = send_local_file_slots(data_fd.get(),
                                                           sender_slots,
                                                           prepared.source_path,
                                                           prepared.file,
                                                           config_,
                                                           priority_fd.get(),
                                                           paused,
                                                           pending_acks,
                                                           report);
            }

            const std::uint64_t file_id = prepared.record.file_id;
            const std::uint64_t data_hash = prepared.data_hash;
            const std::size_t chunk_count = prepared.chunk_count;
            awaiting_acks.emplace(file_id,
                                  AwaitingAckTransfer{std::move(prepared), data_hash, chunk_count});
            consume_sender_acks(pending_acks, awaiting_acks, report);
        }

        while (!awaiting_acks.empty()) {
            const FileAckMessage ack = wait_for_sender_ack(priority_fd.get(), paused, pending_acks);
            record_sender_ack(ack, awaiting_acks, report);
            consume_sender_acks(pending_acks, awaiting_acks, report);
        }
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(prepared_queue.mutex);
            prepared_queue.stop = true;
        }
        prepared_queue.cv_not_full.notify_all();
        prepared_queue.cv_not_empty.notify_all();
        prepare_thread.join();
        throw;
    }

    prepare_thread.join();

    send_session_end(priority_fd.get());
    ::shutdown(data_fd.get(), SHUT_WR);
    finalize_folder_report(report);
    return report;
}

void TransferEngine::run_receiver(const ReceiverRuntimeConfig& runtime) const {
    if (!is_nfs_url(runtime.target_root.string())) {
        std::filesystem::create_directories(runtime.target_root);
    }

    ScopedFd priority_listener = listen_tcp(runtime.bind_host, runtime.priority_port);
    ScopedFd data_listener = listen_tcp(runtime.bind_host, runtime.data_port);
    ScopedFd priority_fd = accept_tcp(priority_listener.get());
    ScopedFd data_fd = accept_tcp(data_listener.get());

    ReceiverSharedState state;
    state.runtime = runtime;
    state.skip_verify = config_.skip_verify;
    state.small_pool_slots = config_.small_pool_slots;
    state.large_pool_slots = config_.large_pool_slots;
    state.priority_fd = priority_fd.get();
    state.data_fd = data_fd.get();

    std::exception_ptr priority_error;
    std::exception_ptr data_error;

    std::thread priority_thread([&] {
        try {
            receiver_priority_loop(state);
        } catch (...) {
            priority_error = std::current_exception();
            shutdown_receiver_sockets(state);
        }
    });

    std::thread data_thread([&] {
        try {
            receiver_data_loop(state, data_fd.get());
        } catch (...) {
            data_error = std::current_exception();
            shutdown_receiver_sockets(state);
        }
    });

    priority_thread.join();
    data_thread.join();

    if (priority_error != nullptr) {
        std::rethrow_exception(priority_error);
    }
    if (data_error != nullptr) {
        std::rethrow_exception(data_error);
    }

    if (!state.directory_specs.empty()) {
        std::sort(state.directory_specs.begin(), state.directory_specs.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
            if (path_depth(lhs.rel_path) != path_depth(rhs.rel_path)) {
                return path_depth(lhs.rel_path) > path_depth(rhs.rel_path);
            }
            return lhs.rel_path > rhs.rel_path;
        });
        auto target_writer = make_target_writer_backend(runtime.target_root.string());
        for (const auto& directory : state.directory_specs) {
            target_writer->apply_directory_metadata(directory);
        }
    }
}

ScanIndex TransferEngine::load_scan_csv(const std::filesystem::path& input_path) {
    return ScanIndex::from_csv(read_text_file(input_path));
}

void TransferEngine::write_scan_csv(const ScanIndex& index, const std::filesystem::path& output_path) {
    write_text_file(output_path, index.to_csv());
}

void TransferEngine::write_diff_csv(const TransferReport& report, const std::filesystem::path& output_path) {
    write_text_file(output_path, report.diff_csv);
}

}  // namespace hypersync
