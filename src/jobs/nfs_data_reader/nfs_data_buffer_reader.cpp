#include "jobs/nfs_data_reader/nfs_data_buffer_reader.hpp"

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"
#include "common/records.hpp"
#include "core/data_buffer_codec.hpp"
#include "core/nfs_backend.hpp"
#include "core/pipeline_buffers.hpp"

namespace hypersync {
namespace {

class NfsDataReaderStopped final : public std::exception {
public:
    [[nodiscard]] const char* what() const noexcept override {
        return "NFS data buffer reader stopped";
    }
};

[[nodiscard]] std::uint64_t file_logical_size(const FileSpec& file) {
    return file.declared_size != 0 ? file.declared_size : file.content.size();
}

}  // namespace

NfsDataBufferReaderJob::NfsDataBufferReaderJob(NfsDataReaderConfig config,
                                               RawBufferPool& data_pool,
                                               BufQueue& output,
                                               FileProvider file_provider,
                                               StopPredicate stop_predicate)
    : ThreadedJob(std::max<std::size_t>(1, config.data_reader_worker_count)),
      config_(std::move(config)),
      data_pool_(data_pool),
      output_(&output),
      file_provider_(std::move(file_provider)),
      stop_predicate_(std::move(stop_predicate)) {
    if (data_pool_.pool_id() != kDataBufferPoolId) {
        throw std::invalid_argument("nfs data buffer reader requires the data buffer pool");
    }
    if (!file_provider_) {
        throw std::invalid_argument("nfs data buffer reader requires a file provider");
    }
}

NfsDataBufferReaderJob::NfsDataBufferReaderJob(NfsDataReaderConfig config,
                                               RawBufferPool& data_pool,
                                               ShardedBufQueue& output,
                                               FileProvider file_provider,
                                               StopPredicate stop_predicate)
    : ThreadedJob(std::max<std::size_t>(1, config.data_reader_worker_count)),
      config_(std::move(config)),
      data_pool_(data_pool),
      sharded_output_(&output),
      file_provider_(std::move(file_provider)),
      stop_predicate_(std::move(stop_predicate)) {
    if (data_pool_.pool_id() != kDataBufferPoolId) {
        throw std::invalid_argument("nfs data buffer reader requires the data buffer pool");
    }
    if (!file_provider_) {
        throw std::invalid_argument("nfs data buffer reader requires a file provider");
    }
}

NfsDataBufferReaderJob::NfsDataBufferReaderJob(NfsDataReaderConfig config,
                                               RawBufferPool& data_pool,
                                               BufferConsumer output,
                                               FileProvider file_provider,
                                               StopPredicate stop_predicate)
    : ThreadedJob(std::max<std::size_t>(1, config.data_reader_worker_count)),
      config_(std::move(config)),
      data_pool_(data_pool),
      direct_output_(std::move(output)),
      file_provider_(std::move(file_provider)),
      stop_predicate_(std::move(stop_predicate)) {
    if (data_pool_.pool_id() != kDataBufferPoolId) {
        throw std::invalid_argument("nfs data buffer reader requires the data buffer pool");
    }
    if (!direct_output_) {
        throw std::invalid_argument("nfs data buffer reader requires a direct output consumer");
    }
    if (!file_provider_) {
        throw std::invalid_argument("nfs data buffer reader requires a file provider");
    }
}

NfsDataBufferReaderJob::~NfsDataBufferReaderJob() {
    stop();
}

void NfsDataBufferReaderJob::set_bytes_read_callback(BytesReadCallback callback) {
    bytes_read_callback_ = std::move(callback);
}

void NfsDataBufferReaderJob::set_file_read_callback(FileReadCallback callback) {
    file_read_callback_ = std::move(callback);
}

void NfsDataBufferReaderJob::set_file_failed_callback(FileFailedCallback callback) {
    file_failed_callback_ = std::move(callback);
}

NfsDataBufferReaderStats NfsDataBufferReaderJob::stats() const {
    NfsDataBufferReaderStats snapshot;
    snapshot.running = running();
    snapshot.worker_count = worker_count();
    snapshot.files_read = files_read_.load(std::memory_order_acquire);
    snapshot.files_failed = files_failed_.load(std::memory_order_acquire);
    snapshot.buffers_read = buffers_read_.load(std::memory_order_acquire);
    snapshot.bytes_read = bytes_read_.load(std::memory_order_acquire);
    return snapshot;
}

void NfsDataBufferReaderJob::run_worker(std::size_t worker_index) {
    NfsDataReaderConfig worker_config = config_;
    worker_config.endpoint_index = worker_index;
    NfsDataReader reader(std::move(worker_config));
    std::optional<FileSpec> carried_file;
    while (!should_stop_now()) {
        if (!wait_until_worker_active(worker_index)) {
            break;
        }
        std::optional<FileSpec> file;
        if (carried_file.has_value()) {
            file = std::move(carried_file);
            carried_file.reset();
        } else {
            file = file_provider_();
        }
        if (!file.has_value()) {
            break;
        }
        if (normalize_path(file->rel_path).empty()) {
            record_file_failed(*file);
            continue;
        }

        try {
            const std::uint64_t logical_size = file_logical_size(*file);
            if (config_.copy_data_from_nfs &&
                config_.pack_small_files &&
                logical_size <= config_.small_file_threshold) {
                FileSpec first_file = std::move(*file);
                bool first_pending = true;
                const PackedSmallFilesReadStats packed_stats = reader.read_small_files_packed(
                    [&]() -> std::optional<FileSpec> {
                        if (first_pending) {
                            first_pending = false;
                            return std::move(first_file);
                        }
                        std::optional<FileSpec> next_file = file_provider_();
                        if (!next_file.has_value()) {
                            return std::nullopt;
                        }
                        if (file_logical_size(*next_file) > config_.small_file_threshold) {
                            carried_file = std::move(*next_file);
                            return std::nullopt;
                        }
                        return next_file;
                    },
                    data_pool_,
                    [&](BufferHandle handle, std::uint64_t file_count, std::uint64_t bytes_read) {
                        if (!publish_buffer(worker_index, handle)) {
                            data_pool_.release(handle);
                            throw NfsDataReaderStopped {};
                        }
                        record_bytes_read(bytes_read);
                        record_files_read(file_count);
                    },
                    [this]() {
                        return should_stop_now();
                    });
                record_files_failed(packed_stats.files_failed);
            } else if (config_.copy_data_from_nfs &&
                       !config_.pack_small_files &&
                       config_.small_file_async_window > 1U &&
                       logical_size <= config_.small_file_threshold) {
                FileSpec first_file = std::move(*file);
                bool first_pending = true;
                std::uint64_t published_files = 0;
                const PackedSmallFilesReadStats window_stats = reader.read_small_files_raw_window(
                    [&]() -> std::optional<FileSpec> {
                        if (first_pending) {
                            first_pending = false;
                            return std::move(first_file);
                        }
                        std::optional<FileSpec> next_file = file_provider_();
                        if (!next_file.has_value()) {
                            return std::nullopt;
                        }
                        if (file_logical_size(*next_file) > config_.small_file_threshold) {
                            carried_file = std::move(*next_file);
                            return std::nullopt;
                        }
                        return next_file;
                    },
                    data_pool_,
                    [&](RawSmallFileRead&& completed) {
                        DataBuffer& buffer = data_buffer(data_pool_, completed.handle);
                        const RecBuf record = make_recbuf(completed.file);
                        complete_trailer(record,
                                         file_logical_size(completed.file),
                                         buffer.trailer,
                                         0);
                        if (!publish_buffer(worker_index, completed.handle)) {
                            data_pool_.release(completed.handle);
                            throw NfsDataReaderStopped {};
                        }
                        record_bytes_read(completed.bytes_read);
                        ++published_files;
                        record_file_read();
                    },
                    [this]() {
                        return should_stop_now();
                    });
                if (window_stats.files_read > published_files) {
                    record_files_read(window_stats.files_read - published_files);
                }
                record_files_failed(window_stats.files_failed);
            } else {
                publish_file_chunks(reader, *file, worker_index);
                record_file_read();
            }
        } catch (const NfsDataReaderStopped&) {
            break;
        } catch (...) {
            record_file_failed(*file);
        }
    }
}

void NfsDataBufferReaderJob::on_stop_requested() {
    if (output_ != nullptr) {
        output_->close();
    }
    if (sharded_output_ != nullptr) {
        sharded_output_->close();
    }
}

void NfsDataBufferReaderJob::on_all_workers_finished() {
    if (output_ != nullptr) {
        output_->close();
    }
    if (sharded_output_ != nullptr) {
        sharded_output_->close();
    }
}

void NfsDataBufferReaderJob::publish_file_chunks(NfsDataReader& reader,
                                                 const FileSpec& file,
                                                 std::size_t worker_index) {
    const RecBuf record = make_recbuf(file);
    const std::uint64_t logical_size = file_logical_size(file);

    const std::uint64_t bytes_streamed =
        reader.stream_file_raw_chunks(file,
                                      data_pool_,
                                      [&](RawFileChunk&& chunk) {
                                          DataBuffer& buffer = data_buffer(data_pool_, chunk.handle);
                                          const std::uint64_t bytes_read = buffer.trailer.data_len;
                                          complete_trailer(record, logical_size, buffer.trailer, chunk.offset);

                                          if (!publish_buffer(worker_index, chunk.handle)) {
                                              data_pool_.release(chunk.handle);
                                              throw NfsDataReaderStopped {};
                                          }
                                          record_bytes_read(bytes_read);
                                      },
                                      [this]() {
                                          return should_stop_now();
                                      });
    if (should_stop_now() && bytes_streamed < logical_size) {
        throw NfsDataReaderStopped {};
    }
}

bool NfsDataBufferReaderJob::publish_buffer(std::size_t worker_index, const BufferHandle& handle) {
    if (direct_output_) {
        return direct_output_(worker_index, handle);
    }
    if (output_ != nullptr) {
        return wait_for_output(worker_index, *output_, handle);
    }
    if (sharded_output_ == nullptr) {
        return false;
    }
    const DataBuffer& buffer = data_buffer(data_pool_, handle);
    const std::size_t shard = output_shard_for(buffer);
    if (sharded_output_->try_push(shard, handle)) {
        return true;
    }
    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_output_full);
    return sharded_output_->push_wait(shard, handle);
}

std::size_t NfsDataBufferReaderJob::output_shard_for(const DataBuffer& buffer) const {
    if (sharded_output_ == nullptr || sharded_output_->shard_count() == 0U) {
        return 0U;
    }
    const std::uint64_t key = is_packed_small_file_buffer(buffer) && buffer.trailer.folder_hash != 0U
                                  ? buffer.trailer.folder_hash
                                  : buffer.trailer.file_id;
    return static_cast<std::size_t>(key % sharded_output_->shard_count());
}

void NfsDataBufferReaderJob::complete_trailer(const RecBuf& record,
                                              std::uint64_t logical_size,
                                              DataBufTrailer& trailer,
                                              std::uint64_t chunk_offset) const {
    trailer.file_id = record.own_hash;
    trailer.folder_hash = record.folder_hash;
    trailer.data_offset = chunk_offset;
    trailer.file_size = logical_size;
    trailer.data_hash = 0;
    trailer.mtime = record.mtime;
    trailer.mode = record.mode;
    trailer.uid = record.uid;
    trailer.gid = record.gid;
    trailer.rel_path = record.rel_path;
    trailer.flags = logical_size <= config_.small_file_threshold ? kFlagSmallFile : 0U;
    if (chunk_offset + trailer.data_len >= logical_size) {
        trailer.flags |= kFlagLastChunk;
    }
}

bool NfsDataBufferReaderJob::should_stop_now() const {
    return stop_requested() || (stop_predicate_ && stop_predicate_());
}

void NfsDataBufferReaderJob::record_bytes_read(std::uint64_t bytes_read) {
    buffers_read_.fetch_add(1, std::memory_order_relaxed);
    bytes_read_.fetch_add(bytes_read, std::memory_order_relaxed);
    if (bytes_read_callback_) {
        bytes_read_callback_(bytes_read);
    }
}

void NfsDataBufferReaderJob::record_file_read() {
    record_files_read(1);
}

void NfsDataBufferReaderJob::record_files_read(std::uint64_t file_count) {
    files_read_.fetch_add(file_count, std::memory_order_relaxed);
    if (file_read_callback_) {
        for (std::uint64_t index = 0; index < file_count; ++index) {
            file_read_callback_();
        }
    }
}

void NfsDataBufferReaderJob::record_file_failed(const FileSpec& file) {
    record_files_failed(1);
    if (file_failed_callback_) {
        file_failed_callback_(file);
    }
}

void NfsDataBufferReaderJob::record_files_failed(std::uint64_t file_count) {
    files_failed_.fetch_add(file_count, std::memory_order_relaxed);
}

}  // namespace hypersync
