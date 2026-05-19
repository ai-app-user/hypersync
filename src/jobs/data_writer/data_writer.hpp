#ifndef HYPERSYNC_JOBS_DATA_WRITER_HPP
#define HYPERSYNC_JOBS_DATA_WRITER_HPP

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/buffer_pool.hpp"
#include "common/types.hpp"
#include "core/pipeline_buffers.hpp"
#include "jobs/queue_job.hpp"
#include "jobs/threaded_job.hpp"

namespace hypersync {

class ConfigStore;
class TargetWriterBackend;

struct DataWriterConfig {
    std::size_t handle_cache_limit;
    bool verify_hash;

    DataWriterConfig();
    DataWriterConfig(std::size_t handle_cache_limit, bool verify_hash);
};

[[nodiscard]] DataWriterConfig load_data_writer_config(const ConfigStore& config);

class DataWriter : public TypedQueueJob<DataChunk> {
public:
    explicit DataWriter(DataWriterConfig config = {});

    void predeclare_directory(std::string path);
    [[nodiscard]] bool known_directory(std::string_view path) const;
    void queue_chunk(DataChunk chunk);
    [[nodiscard]] ChunkProgress progress_for(std::uint64_t file_id) const;
    [[nodiscard]] std::size_t completed_files() const;
    [[nodiscard]] const DataWriterConfig& config() const;

private:
    DataWriterConfig config_;
    std::unordered_set<std::string> dir_cache_;
    std::unordered_set<std::uint64_t> completed_file_ids_;
    std::unordered_map<std::uint64_t, ChunkProgress> progress_;
    std::size_t completed_files_ = 0;
};

struct TargetMetaWriterConfig {
    std::size_t worker_count = 1;
    std::string target_root;

    TargetMetaWriterConfig();
    TargetMetaWriterConfig(std::size_t worker_count, std::string target_root);
};

struct TargetDataWriterConfig {
    std::size_t worker_count = 1;
    std::string target_root;
    bool verify_hash = false;
    std::size_t async_window = 1;
    bool preserve_metadata = true;
    bool fsync_on_finish = true;
    bool ensure_parent_directories = true;
    bool stable_small_file_writes = false;
    bool tcp_cork_small_file_writes = false;
    bool direct_reactor_writes = false;
    bool direct_reactor_submit = false;
    std::size_t reactors_per_ip = 1;
    std::size_t reactor_count = 0;
    std::size_t max_concurrent_file_transactions = 64;

    TargetDataWriterConfig();
    TargetDataWriterConfig(std::size_t worker_count,
                           std::string target_root,
                           bool verify_hash = false,
                           std::size_t async_window = 1,
                           bool preserve_metadata = true,
                           bool fsync_on_finish = true,
                           bool ensure_parent_directories = true,
                           bool stable_small_file_writes = false,
                           bool tcp_cork_small_file_writes = false,
                           std::size_t reactors_per_ip = 1,
                           std::size_t max_concurrent_file_transactions = 64);
};

[[nodiscard]] TargetMetaWriterConfig load_target_meta_writer_config(const ConfigStore& config);
[[nodiscard]] TargetDataWriterConfig load_target_data_writer_config(const ConfigStore& config);
[[nodiscard]] std::size_t target_data_writer_effective_worker_count(const TargetDataWriterConfig& config);

struct TargetWriterStats {
    bool running = false;
    std::size_t worker_count = 0;
    std::uint64_t buffers_processed = 0;
    std::uint64_t files_written = 0;
    std::uint64_t files_failed = 0;
    std::uint64_t folders_written = 0;
    std::uint64_t bytes_written = 0;
};

// Consumes flat-folder metadata buffers and creates/applies target directory
// metadata through the selected target backend (NFS, local FS, or future FS).
class TargetMetaWriterJob final : public ThreadedJob {
public:
    TargetMetaWriterJob(TargetMetaWriterConfig config,
                        RawBufferPool& metadata_pool,
                        BufQueue& input);
    TargetMetaWriterJob(TargetMetaWriterConfig config,
                        RawBufferPool& metadata_pool,
                        ShardedBufQueue& input);
    ~TargetMetaWriterJob() override;

    [[nodiscard]] TargetWriterStats stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;

private:
    [[nodiscard]] bool pop_input(std::size_t worker_index, BufferHandle& handle);
    void process_buffer(TargetWriterBackend& backend, const BufferHandle& handle);
    void record_folder_written();

    TargetMetaWriterConfig config_;
    RawBufferPool& metadata_pool_;
    BufQueue* input_ = nullptr;
    ShardedBufQueue* sharded_input_ = nullptr;
    std::atomic<std::uint64_t> buffers_processed_ {0};
    std::atomic<std::uint64_t> folders_written_ {0};
};

// Consumes DataBuffer handles and writes file payloads to the selected target
// backend. Use the sharded constructor for multi-worker large-file writes so
// every file_id remains owned by exactly one writer lane.
class TargetDataWriterJob final : public ThreadedJob {
public:
    TargetDataWriterJob(TargetDataWriterConfig config,
                        RawBufferPool& data_pool,
                        BufQueue& input);
    TargetDataWriterJob(TargetDataWriterConfig config,
                        RawBufferPool& data_pool,
                        ShardedBufQueue& input);
    ~TargetDataWriterJob() override;

    [[nodiscard]] TargetWriterStats stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;

private:
    [[nodiscard]] bool pop_input(std::size_t worker_index, BufferHandle& handle);
    void process_buffer(TargetWriterBackend& backend, const BufferHandle& handle);
    void process_regular_batch(TargetWriterBackend& backend, const std::vector<BufferHandle>& handles);
    void process_packed_small_file_batch(TargetWriterBackend& backend, const std::vector<BufferHandle>& handles);
    void write_regular_buffer(TargetWriterBackend& backend, const DataBuffer& buffer);
    void write_packed_small_files(TargetWriterBackend& backend, const DataBuffer& buffer);
    static FileSpec file_spec_from_trailer(const DataBufTrailer& trailer);

    void record_buffer(std::uint64_t bytes);
    void record_file_written();
    void record_file_failed();

    TargetDataWriterConfig config_;
    RawBufferPool& data_pool_;
    BufQueue* input_ = nullptr;
    ShardedBufQueue* sharded_input_ = nullptr;
    std::atomic<std::uint64_t> buffers_processed_ {0};
    std::atomic<std::uint64_t> files_written_ {0};
    std::atomic<std::uint64_t> files_failed_ {0};
    std::atomic<std::uint64_t> bytes_written_ {0};
};

}  // namespace hypersync

#endif
