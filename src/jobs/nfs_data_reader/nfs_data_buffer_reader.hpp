#ifndef HYPERSYNC_JOBS_NFS_DATA_BUFFER_READER_HPP
#define HYPERSYNC_JOBS_NFS_DATA_BUFFER_READER_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>

#include "common/buffer_pool.hpp"
#include "common/types.hpp"
#include "jobs/nfs_data_reader/nfs_data_reader.hpp"
#include "jobs/threaded_job.hpp"

namespace hypersync {

struct NfsDataBufferReaderStats {
    bool running = false;
    std::size_t worker_count = 0;
    std::uint64_t files_read = 0;
    std::uint64_t files_failed = 0;
    std::uint64_t buffers_read = 0;
    std::uint64_t bytes_read = 0;
};

// Reads file work from a provider and publishes filled DataBuffer handles.
//
// The metadata scanner is still being migrated, so the input side is a narrow
// FileSpec provider callback. The output side follows the new pipeline contract:
// this job owns raw buffers from a preallocated pool, fills them with NFS data,
// and transfers ownership through a BufQueue without allocating chunk storage.
class NfsDataBufferReaderJob : public ThreadedJob {
public:
    using FileProvider = std::function<std::optional<FileSpec>()>;
    using BytesReadCallback = std::function<void(std::uint64_t)>;
    using FileReadCallback = std::function<void()>;
    using FileFailedCallback = std::function<void(const FileSpec&)>;
    using StopPredicate = std::function<bool()>;

    NfsDataBufferReaderJob(NfsDataReaderConfig config,
                           RawBufferPool& data_pool,
                           BufQueue& output,
                           FileProvider file_provider,
                           StopPredicate stop_predicate = {});
    ~NfsDataBufferReaderJob() override;

    void set_bytes_read_callback(BytesReadCallback callback);
    void set_file_read_callback(FileReadCallback callback);
    void set_file_failed_callback(FileFailedCallback callback);

    [[nodiscard]] NfsDataBufferReaderStats stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;
    void on_all_workers_finished() override;

private:
    void publish_file_chunks(NfsDataReader& reader, const FileSpec& file, std::size_t worker_index);
    void complete_trailer(const RecBuf& record,
                          std::uint64_t logical_size,
                          DataBufTrailer& trailer,
                          std::uint64_t chunk_offset) const;
    [[nodiscard]] bool should_stop_now() const;
    void record_bytes_read(std::uint64_t bytes_read);
    void record_file_read();
    void record_files_read(std::uint64_t file_count);
    void record_file_failed(const FileSpec& file);
    void record_files_failed(std::uint64_t file_count);

    NfsDataReaderConfig config_;
    RawBufferPool& data_pool_;
    BufQueue& output_;
    FileProvider file_provider_;
    StopPredicate stop_predicate_;
    BytesReadCallback bytes_read_callback_;
    FileReadCallback file_read_callback_;
    FileFailedCallback file_failed_callback_;
    std::atomic<std::uint64_t> files_read_ {0};
    std::atomic<std::uint64_t> files_failed_ {0};
    std::atomic<std::uint64_t> buffers_read_ {0};
    std::atomic<std::uint64_t> bytes_read_ {0};
};

}  // namespace hypersync

#endif
