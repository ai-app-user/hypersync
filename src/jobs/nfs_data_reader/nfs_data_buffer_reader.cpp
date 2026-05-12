#include "jobs/nfs_data_reader/nfs_data_buffer_reader.hpp"

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "common/hash_utils.hpp"
#include "common/records.hpp"
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
      output_(output),
      file_provider_(std::move(file_provider)),
      stop_predicate_(std::move(stop_predicate)) {
    if (data_pool_.pool_id() != kDataBufferPoolId) {
        throw std::invalid_argument("nfs data buffer reader requires the data buffer pool");
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
    (void)worker_index;
    NfsDataReader reader(config_);
    while (!should_stop_now()) {
        const std::optional<FileSpec> file = file_provider_();
        if (!file.has_value()) {
            break;
        }

        try {
            publish_file_chunks(reader, *file);
            record_file_read();
        } catch (const NfsDataReaderStopped&) {
            break;
        } catch (...) {
            record_file_failed(*file);
        }
    }
}

void NfsDataBufferReaderJob::on_stop_requested() {
    output_.close();
}

void NfsDataBufferReaderJob::on_all_workers_finished() {
    output_.close();
}

void NfsDataBufferReaderJob::publish_file_chunks(NfsDataReader& reader, const FileSpec& file) {
    const RecBuf record = make_recbuf(file);
    const std::uint64_t logical_size = file_logical_size(file);

    const std::uint64_t bytes_streamed =
        reader.stream_file_raw_chunks(file,
                                      data_pool_,
                                      [&](RawFileChunk&& chunk) {
                                          DataBuffer& buffer = data_buffer(data_pool_, chunk.handle);
                                          const std::uint64_t bytes_read = buffer.trailer.data_len;
                                          complete_trailer(record, logical_size, buffer.trailer, chunk.offset);

                                          if (!output_.push_wait(chunk.handle)) {
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
    files_read_.fetch_add(1, std::memory_order_relaxed);
    if (file_read_callback_) {
        file_read_callback_();
    }
}

void NfsDataBufferReaderJob::record_file_failed(const FileSpec& file) {
    files_failed_.fetch_add(1, std::memory_order_relaxed);
    if (file_failed_callback_) {
        file_failed_callback_(file);
    }
}

}  // namespace hypersync
