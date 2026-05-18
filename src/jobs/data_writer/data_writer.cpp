#include "jobs/data_writer/data_writer.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "common/config.hpp"
#include "common/path_utils.hpp"
#include "core/data_buffer_codec.hpp"
#include "core/flat_folder_buffer_codec.hpp"
#include "core/nfs_backend.hpp"
#include "core/pipeline_buffers.hpp"

namespace hypersync {
namespace {

[[nodiscard]] std::string backend_job_suffix(std::string_view root) {
    if (is_nfs_url(root)) {
        return "NFS";
    }
    if (root.rfind("synthetic-profile://", 0) == 0) {
        return "SYN";
    }
    return "FS";
}

[[nodiscard]] std::string child_path(std::string_view folder, std::string_view child) {
    if (folder.empty()) {
        return std::string(child);
    }
    std::string path;
    path.reserve(folder.size() + 1U + child.size());
    path.append(folder);
    path.push_back('/');
    path.append(child);
    return path;
}

}  // namespace

DataWriterConfig::DataWriterConfig()
    : DataWriterConfig(load_data_writer_config(ConfigStore{})) {}

DataWriterConfig::DataWriterConfig(std::size_t handle_cache_limit, bool verify_hash)
    : handle_cache_limit(handle_cache_limit),
      verify_hash(verify_hash) {}

DataWriterConfig load_data_writer_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("data_writer"));
    return DataWriterConfig(config_size_t(values, "handle_cache_limit"),
                            config_bool(values, "verify_hash"));
}

DataWriter::DataWriter(DataWriterConfig config)
    : TypedQueueJob("data_writer", message_kinds::data_chunk), config_(std::move(config)) {}

void DataWriter::predeclare_directory(std::string path) {
    dir_cache_.insert(normalize_path(path));
}

bool DataWriter::known_directory(std::string_view path) const {
    return dir_cache_.find(normalize_path(path)) != dir_cache_.end();
}

void DataWriter::queue_chunk(DataChunk chunk) {
    const std::string dir = parent_path(chunk.trailer.rel_path);
    if (!known_directory(dir)) {
        predeclare_directory(dir);
    }

    auto& progress = progress_[chunk.trailer.file_id];
    ++progress.completed_chunks;
    if ((chunk.trailer.flags & kFlagLastChunk) != 0U && progress.total_chunks < progress.completed_chunks) {
        progress.total_chunks = progress.completed_chunks;
    }
    if (progress.total_chunks != 0 && progress.total_chunks == progress.completed_chunks &&
        completed_file_ids_.insert(chunk.trailer.file_id).second) {
        ++completed_files_;
    }

    publish_item(std::move(chunk));
}

ChunkProgress DataWriter::progress_for(std::uint64_t file_id) const {
    const auto it = progress_.find(file_id);
    if (it == progress_.end()) {
        return {};
    }
    return it->second;
}

std::size_t DataWriter::completed_files() const {
    return completed_files_;
}

const DataWriterConfig& DataWriter::config() const {
    return config_;
}

TargetMetaWriterConfig::TargetMetaWriterConfig()
    : TargetMetaWriterConfig(load_target_meta_writer_config(ConfigStore{})) {}

TargetMetaWriterConfig::TargetMetaWriterConfig(std::size_t worker_count, std::string target_root)
    : worker_count(std::max<std::size_t>(1U, worker_count)),
      target_root(std::move(target_root)) {}

TargetDataWriterConfig::TargetDataWriterConfig()
    : TargetDataWriterConfig(load_target_data_writer_config(ConfigStore{})) {}

TargetDataWriterConfig::TargetDataWriterConfig(std::size_t worker_count,
                                               std::string target_root,
                                               bool verify_hash,
                                               std::size_t async_window)
    : worker_count(std::max<std::size_t>(1U, worker_count)),
      target_root(std::move(target_root)),
      verify_hash(verify_hash),
      async_window(std::max<std::size_t>(1U, async_window)) {}

TargetMetaWriterConfig load_target_meta_writer_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("target_meta_writer"));
    return TargetMetaWriterConfig(config_size_t_or(values, "worker_count", 1U),
                                  config_string_or(values, "target_root", "."));
}

TargetDataWriterConfig load_target_data_writer_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("target_data_writer"));
    return TargetDataWriterConfig(config_size_t_or(values, "worker_count", 1U),
                                  config_string_or(values, "target_root", "."),
                                  config_bool_or(values, "verify_hash", false),
                                  config_size_t_or(values, "async_window", 1U));
}

TargetMetaWriterJob::TargetMetaWriterJob(TargetMetaWriterConfig config,
                                         RawBufferPool& metadata_pool,
                                         BufQueue& input)
    : ThreadedJob(config.worker_count),
      config_(std::move(config)),
      metadata_pool_(metadata_pool),
      input_(&input) {
    if (metadata_pool_.pool_id() != kMetadataBatchBufferPoolId) {
        throw std::invalid_argument("MetaWriter-" + backend_job_suffix(config_.target_root) +
                                    " requires metadata batch buffers");
    }
}

TargetMetaWriterJob::TargetMetaWriterJob(TargetMetaWriterConfig config,
                                         RawBufferPool& metadata_pool,
                                         ShardedBufQueue& input)
    : ThreadedJob(config.worker_count),
      config_(std::move(config)),
      metadata_pool_(metadata_pool),
      sharded_input_(&input) {
    if (metadata_pool_.pool_id() != kMetadataBatchBufferPoolId) {
        throw std::invalid_argument("MetaWriter-" + backend_job_suffix(config_.target_root) +
                                    " requires metadata batch buffers");
    }
    if (config_.worker_count < input.shard_count()) {
        throw std::invalid_argument("MetaWriter-" + backend_job_suffix(config_.target_root) +
                                    " needs at least one worker per input shard");
    }
}

TargetMetaWriterJob::~TargetMetaWriterJob() {
    stop();
}

TargetWriterStats TargetMetaWriterJob::stats() const {
    TargetWriterStats snapshot;
    snapshot.running = running();
    snapshot.worker_count = worker_count();
    snapshot.buffers_processed = buffers_processed_.load(std::memory_order_acquire);
    snapshot.folders_written = folders_written_.load(std::memory_order_acquire);
    return snapshot;
}

void TargetMetaWriterJob::run_worker(std::size_t worker_index) {
    auto backend = make_target_writer_backend(config_.target_root);
    BufferHandle handle;
    while (!stop_requested() && pop_input(worker_index, handle)) {
        try {
            process_buffer(*backend, handle);
        } catch (...) {
            metadata_pool_.release(handle);
            throw;
        }
        metadata_pool_.release(handle);
    }
}

void TargetMetaWriterJob::on_stop_requested() {
    if (input_ != nullptr) {
        input_->close();
    }
    if (sharded_input_ != nullptr) {
        sharded_input_->close();
    }
}

bool TargetMetaWriterJob::pop_input(std::size_t worker_index, BufferHandle& handle) {
    if (input_ != nullptr) {
        return wait_for_input(worker_index, *input_, handle);
    }
    if (sharded_input_ == nullptr) {
        return false;
    }
    const std::size_t shard = sharded_input_->shard_count() == 0U ? 0U : worker_index % sharded_input_->shard_count();
    if (sharded_input_->shard(shard).try_pop(handle)) {
        return true;
    }
    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_input_empty);
    return sharded_input_->shard(shard).pop_wait(handle);
}

void TargetMetaWriterJob::process_buffer(TargetWriterBackend& backend, const BufferHandle& handle) {
    const MetadataBatchBuffer& buffer = metadata_batch_buffer(metadata_pool_, handle);
    if (!is_flat_folder_buffer(buffer)) {
        throw std::runtime_error("MetaWriter-" + backend_job_suffix(config_.target_root) +
                                 " received non-flat-folder metadata buffer");
    }
    const FlatFolderBufferInfo info = flat_folder_buffer_info(buffer);
    if (info.failed) {
        buffers_processed_.fetch_add(1U, std::memory_order_relaxed);
        return;
    }

    visit_flat_folder_children(buffer, [&](FlatFolderChildView child) {
        if (child.is_file) {
            return;
        }
        FileSpec folder;
        folder.rel_path = child_path(info.folder_path, child.name);
        folder.mtime = child.mtime;
        folder.mode = child.mode != 0U ? child.mode : 0755U;
        folder.uid = child.uid;
        folder.gid = child.gid;
        backend.ensure_directory(folder);
        record_folder_written();
    });

    if (info.final_batch) {
        FileSpec folder;
        folder.rel_path = std::string(info.folder_path);
        folder.mtime = info.folder_mtime;
        folder.mode = info.folder_mode != 0U ? info.folder_mode : 0755U;
        folder.uid = info.folder_uid;
        folder.gid = info.folder_gid;
        backend.apply_directory_metadata(folder);
        record_folder_written();
    }
    buffers_processed_.fetch_add(1U, std::memory_order_relaxed);
}

void TargetMetaWriterJob::record_folder_written() {
    folders_written_.fetch_add(1U, std::memory_order_relaxed);
}

TargetDataWriterJob::TargetDataWriterJob(TargetDataWriterConfig config,
                                         RawBufferPool& data_pool,
                                         BufQueue& input)
    : ThreadedJob(config.worker_count),
      config_(std::move(config)),
      data_pool_(data_pool),
      input_(&input) {
    if (data_pool_.pool_id() != kDataBufferPoolId) {
        throw std::invalid_argument("DataWriter-" + backend_job_suffix(config_.target_root) +
                                    " requires data buffers");
    }
    if (config_.worker_count > 1U) {
        throw std::invalid_argument("DataWriter-" + backend_job_suffix(config_.target_root) +
                                    " needs a sharded input queue for multi-worker writes");
    }
}

TargetDataWriterJob::TargetDataWriterJob(TargetDataWriterConfig config,
                                         RawBufferPool& data_pool,
                                         ShardedBufQueue& input)
    : ThreadedJob(config.worker_count),
      config_(std::move(config)),
      data_pool_(data_pool),
      sharded_input_(&input) {
    if (data_pool_.pool_id() != kDataBufferPoolId) {
        throw std::invalid_argument("DataWriter-" + backend_job_suffix(config_.target_root) +
                                    " requires data buffers");
    }
    if (config_.worker_count < input.shard_count()) {
        throw std::invalid_argument("DataWriter-" + backend_job_suffix(config_.target_root) +
                                    " needs at least one worker per input shard");
    }
}

TargetDataWriterJob::~TargetDataWriterJob() {
    stop();
}

TargetWriterStats TargetDataWriterJob::stats() const {
    TargetWriterStats snapshot;
    snapshot.running = running();
    snapshot.worker_count = worker_count();
    snapshot.buffers_processed = buffers_processed_.load(std::memory_order_acquire);
    snapshot.files_written = files_written_.load(std::memory_order_acquire);
    snapshot.files_failed = files_failed_.load(std::memory_order_acquire);
    snapshot.bytes_written = bytes_written_.load(std::memory_order_acquire);
    return snapshot;
}

void TargetDataWriterJob::run_worker(std::size_t worker_index) {
    auto backend = make_target_writer_backend(config_.target_root);
    BufferHandle handle;
    while (!stop_requested() && pop_input(worker_index, handle)) {
        std::vector<BufferHandle> batch;
        try {
            if (config_.async_window > 1U &&
                sharded_input_ != nullptr &&
                !is_packed_small_file_buffer(data_buffer(data_pool_, handle))) {
                batch.push_back(handle);
                const std::size_t shard =
                    sharded_input_->shard_count() == 0U ? 0U : worker_index % sharded_input_->shard_count();
                while (batch.size() < config_.async_window) {
                    BufferHandle next;
                    if (!sharded_input_->try_pop(shard, next)) {
                        break;
                    }
                    if (is_packed_small_file_buffer(data_buffer(data_pool_, next))) {
                        sharded_input_->push_wait(shard, next);
                        break;
                    }
                    batch.push_back(next);
                }
                process_regular_batch(*backend, batch);
            } else {
                process_buffer(*backend, handle);
            }
        } catch (...) {
            if (batch.empty()) {
                data_pool_.release(handle);
            } else {
                for (const BufferHandle& batched : batch) {
                    data_pool_.release(batched);
                }
            }
            throw;
        }
        if (batch.empty()) {
            data_pool_.release(handle);
        } else {
            for (const BufferHandle& batched : batch) {
                data_pool_.release(batched);
            }
        }
    }
}

void TargetDataWriterJob::on_stop_requested() {
    if (input_ != nullptr) {
        input_->close();
    }
    if (sharded_input_ != nullptr) {
        sharded_input_->close();
    }
}

bool TargetDataWriterJob::pop_input(std::size_t worker_index, BufferHandle& handle) {
    if (input_ != nullptr) {
        return wait_for_input(worker_index, *input_, handle);
    }
    if (sharded_input_ == nullptr) {
        return false;
    }
    const std::size_t shard = sharded_input_->shard_count() == 0U ? 0U : worker_index % sharded_input_->shard_count();
    if (sharded_input_->shard(shard).try_pop(handle)) {
        return true;
    }
    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_input_empty);
    return sharded_input_->shard(shard).pop_wait(handle);
}

void TargetDataWriterJob::process_buffer(TargetWriterBackend& backend, const BufferHandle& handle) {
    const DataBuffer& buffer = data_buffer(data_pool_, handle);
    if (is_packed_small_file_buffer(buffer)) {
        write_packed_small_files(backend, buffer);
        record_buffer(0);
    } else {
        write_regular_buffer(backend, buffer);
        record_buffer(buffer.trailer.data_len);
    }
}

void TargetDataWriterJob::process_regular_batch(TargetWriterBackend& backend,
                                                const std::vector<BufferHandle>& handles) {
    std::vector<TargetWriterBackend::WriteChunk> chunks;
    chunks.reserve(handles.size());
    for (const BufferHandle& handle : handles) {
        const DataBuffer& buffer = data_buffer(data_pool_, handle);
        if (is_packed_small_file_buffer(buffer)) {
            throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                     " cannot batch packed-small-file buffers");
        }
        const std::size_t data_len = static_cast<std::size_t>(buffer.trailer.data_len);
        if (data_len > buffer.bytes.size()) {
            record_file_failed();
            throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                     " received oversized data buffer");
        }
        TargetWriterBackend::WriteChunk chunk;
        chunk.spec = file_spec_from_trailer(buffer.trailer);
        chunk.data = std::string_view(reinterpret_cast<const char*>(buffer.bytes.data()), data_len);
        chunk.offset = buffer.trailer.data_offset;
        chunk.last_chunk = (buffer.trailer.flags & kFlagLastChunk) != 0U;
        chunks.push_back(std::move(chunk));
    }

    backend.write_chunks(chunks);
    for (const TargetWriterBackend::WriteChunk& chunk : chunks) {
        record_buffer(chunk.data.size());
        if (chunk.last_chunk) {
            record_file_written();
        }
    }
}

void TargetDataWriterJob::write_regular_buffer(TargetWriterBackend& backend, const DataBuffer& buffer) {
    const FileSpec file = file_spec_from_trailer(buffer.trailer);
    const std::size_t data_len = static_cast<std::size_t>(buffer.trailer.data_len);
    if (data_len > buffer.bytes.size()) {
        record_file_failed();
        throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                 " received oversized data buffer");
    }
    const auto* bytes = reinterpret_cast<const char*>(buffer.bytes.data());
    backend.write_chunk(file, std::string_view(bytes, data_len), buffer.trailer.data_offset);
    if ((buffer.trailer.flags & kFlagLastChunk) != 0U) {
        backend.finish_file(file);
        record_file_written();
    }
}

void TargetDataWriterJob::write_packed_small_files(TargetWriterBackend& backend, const DataBuffer& buffer) {
    std::uint64_t payload_bytes = 0;
    const bool ok = visit_packed_small_files(buffer, [&](PackedSmallFileView view) {
        FileSpec file;
        file.rel_path = std::string(view.rel_path);
        file.declared_size = view.file_size;
        file.mtime = view.mtime;
        file.mode = view.mode != 0U ? view.mode : 0644U;
        file.uid = view.uid;
        file.gid = view.gid;
        backend.write_chunk(file, view.data, 0);
        backend.finish_file(file);
        payload_bytes += view.data.size();
        record_file_written();
    });
    if (!ok) {
        record_file_failed();
        throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                 " received malformed packed-small-file buffer");
    }
    bytes_written_.fetch_add(payload_bytes, std::memory_order_relaxed);
}

FileSpec TargetDataWriterJob::file_spec_from_trailer(const DataBufTrailer& trailer) {
    FileSpec file;
    file.rel_path = std::string(trailer.rel_path.view());
    file.declared_size = trailer.file_size;
    file.mtime = trailer.mtime;
    file.mode = trailer.mode != 0U ? trailer.mode : 0644U;
    file.uid = trailer.uid;
    file.gid = trailer.gid;
    return file;
}

void TargetDataWriterJob::record_buffer(std::uint64_t bytes) {
    buffers_processed_.fetch_add(1U, std::memory_order_relaxed);
    if (bytes != 0U) {
        bytes_written_.fetch_add(bytes, std::memory_order_relaxed);
    }
}

void TargetDataWriterJob::record_file_written() {
    files_written_.fetch_add(1U, std::memory_order_relaxed);
}

void TargetDataWriterJob::record_file_failed() {
    files_failed_.fetch_add(1U, std::memory_order_relaxed);
}

}  // namespace hypersync
