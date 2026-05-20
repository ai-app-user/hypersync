#include "jobs/data_writer/data_writer.hpp"

#include <algorithm>
#include <stdexcept>
#include <thread>
#include <utility>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

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
    if (is_null_url(root)) {
        return "NULL";
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

[[nodiscard]] bool is_empty_regular_data_buffer(const DataBuffer& buffer) {
    return !is_packed_small_file_buffer(buffer) &&
           buffer.trailer.rel_path.view().empty() &&
           buffer.trailer.data_len == 0U;
}

void pin_current_thread_to_cpu(std::size_t cpu_index) noexcept {
#if defined(__linux__)
    const unsigned int cpu_count = std::thread::hardware_concurrency();
    if (cpu_count == 0U) {
        return;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<int>(cpu_index % cpu_count), &set);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
    (void)cpu_index;
#endif
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
                                               std::size_t async_window,
                                               bool preserve_metadata,
                                               bool fsync_on_finish,
                                               bool ensure_parent_directories,
                                               bool stable_small_file_writes,
                                               bool tcp_cork_small_file_writes,
                                               std::size_t reactors_per_ip,
                                               std::size_t max_concurrent_file_transactions)
    : worker_count(std::max<std::size_t>(1U, worker_count)),
      target_root(std::move(target_root)),
      verify_hash(verify_hash),
      async_window(std::max<std::size_t>(1U, async_window)),
      preserve_metadata(preserve_metadata),
      fsync_on_finish(fsync_on_finish),
      ensure_parent_directories(ensure_parent_directories),
      stable_small_file_writes(stable_small_file_writes),
      tcp_cork_small_file_writes(tcp_cork_small_file_writes),
      reactors_per_ip(std::max<std::size_t>(1U, reactors_per_ip)),
      max_concurrent_file_transactions(std::max<std::size_t>(1U, max_concurrent_file_transactions)) {}

TargetMetaWriterConfig load_target_meta_writer_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("target_meta_writer"));
    TargetMetaWriterConfig result(config_size_t_or(values, "worker_count", 1U),
                                  config_string_or(values, "target_root", "."));
    result.preserve_metadata = config_bool_or(values, "preserve_metadata", true);
    result.async_window = config_size_t_or(values, "async_window", 64U);
    return result;
}

TargetDataWriterConfig load_target_data_writer_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("target_data_writer"));
    TargetDataWriterConfig result(config_size_t_or(values, "worker_count", 1U),
                                  config_string_or(values, "target_root", "."),
                                  config_bool_or(values, "verify_hash", false),
                                  config_size_t_or(values, "async_window", 1U),
                                  config_bool_or(values, "preserve_metadata", true),
                                  config_bool_or(values, "fsync_on_finish", true),
                                  config_bool_or(values, "ensure_parent_directories", true),
                                  config_bool_or(values, "stable_small_file_writes", false),
                                  config_bool_or(values, "tcp_cork_small_file_writes", false),
                                  config_size_t_or(values, "reactors_per_ip", 1U),
                                  config_size_t_or(values, "max_concurrent_file_transactions", 64U));
    result.reactor_count = config_size_t_or(values, "reactor_count", 0U);
    result.direct_reactor_submit = config_bool_or(values, "direct_reactor_submit", false);
    return result;
}

std::size_t target_data_writer_effective_worker_count(const TargetDataWriterConfig& config) {
    const std::size_t configured = std::max<std::size_t>(1U, config.worker_count);
    if (config.direct_reactor_writes && is_nfs_url(config.target_root)) {
        return std::min<std::size_t>(configured, 16U);
    }
    return configured;
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
    TargetWriterBackend::Options options;
    options.preserve_metadata = config_.preserve_metadata;
    options.max_concurrent_file_transactions = std::max<std::size_t>(1U, config_.async_window);
    auto backend = make_target_writer_backend(config_.target_root, worker_index, options);
    BufferHandle handle;
    while (!stop_requested() && pop_input(worker_index, handle)) {
        std::vector<BufferHandle> handles;
        handles.reserve(std::max<std::size_t>(1U, config_.async_window));
        handles.push_back(handle);
        while (handles.size() < std::max<std::size_t>(1U, config_.async_window) &&
               try_pop_input(worker_index, handle)) {
            handles.push_back(handle);
        }
        try {
            process_buffers(*backend, handles);
        } catch (...) {
            for (const BufferHandle& pending : handles) {
                metadata_pool_.release(pending);
            }
            throw;
        }
        for (const BufferHandle& processed : handles) {
            metadata_pool_.release(processed);
        }
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

bool TargetMetaWriterJob::try_pop_input(std::size_t worker_index, BufferHandle& handle) {
    if (input_ != nullptr) {
        return input_->try_pop(handle);
    }
    if (sharded_input_ == nullptr) {
        return false;
    }
    const std::size_t shard = sharded_input_->shard_count() == 0U ? 0U : worker_index % sharded_input_->shard_count();
    return sharded_input_->shard(shard).try_pop(handle);
}

void TargetMetaWriterJob::process_buffers(TargetWriterBackend& backend,
                                          const std::vector<BufferHandle>& handles) {
    std::vector<FileSpec> folders;
    std::vector<FileSpec> final_folders;
    std::uint64_t processed = 0;

    for (const BufferHandle& handle : handles) {
        const MetadataBatchBuffer& buffer = metadata_batch_buffer(metadata_pool_, handle);
        if (!is_flat_folder_buffer(buffer)) {
            throw std::runtime_error("MetaWriter-" + backend_job_suffix(config_.target_root) +
                                     " received non-flat-folder metadata buffer");
        }
        const FlatFolderBufferInfo info = flat_folder_buffer_info(buffer);
        ++processed;
        if (info.failed) {
            continue;
        }

        folders.reserve(folders.size() + static_cast<std::size_t>(info.total_folder_count));
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
            folders.push_back(std::move(folder));
        });

        if (info.final_batch && config_.preserve_metadata) {
            FileSpec folder;
            folder.rel_path = std::string(info.folder_path);
            folder.mtime = info.folder_mtime;
            folder.mode = info.folder_mode != 0U ? info.folder_mode : 0755U;
            folder.uid = info.folder_uid;
            folder.gid = info.folder_gid;
            final_folders.push_back(std::move(folder));
        }
    }

    backend.ensure_directories(folders);
    if (!folders.empty()) {
        folders_written_.fetch_add(folders.size(), std::memory_order_relaxed);
    }
    for (const FileSpec& folder : final_folders) {
        backend.apply_directory_metadata(folder);
        record_folder_written();
    }
    buffers_processed_.fetch_add(processed, std::memory_order_relaxed);
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

    std::vector<FileSpec> folders;
    folders.reserve(static_cast<std::size_t>(info.total_folder_count));
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
        folders.push_back(std::move(folder));
    });
    backend.ensure_directories(folders);
    if (!folders.empty()) {
        folders_written_.fetch_add(folders.size(), std::memory_order_relaxed);
    }

    if (info.final_batch) {
        FileSpec folder;
        folder.rel_path = std::string(info.folder_path);
        folder.mtime = info.folder_mtime;
        folder.mode = info.folder_mode != 0U ? info.folder_mode : 0755U;
        folder.uid = info.folder_uid;
        folder.gid = info.folder_gid;
        if (config_.preserve_metadata) {
            backend.apply_directory_metadata(folder);
            record_folder_written();
        }
    }
    buffers_processed_.fetch_add(1U, std::memory_order_relaxed);
}

void TargetMetaWriterJob::record_folder_written() {
    folders_written_.fetch_add(1U, std::memory_order_relaxed);
}

TargetDataWriterJob::TargetDataWriterJob(TargetDataWriterConfig config,
                                         RawBufferPool& data_pool,
                                         BufQueue& input)
    : ThreadedJob(target_data_writer_effective_worker_count(config)),
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
    : ThreadedJob(target_data_writer_effective_worker_count(config)),
      config_(std::move(config)),
      data_pool_(data_pool),
      sharded_input_(&input) {
    if (data_pool_.pool_id() != kDataBufferPoolId) {
        throw std::invalid_argument("DataWriter-" + backend_job_suffix(config_.target_root) +
                                    " requires data buffers");
    }
    if (worker_count() < input.shard_count()) {
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
    TargetWriterBackend::Options options;
    options.preserve_metadata = config_.preserve_metadata;
    options.fsync_on_finish = config_.fsync_on_finish;
    options.ensure_parent_directories = config_.ensure_parent_directories;
    options.stable_small_file_writes = config_.stable_small_file_writes;
    options.tcp_cork_small_file_writes = config_.tcp_cork_small_file_writes;
    options.direct_reactor_lane = config_.direct_reactor_writes && is_nfs_url(config_.target_root);
    options.reactors_per_ip = std::max<std::size_t>(1U, config_.reactors_per_ip);
    options.reactor_count = config_.reactor_count;
    options.max_concurrent_file_transactions = config_.max_concurrent_file_transactions;
    if (options.direct_reactor_lane) {
        pin_current_thread_to_cpu(worker_index);
    }
    auto backend = make_target_writer_backend(config_.target_root, worker_index, options);
    BufferHandle handle;
    while (!stop_requested() && pop_input(worker_index, handle)) {
        std::vector<BufferHandle> batch;
        try {
            const bool first_is_packed = is_packed_small_file_buffer(data_buffer(data_pool_, handle));
            if (config_.async_window > 1U && sharded_input_ != nullptr) {
                batch.push_back(handle);
                const std::size_t shard =
                    sharded_input_->shard_count() == 0U ? 0U : worker_index % sharded_input_->shard_count();
                while (batch.size() < config_.async_window) {
                    BufferHandle next;
                    if (!sharded_input_->try_pop(shard, next)) {
                        break;
                    }
                    const bool next_is_packed = is_packed_small_file_buffer(data_buffer(data_pool_, next));
                    if (next_is_packed != first_is_packed) {
                        sharded_input_->push_wait(shard, next);
                        break;
                    }
                    batch.push_back(next);
                }
                if (first_is_packed) {
                    process_packed_small_file_batch(*backend, batch);
                } else {
                    process_regular_batch(*backend, batch);
                }
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
        if (is_empty_regular_data_buffer(buffer)) {
            record_buffer(0);
            continue;
        }
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
        if (chunk.spec.rel_path.empty()) {
            record_file_failed();
            throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                     " received a regular data buffer without a relative path");
        }
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

void TargetDataWriterJob::process_packed_small_file_batch(TargetWriterBackend& backend,
                                                          const std::vector<BufferHandle>& handles) {
    std::vector<TargetWriterBackend::WriteChunk> files;
    std::uint64_t payload_bytes = 0;
    std::size_t malformed_buffers = 0;
    std::size_t buffers_processed = 0;

    for (const BufferHandle& handle : handles) {
        const DataBuffer& buffer = data_buffer(data_pool_, handle);
        if (!is_packed_small_file_buffer(buffer)) {
            throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                     " cannot mix regular and packed-small-file buffers");
        }
        files.reserve(files.size() + packed_small_file_count(buffer));
        const bool ok = visit_packed_small_files(buffer, [&](PackedSmallFileView view) {
            FileSpec file;
            file.rel_path = std::string(view.rel_path);
            file.declared_size = view.file_size;
            file.mtime = view.mtime;
            file.mode = view.mode != 0U ? view.mode : 0644U;
            file.uid = view.uid;
            file.gid = view.gid;
            TargetWriterBackend::WriteChunk chunk;
            chunk.spec = std::move(file);
            chunk.data = view.data;
            chunk.offset = 0;
            chunk.last_chunk = true;
            files.push_back(std::move(chunk));
            payload_bytes += view.data.size();
        });
        if (!ok) {
            ++malformed_buffers;
        }
        ++buffers_processed;
    }

    if (malformed_buffers != 0U) {
        record_file_failed();
        throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                 " received malformed packed-small-file buffer");
    }

    backend.write_files(files);
    files_written_.fetch_add(files.size(), std::memory_order_relaxed);
    bytes_written_.fetch_add(payload_bytes, std::memory_order_relaxed);
    buffers_processed_.fetch_add(buffers_processed, std::memory_order_relaxed);
}

void TargetDataWriterJob::write_regular_buffer(TargetWriterBackend& backend, const DataBuffer& buffer) {
    if (is_empty_regular_data_buffer(buffer)) {
        record_buffer(0);
        return;
    }
    const FileSpec file = file_spec_from_trailer(buffer.trailer);
    if (file.rel_path.empty()) {
        record_file_failed();
        throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                 " received a regular data buffer without a relative path");
    }
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
    std::vector<TargetWriterBackend::WriteChunk> files;
    files.reserve(packed_small_file_count(buffer));
    const bool ok = visit_packed_small_files(buffer, [&](PackedSmallFileView view) {
        FileSpec file;
        file.rel_path = std::string(view.rel_path);
        file.declared_size = view.file_size;
        file.mtime = view.mtime;
        file.mode = view.mode != 0U ? view.mode : 0644U;
        file.uid = view.uid;
        file.gid = view.gid;
        TargetWriterBackend::WriteChunk chunk;
        chunk.spec = std::move(file);
        chunk.data = view.data;
        chunk.offset = 0;
        chunk.last_chunk = true;
        files.push_back(std::move(chunk));
        payload_bytes += view.data.size();
    });
    if (!ok) {
        record_file_failed();
        throw std::runtime_error("DataWriter-" + backend_job_suffix(config_.target_root) +
                                 " received malformed packed-small-file buffer");
    }
    backend.write_files(files);
    for (std::size_t index = 0; index < files.size(); ++index) {
        record_file_written();
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
