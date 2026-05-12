#include "jobs/nfs_meta_reader/nfs_meta_reader.hpp"

#include "common/config.hpp"
#include "core/nfs_backend.hpp"
#include "common/records.hpp"

namespace hypersync {

NfsMetaReaderConfig::NfsMetaReaderConfig()
    : NfsMetaReaderConfig(load_nfs_meta_reader_config(ConfigStore{})) {}

NfsMetaReaderConfig::NfsMetaReaderConfig(std::size_t worker_count,
                                         std::size_t recbuf_window,
                                         std::size_t streaming_mode_threshold,
                                         bool async_readdir,
                                         std::string source_root,
                                         bool recursive,
                                         std::size_t async_directory_depth)
    : worker_count(worker_count),
      thread_count(worker_count),
      async_directory_depth(async_directory_depth),
      recbuf_window(recbuf_window),
      streaming_mode_threshold(streaming_mode_threshold),
      async_readdir(async_readdir),
      source_root(std::move(source_root)),
      recursive(recursive) {}

NfsMetaReaderConfig load_nfs_meta_reader_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("nfs_meta_reader"));
    const std::size_t worker_count =
        config_size_t_or(values, "worker_count", config_size_t_or(values, "thread_count", 8));
    const std::size_t async_directory_depth = config_size_t_or(values, "async_directory_depth", 1);
    return NfsMetaReaderConfig(worker_count,
                               config_size_t(values, "recbuf_window"),
                               config_size_t(values, "streaming_mode_threshold"),
                               config_bool(values, "async_readdir"),
                               config_string(values, "source_root"),
                               config_bool(values, "recursive"),
                               async_directory_depth);
}

NfsMetaReader::NfsMetaReader(NfsMetaReaderConfig config)
    : TypedQueueJob("nfs_meta_reader", message_kinds::file_record),
      config_(std::move(config)),
      backend_(make_nfs_backend(config_.source_root)) {}

NfsMetaReader::~NfsMetaReader() = default;

std::vector<FileSpec> NfsMetaReader::scan_tree() const {
    return backend().list_files(config_.recursive);
}

void NfsMetaReader::publish_tree() {
    if (active_folder_.has_value()) {
        std::uint64_t files_total = 0;
        backend().visit_folder(
            active_folder_->rel_path,
            [this, &files_total](FileSpec spec) {
                ++files_total;
                publish_record(make_recbuf(spec));
            },
            [this](FileSpec spec) {
                FolderRecord child;
                child.rel_path = spec.rel_path;
                child.recursive = config_.recursive;
                discover_child_folder(std::move(child));
            });
        finish_folder(files_total);
        return;
    }

    backend().visit_files(config_.recursive, [this](FileSpec spec) {
        publish_record(make_recbuf(spec));
    });
}

void NfsMetaReader::stream_tree_to(Job& downstream) {
    backend().visit_metadata(
        config_.recursive,
        [this, &downstream](FileSpec spec) {
            ++files_seen_;
            downstream.push_back(JobMessage{message_kinds::file_record, make_recbuf(spec)});
        },
        [&downstream](FileSpec spec) {
            FolderRecord folder;
            folder.rel_path = spec.rel_path;
            downstream.push_back(JobMessage{message_kinds::folder_record, std::move(folder)});
        });
}

bool NfsMetaReader::using_async_backend() const {
    return backend().uses_async_api();
}

void NfsMetaReader::begin_folder(FolderRecord folder) {
    active_folder_ = std::move(folder);
}

void NfsMetaReader::publish_record(RecBuf record) {
    ++files_seen_;
    publish_item(std::move(record));
}

void NfsMetaReader::discover_child_folder(FolderRecord child) {
    discovered_children_.push_back(std::move(child));
}

void NfsMetaReader::finish_folder(std::uint64_t files_total) {
    if (active_folder_.has_value()) {
        active_folder_->files_total = files_total;
        active_folder_->reading_offset = files_total;
        ++folders_completed_;
    }
}

const NfsMetaReaderConfig& NfsMetaReader::config() const {
    return config_;
}

std::optional<FolderRecord> NfsMetaReader::active_folder() const {
    return active_folder_;
}

std::size_t NfsMetaReader::files_seen() const {
    return files_seen_;
}

std::size_t NfsMetaReader::folders_completed() const {
    return folders_completed_;
}

std::vector<FolderRecord> NfsMetaReader::take_discovered_folders() {
    std::vector<FolderRecord> result = std::move(discovered_children_);
    discovered_children_.clear();
    return result;
}

NfsBackend& NfsMetaReader::backend() const {
    return *backend_;
}

}  // namespace hypersync
