#ifndef HYPERSYNC_JOBS_NFS_META_READER_HPP
#define HYPERSYNC_JOBS_NFS_META_READER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/types.hpp"
#include "jobs/queue_job.hpp"

namespace hypersync {

class ConfigStore;
class NfsBackend;

struct NfsMetaReaderConfig {
    std::size_t worker_count;
    std::size_t thread_count;
    std::size_t async_directory_depth;
    std::size_t readdirplus_page_bytes;
    std::size_t recbuf_window;
    std::size_t streaming_mode_threshold;
    bool async_readdir;
    std::string source_root;
    bool recursive;

    NfsMetaReaderConfig();
    NfsMetaReaderConfig(std::size_t worker_count,
                        std::size_t recbuf_window,
                        std::size_t streaming_mode_threshold,
                        bool async_readdir,
                        std::string source_root,
                        bool recursive,
                        std::size_t async_directory_depth = 256U,
                        std::size_t readdirplus_page_bytes = 256U * 1024U);
};

[[nodiscard]] NfsMetaReaderConfig load_nfs_meta_reader_config(const ConfigStore& config);

class NfsMetaReader : public TypedQueueJob<RecBuf> {
public:
    explicit NfsMetaReader(NfsMetaReaderConfig config = {});
    ~NfsMetaReader();

    [[nodiscard]] std::vector<FileSpec> scan_tree() const;
    void publish_tree();
    void stream_tree_to(Job& downstream);
    [[nodiscard]] bool using_async_backend() const;

    void begin_folder(FolderRecord folder);
    void publish_record(RecBuf record);
    void discover_child_folder(FolderRecord child);
    void finish_folder(std::uint64_t files_total);

    [[nodiscard]] const NfsMetaReaderConfig& config() const;
    [[nodiscard]] std::optional<FolderRecord> active_folder() const;
    [[nodiscard]] std::size_t files_seen() const;
    [[nodiscard]] std::size_t folders_completed() const;
    [[nodiscard]] std::vector<FolderRecord> take_discovered_folders();

private:
    [[nodiscard]] NfsBackend& backend() const;

    NfsMetaReaderConfig config_;
    mutable std::unique_ptr<NfsBackend> backend_;
    std::optional<FolderRecord> active_folder_;
    std::vector<FolderRecord> discovered_children_;
    std::size_t files_seen_ = 0;
    std::size_t folders_completed_ = 0;
};

}  // namespace hypersync

#endif
