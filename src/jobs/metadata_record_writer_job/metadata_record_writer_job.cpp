#include "jobs/metadata_record_writer_job/metadata_record_writer_job.hpp"

#include <vector>

#include "core/flat_folder_buffer_codec.hpp"
#include "core/metadata_buffer_codec.hpp"
#include "core/pipeline_buffers.hpp"

namespace hypersync {
namespace {

std::string child_path_for_flat_folder_writer(std::string_view folder_path, std::string_view child_name) {
    if (folder_path.empty()) {
        return std::string(child_name);
    }
    std::string path;
    path.reserve(folder_path.size() + 1U + child_name.size());
    path.append(folder_path);
    path.push_back('/');
    path.append(child_name);
    return path;
}

void decode_flat_folder_metadata_batch(const MetadataBatchBuffer& buffer,
                                       std::vector<FileSpec>& files,
                                       std::vector<MetadataFolderRecord>& folders) {
    const FlatFolderBufferInfo info = flat_folder_buffer_info(buffer);
    files.reserve(info.child_record_count);
    visit_flat_folder_children(buffer, [&](FlatFolderChildView child) {
        if (!child.is_file) {
            return;
        }
        FileSpec file;
        file.rel_path = child_path_for_flat_folder_writer(info.folder_path, child.name);
        file.declared_size = child.logical_size;
        file.mtime = child.mtime;
        file.mode = child.mode;
        file.uid = child.uid;
        file.gid = child.gid;
        files.push_back(std::move(file));
    });

    if (info.final_batch) {
        MetadataFolderRecord folder;
        folder.spec.rel_path = std::string(info.folder_path);
        folder.spec.mtime = info.folder_mtime;
        folder.spec.mode = info.folder_mode;
        folder.spec.uid = info.folder_uid;
        folder.spec.gid = info.folder_gid;
        folder.flat_file_count = static_cast<std::size_t>(info.total_file_count);
        folder.flat_logical_size_bytes = info.total_logical_size_bytes;
        folders.push_back(std::move(folder));
    }
}

}  // namespace

MetadataRecordWriterJob::MetadataRecordWriterJob(std::size_t worker_count,
                                                 BufQueue& input,
                                                 const BufferPoolRegistry& registry,
                                                 MetadataRecordWriterConfig writer_config)
    : BufferConsumerJob(worker_count, input, registry),
      writer_(std::move(writer_config)) {}

MetadataRecordWriterJob::~MetadataRecordWriterJob() = default;

MetadataRecordWriterJobStats MetadataRecordWriterJob::writer_stats() const {
    return {writer_.files_written(), writer_.folders_written()};
}

void MetadataRecordWriterJob::process_buffer(const BufferHandle& handle, RawBufferPool& pool) {
    std::vector<FileSpec> files;
    std::vector<MetadataFolderRecord> folders;

    if (handle.pool_id == kMetadataBatchBufferPoolId) {
        const MetadataBatchBuffer& buffer = metadata_batch_buffer(pool, handle);
        if (is_flat_folder_buffer(buffer)) {
            decode_flat_folder_metadata_batch(buffer, files, folders);
        } else {
            decode_metadata_batch(buffer, files, folders);
        }
    } else {
        const MetadataBuffer& buffer = metadata_buffer(pool, handle);
        if (buffer.record_kind == MetadataBufferRecordKind::file) {
            files.push_back(decode_metadata_file_record(buffer));
        } else if (buffer.record_kind == MetadataBufferRecordKind::folder) {
            folders.push_back(decode_metadata_folder_record(buffer));
        }
    }

    if (!files.empty() || !folders.empty()) {
        writer_.write_batch(files, folders);
    }
    pool.release(handle);
}

void MetadataRecordWriterJob::on_all_workers_finished() {
    writer_.close();
}

}  // namespace hypersync
