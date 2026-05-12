#include "jobs/metadata_record_writer_job/metadata_record_writer_job.hpp"

#include <vector>

#include "core/metadata_buffer_codec.hpp"
#include "core/pipeline_buffers.hpp"

namespace hypersync {

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
        decode_metadata_batch(metadata_batch_buffer(pool, handle), files, folders);
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
