#ifndef HYPERSYNC_JOBS_METADATA_RECORD_WRITER_JOB_HPP
#define HYPERSYNC_JOBS_METADATA_RECORD_WRITER_JOB_HPP

// Metadata writer pipeline job.
//
// Consumes raw metadata buffers, decodes file/folder records, writes them
// through MetadataRecordWriter, and releases each buffer after durable handoff
// to the writer. The job does not know who produced the buffers.

#include "core/metadata_record_writer.hpp"
#include "jobs/buffer_consumer_job.hpp"

namespace hypersync {

struct MetadataRecordWriterJobStats {
    std::uint64_t files_written = 0;
    std::uint64_t folders_written = 0;
};

class MetadataRecordWriterJob : public BufferConsumerJob {
public:
    MetadataRecordWriterJob(std::size_t worker_count,
                            BufQueue& input,
                            const BufferPoolRegistry& registry,
                            MetadataRecordWriterConfig writer_config);
    ~MetadataRecordWriterJob() override;

    [[nodiscard]] MetadataRecordWriterJobStats writer_stats() const;

protected:
    void process_buffer(const BufferHandle& handle, RawBufferPool& pool) override;
    void on_all_workers_finished() override;

private:
    MetadataRecordWriter writer_;
};

}  // namespace hypersync

#endif
