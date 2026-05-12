#ifndef HYPERSYNC_CORE_METADATA_BUFFER_CODEC_HPP
#define HYPERSYNC_CORE_METADATA_BUFFER_CODEC_HPP

// Fixed-layout metadata-buffer codec used at pipeline and transport boundaries.
//
// The queue layer carries only raw BufferHandle values. These helpers interpret
// an owned MetadataBuffer payload as a compact file/folder record without
// allocating or depending on a concrete upstream or downstream job.

#include <string_view>
#include <vector>

#include "core/metadata_record_writer.hpp"
#include "core/pipeline_buffers.hpp"
#include "jobs/file_metadata_generator/file_metadata_generator.hpp"

namespace hypersync {

bool encode_metadata_file_record(MetadataBuffer& buffer, const FileSpec& file);
bool encode_metadata_folder_record(MetadataBuffer& buffer, const MetadataFolderRecord& folder);
[[nodiscard]] FileSpec decode_metadata_file_record(const MetadataBuffer& buffer);
[[nodiscard]] MetadataFolderRecord decode_metadata_folder_record(const MetadataBuffer& buffer);

void reset_metadata_batch(MetadataBatchBuffer& buffer);
bool append_metadata_batch_file(MetadataBatchBuffer& buffer, const FileSpec& file);
bool append_metadata_batch_folder(MetadataBatchBuffer& buffer, const MetadataFolderRecord& folder);
bool append_metadata_batch_file(MetadataBatchBuffer& buffer, const GeneratedFileMetadataView& file);
bool append_metadata_batch_folder(MetadataBatchBuffer& buffer, const GeneratedFolderMetadataView& folder);
void decode_metadata_batch(const MetadataBatchBuffer& buffer,
                           std::vector<FileSpec>& files,
                           std::vector<MetadataFolderRecord>& folders);

}  // namespace hypersync

#endif
