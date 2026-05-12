#include "jobs/scan_writer/scan_writer.hpp"

#include "common/config.hpp"
#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"

namespace hypersync {

ScanWriterConfig::ScanWriterConfig()
    : ScanWriterConfig(load_scan_writer_config(ConfigStore{})) {}

ScanWriterConfig::ScanWriterConfig(char scan_side, bool flush_per_file)
    : scan_side(scan_side),
      flush_per_file(flush_per_file) {}

ScanWriterConfig load_scan_writer_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("scan_writer"));
    return ScanWriterConfig(config_char(values, "scan_side"),
                            config_bool(values, "flush_per_file"));
}

ScanWriter::ScanWriter(ScanWriterConfig config)
    : TypedQueueJob("scan_writer", message_kinds::file_snapshot), config_(std::move(config)) {}

std::optional<FileSnapshot> ScanWriter::snapshot_from_chunk(const DataChunk& chunk) const {
    if ((chunk.trailer.flags & kFlagLastChunk) == 0U || (chunk.trailer.flags & kFlagHashValid) == 0U) {
        return std::nullopt;
    }

    FileSnapshot snapshot;
    snapshot.folder_hash = chunk.trailer.folder_hash;
    snapshot.file_hash = path_hash(base_name(chunk.trailer.rel_path), folder_hash_for_path(parent_path(chunk.trailer.rel_path)));
    snapshot.rel_path = normalize_path(chunk.trailer.rel_path);
    snapshot.size = chunk.trailer.file_size;
    snapshot.mtime = chunk.trailer.mtime;
    snapshot.mode = chunk.trailer.mode;
    snapshot.uid = chunk.trailer.uid;
    snapshot.gid = chunk.trailer.gid;
    snapshot.data_hash = chunk.trailer.data_hash;
    snapshot.hash_ts = 0;
    snapshot.scan_side = config_.scan_side;
    return snapshot;
}

void ScanWriter::record_chunk(const DataChunk& chunk) {
    const auto snapshot = snapshot_from_chunk(chunk);
    if (!snapshot.has_value()) {
        record_deferred();
        return;
    }
    ++rows_written_;
    publish_item(*snapshot);
}

std::size_t ScanWriter::rows_written() const {
    return rows_written_;
}

const ScanWriterConfig& ScanWriter::config() const {
    return config_;
}

}  // namespace hypersync
