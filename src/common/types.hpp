#ifndef HYPERSYNC_COMMON_TYPES_HPP
#define HYPERSYNC_COMMON_TYPES_HPP

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/fixed_string.hpp"

namespace hypersync {

class ConfigStore;

constexpr std::size_t kSmallFileThreshold = 128 * 1024;
constexpr std::size_t kLargeChunkBytes = 1024 * 1024;
constexpr std::uint32_t kSlotValid = 0xDEADBEEFU;
constexpr std::uint32_t kFlagLastChunk = 1U << 0;
constexpr std::uint32_t kFlagSmallFile = 1U << 1;
constexpr std::uint32_t kFlagHashValid = 1U << 2;
constexpr std::uint32_t kFlagPackedSmallFiles = 1U << 3;
constexpr std::size_t kRecBufNameBytes = 128;
constexpr std::size_t kRecBufRelPathBytes = 256;
constexpr std::size_t kDataBufRelPathBytes = 3552;

enum class Mode {
    transfer,
    scan,
    dry_run,
};

enum class EndpointRole {
    sender,
    receiver,
};

enum class FileState {
    pending,
    checking,
    skipped,
    reading,
    transferring,
    receiving,
    writing,
    failed,
    done,
};

enum class FolderState {
    pending,
    reading,
    transferring,
    awaiting_ack,
    receiving,
    writing,
    done,
};

enum class DiffKind {
    skip,
    new_file,
    changed,
    target_only,
    failed,
};

struct DataBufTrailer {
    std::uint64_t file_id = 0;
    std::uint64_t folder_hash = 0;
    std::uint64_t data_offset = 0;
    std::uint64_t data_len = 0;
    std::uint64_t file_size = 0;
    std::uint64_t data_hash = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint32_t chunk_hash = 0;
    std::uint32_t flags = 0;
    FixedString<kDataBufRelPathBytes> rel_path;
    std::uint32_t slot_valid = kSlotValid;
    std::uint32_t reserved = 0;
};

struct RecBuf {
    std::uint64_t own_hash = 0;
    std::uint64_t parent_hash = 0;
    std::uint64_t folder_hash = 0;
    FixedString<kRecBufNameBytes> name;
    FixedString<kRecBufRelPathBytes> rel_path;
    std::uint64_t size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint64_t inode = 0;
    std::uint64_t data_hash = 0;
    std::uint64_t hash_ts = 0;
    bool need_check = true;
    bool need_data = true;
    bool hash_verified = false;
    std::uint64_t bytes_sent = 0;
    std::uint64_t bytes_acked = 0;
    FileState state = FileState::pending;
};

struct FolderRecord {
    std::uint64_t md_hash = 0;
    std::uint64_t parent_hash = 0;
    std::string rel_path;
    bool recursive = true;
    bool need_check = true;
    std::uint64_t files_discovered = 0;
    std::uint64_t files_total = 0;
    std::uint64_t files_completed = 0;
    std::uint64_t files_skipped = 0;
    std::uint64_t flat_size_bytes = 0;
    std::uint64_t bytes_transferred = 0;
    std::uint64_t folder_data_hash = 0;
    std::uint64_t last_scan_ts = 0;
    std::uint64_t files_received = 0;
    std::uint64_t files_written = 0;
    std::uint64_t reading_offset = 0;
    std::uint8_t priority = 0;
    std::uint64_t created_ts = 0;
    std::uint64_t started_ts = 0;
    std::uint64_t completed_ts = 0;
    std::uint64_t state_ts = 0;
    FolderState state = FolderState::pending;
    FolderState remote_state = FolderState::pending;
};

struct FileSpec {
    FileSpec() = default;
    FileSpec(std::string rel_path,
             std::string content,
             std::uint64_t mtime = 0,
             std::uint32_t mode = 0644,
             std::uint32_t uid = 0,
             std::uint32_t gid = 0,
             bool need_check = true,
             std::uint64_t declared_size = 0,
             std::string hash_algorithm = {},
             std::string content_hash = {},
             std::uint64_t hash_block_size = 0,
             std::uint64_t hash_block_count = 0,
             std::string block_hash_algorithm = {},
             std::string block_hashes = {})
        : rel_path(std::move(rel_path)),
          content(std::move(content)),
          mtime(mtime),
          mode(mode),
          uid(uid),
          gid(gid),
          need_check(need_check),
          declared_size(declared_size),
          hash_algorithm(std::move(hash_algorithm)),
          content_hash(std::move(content_hash)),
          hash_block_size(hash_block_size),
          hash_block_count(hash_block_count),
          block_hash_algorithm(std::move(block_hash_algorithm)),
          block_hashes(std::move(block_hashes)) {}

    std::string rel_path;
    std::string content;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0644;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    bool need_check = true;
    std::uint64_t declared_size = 0;
    std::string hash_algorithm;
    std::string content_hash;
    std::uint64_t hash_block_size = 0;
    std::uint64_t hash_block_count = 0;
    std::string block_hash_algorithm;
    std::string block_hashes;
    // Opaque NFSv3 file handle captured from READDIRPLUS. It is only
    // interpreted by the libnfs backend; other jobs treat it as metadata.
    std::vector<std::uint8_t> nfs_handle;
};

struct FileSnapshot {
    std::uint64_t folder_hash = 0;
    std::uint64_t file_hash = 0;
    std::string rel_path;
    std::uint64_t size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint64_t data_hash = 0;
    std::uint64_t hash_ts = 0;
    char scan_side = 'S';
};

struct DataChunk {
    std::uint64_t entry_id = 0;
    std::string data;
    DataBufTrailer trailer;
    bool cached = false;
};

struct ChunkProgress {
    std::size_t total_chunks = 0;
    std::size_t completed_chunks = 0;
};

struct WatermarkStatus {
    bool soft_throttle = false;
    bool spill_to_nvme = false;
    bool hard_stop = false;
    bool prefetch_to_ram = false;
    bool pause_reader = false;
};

struct FileOutcome {
    std::string rel_path;
    DiffKind diff = DiffKind::new_file;
    FileState sender_state = FileState::pending;
    FileState receiver_state = FileState::pending;
    std::uint64_t size = 0;
    std::uint64_t data_hash = 0;
    std::size_t attempts = 0;
    std::size_t chunk_count = 0;
    bool hash_verified = false;
};

struct TransferReport {
    Mode mode = Mode::transfer;
    std::size_t files_total = 0;
    std::size_t folders_total = 0;
    std::size_t files_transferred = 0;
    std::size_t files_skipped = 0;
    std::size_t files_changed = 0;
    std::size_t files_new = 0;
    std::size_t files_target_only = 0;
    std::size_t files_failed = 0;
    std::size_t retries = 0;
    std::size_t chunks_sent = 0;
    std::uint64_t bytes_planned = 0;
    std::uint64_t bytes_transferred = 0;
    std::string diff_csv;
    std::vector<FileSnapshot> source_scan_rows;
    std::vector<FileSnapshot> target_scan_rows;
    std::map<std::string, FolderRecord> folders;
    std::map<std::string, FileOutcome> files;
};

struct EngineConfig {
    Mode mode;
    std::size_t small_file_threshold;
    std::size_t large_chunk_bytes;
    std::size_t small_pool_slots;
    std::size_t large_pool_slots;
    std::size_t max_retries;
    bool skip_verify;
    std::unordered_map<std::string, std::size_t> forced_failures;

    EngineConfig();
    EngineConfig(Mode mode,
                 std::size_t small_file_threshold,
                 std::size_t large_chunk_bytes,
                 std::size_t small_pool_slots,
                 std::size_t large_pool_slots,
                 std::size_t max_retries,
                 bool skip_verify,
                 std::unordered_map<std::string, std::size_t> forced_failures = {});
};

[[nodiscard]] EngineConfig load_engine_config(const ConfigStore& config);
std::string to_string(Mode mode);
std::string to_string(EndpointRole role);
std::string to_string(FileState state);
std::string to_string(FolderState state);
std::string to_string(DiffKind diff);

}  // namespace hypersync

#endif
