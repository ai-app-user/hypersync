#include "common/records.hpp"

#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"

namespace hypersync {

RecBuf make_recbuf(const FileSpec& file) {
    RecBuf record;
    const std::string normalized = normalize_path(file.rel_path);
    record.name = base_name(normalized);
    record.rel_path = normalized;
    record.parent_hash = folder_hash_for_path(parent_path(normalized));
    record.folder_hash = record.parent_hash;
    record.own_hash = path_hash(record.name, record.parent_hash);
    record.size = file.declared_size != 0 ? file.declared_size : file.content.size();
    record.mtime = file.mtime;
    record.mode = file.mode;
    record.uid = file.uid;
    record.gid = file.gid;
    record.need_check = file.need_check;
    record.need_data = true;
    return record;
}

FileSnapshot make_snapshot(const FileSpec& file, std::uint64_t data_hash, char scan_side) {
    const RecBuf record = make_recbuf(file);
    FileSnapshot snapshot;
    snapshot.folder_hash = record.folder_hash;
    snapshot.file_hash = record.own_hash;
    snapshot.rel_path = record.rel_path;
    snapshot.size = record.size;
    snapshot.mtime = record.mtime;
    snapshot.mode = record.mode;
    snapshot.uid = record.uid;
    snapshot.gid = record.gid;
    snapshot.data_hash = data_hash;
    snapshot.hash_ts = record.mtime;
    snapshot.scan_side = scan_side;
    return snapshot;
}

}  // namespace hypersync
