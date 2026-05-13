#include "common/scan_index.hpp"

#include <algorithm>
#include <charconv>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"

namespace hypersync {

namespace {

std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> fields;
    std::string current;
    bool in_quotes = false;
    for (std::size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (in_quotes) {
            if (ch == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    current.push_back('"');
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                current.push_back(ch);
            }
        } else {
            if (ch == '"') {
                in_quotes = true;
            } else if (ch == ',') {
                fields.push_back(std::move(current));
                current.clear();
            } else {
                current.push_back(ch);
            }
        }
    }
    fields.push_back(std::move(current));
    return fields;
}

std::string csv_quote(const std::string& value) {
    if (value.find_first_of(",\"\r\n") == std::string::npos) {
        return value;
    }
    std::string result = "\"";
    for (char ch : value) {
        if (ch == '"') {
            result += "\"\"";
        } else {
            result += ch;
        }
    }
    result += '"';
    return result;
}

template <typename T>
T parse_integral(const std::string& text, const char* field_name) {
    T value{};
    const auto* begin = text.data();
    const auto* end = text.data() + text.size();
    const auto result = std::from_chars(begin, end, value);
    if (result.ec != std::errc{} || result.ptr != end) {
        throw std::invalid_argument(std::string("invalid numeric field: ") + field_name);
    }
    return value;
}

}  // namespace

void ScanIndex::add(FileSnapshot snapshot) {
    snapshot.rel_path = normalize_path(snapshot.rel_path);
    if (rows_by_path_.find(snapshot.rel_path) == rows_by_path_.end()) {
        folder_index_[parent_path(snapshot.rel_path)].push_back(snapshot.rel_path);
    }
    rows_by_path_[snapshot.rel_path] = std::move(snapshot);
}

bool ScanIndex::empty() const {
    return rows_by_path_.empty();
}

std::optional<FileSnapshot> ScanIndex::find(std::string_view rel_path) const {
    const auto it = rows_by_path_.find(normalize_path(rel_path));
    if (it == rows_by_path_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool ScanIndex::file_matches(const FileSnapshot& snapshot) const {
    const auto existing = find(snapshot.rel_path);
    if (!existing.has_value()) {
        return false;
    }
    return existing->file_hash == snapshot.file_hash &&
           existing->size == snapshot.size &&
           existing->mtime == snapshot.mtime &&
           existing->data_hash == snapshot.data_hash;
}

bool ScanIndex::metadata_matches(std::string_view rel_path, std::uint64_t size, std::uint64_t mtime) const {
    const auto existing = find(rel_path);
    if (!existing.has_value()) {
        return false;
    }
    return existing->size == size && existing->mtime == mtime;
}

std::uint64_t ScanIndex::folder_data_hash(std::string_view folder_path) const {
    const std::string normalized_folder = normalize_path(folder_path);
    const auto folder_it = folder_index_.find(normalized_folder);
    if (folder_it == folder_index_.end()) {
        return compute_folder_data_hash({});
    }
    std::vector<FileSnapshot> entries;
    entries.reserve(folder_it->second.size());
    for (const auto& path : folder_it->second) {
        const auto row_it = rows_by_path_.find(path);
        if (row_it != rows_by_path_.end()) {
            entries.push_back(row_it->second);
        }
    }
    return compute_folder_data_hash(entries);
}

std::vector<FileSnapshot> ScanIndex::rows() const {
    std::vector<FileSnapshot> entries;
    entries.reserve(rows_by_path_.size());
    for (const auto& [_, snapshot] : rows_by_path_) {
        entries.push_back(snapshot);
    }
    std::sort(entries.begin(), entries.end(), [](const FileSnapshot& lhs, const FileSnapshot& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });
    return entries;
}

std::string ScanIndex::to_csv() const {
    std::ostringstream out;
    out << "folder_hash,file_hash,rel_path,size,mtime,mode,uid,gid,data_hash,hash_ts,scan_side\n";
    for (const auto& snapshot : rows()) {
        out << snapshot.folder_hash << ','
            << snapshot.file_hash << ','
            << csv_quote(snapshot.rel_path) << ','
            << snapshot.size << ','
            << snapshot.mtime << ','
            << snapshot.mode << ','
            << snapshot.uid << ','
            << snapshot.gid << ','
            << snapshot.data_hash << ','
            << snapshot.hash_ts << ','
            << snapshot.scan_side << '\n';
    }
    return out.str();
}

ScanIndex ScanIndex::from_csv(std::string_view csv) {
    std::istringstream input{std::string(csv)};
    std::string line;
    bool saw_header = false;
    bool rich_scan_csv = false;
    ScanIndex index;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        if (!saw_header) {
            if (line == "folder_hash,file_hash,rel_path,size,mtime,mode,uid,gid,data_hash,hash_ts,scan_side") {
                rich_scan_csv = false;
            } else if (line == "record_type,rel_path,size,mtime,mode,uid,gid,flat_file_count,flat_logical_size_bytes,hash_algorithm,content_hash,hash_block_size,hash_block_count,block_hash_algorithm,block_hashes,scan_run_id,run_started_at_utc,run_started_unix_ns,source_root,run_settings") {
                rich_scan_csv = true;
            } else {
                throw std::invalid_argument("unexpected CSV header");
            }
            saw_header = true;
            continue;
        }
        const auto fields = split_csv_line(line);
        if (rich_scan_csv) {
            if (fields.size() != 20) {
                throw std::invalid_argument("unexpected CSV field count");
            }
            if (fields[0] != "file") {
                continue;
            }

            FileSnapshot snapshot;
            snapshot.rel_path = normalize_path(fields[1]);
            snapshot.folder_hash = folder_hash_for_path(parent_path(snapshot.rel_path));
            snapshot.file_hash = path_hash(base_name(snapshot.rel_path), snapshot.folder_hash);
            snapshot.size = parse_integral<std::uint64_t>(fields[2], "size");
            snapshot.mtime = parse_integral<std::uint64_t>(fields[3], "mtime");
            snapshot.mode = parse_integral<std::uint32_t>(fields[4], "mode");
            snapshot.uid = parse_integral<std::uint32_t>(fields[5], "uid");
            snapshot.gid = parse_integral<std::uint32_t>(fields[6], "gid");
            snapshot.hash_ts = fields[17].empty() ? 0U : parse_integral<std::uint64_t>(fields[17], "run_started_unix_ns");
            index.add(std::move(snapshot));
            continue;
        }
        if (fields.size() != 11) {
            throw std::invalid_argument("unexpected CSV field count");
        }

        FileSnapshot snapshot;
        snapshot.folder_hash = parse_integral<std::uint64_t>(fields[0], "folder_hash");
        snapshot.file_hash = parse_integral<std::uint64_t>(fields[1], "file_hash");
        snapshot.rel_path = normalize_path(fields[2]);
        snapshot.size = parse_integral<std::uint64_t>(fields[3], "size");
        snapshot.mtime = parse_integral<std::uint64_t>(fields[4], "mtime");
        snapshot.mode = parse_integral<std::uint32_t>(fields[5], "mode");
        snapshot.uid = parse_integral<std::uint32_t>(fields[6], "uid");
        snapshot.gid = parse_integral<std::uint32_t>(fields[7], "gid");
        snapshot.data_hash = parse_integral<std::uint64_t>(fields[8], "data_hash");
        snapshot.hash_ts = parse_integral<std::uint64_t>(fields[9], "hash_ts");
        if (fields[10].size() != 1U) {
            throw std::invalid_argument("invalid scan_side");
        }
        snapshot.scan_side = fields[10][0];
        index.add(std::move(snapshot));
    }

    if (!saw_header) {
        throw std::invalid_argument("missing CSV header");
    }
    return index;
}

}  // namespace hypersync
