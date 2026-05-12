#include "jobs/data_cacher/data_cacher.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>

#include "common/config.hpp"

namespace hypersync {

namespace {

void write_u32(std::ostream& output, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        output.put(static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xFFU));
    }
}

void write_u64(std::ostream& output, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        output.put(static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xFFU));
    }
}

std::uint32_t read_u32(std::istream& input) {
    unsigned char bytes[4] {};
    input.read(reinterpret_cast<char*>(bytes), 4);
    if (!input) {
        throw std::runtime_error("failed to read cached u32");
    }
    return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
           (static_cast<std::uint32_t>(bytes[1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[2]) << 8U) |
           static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t read_u64(std::istream& input) {
    unsigned char bytes[8] {};
    input.read(reinterpret_cast<char*>(bytes), 8);
    if (!input) {
        throw std::runtime_error("failed to read cached u64");
    }
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8U) | static_cast<std::uint64_t>(bytes[i]);
    }
    return value;
}

std::uint64_t serialized_size(const DataBufTrailer& trailer, std::uint64_t data_bytes) {
    return 8ULL * 8ULL + 4ULL * 6ULL + static_cast<std::uint64_t>(trailer.rel_path.size()) + data_bytes;
}

std::string to_hex_path(std::uint64_t entry_id) {
    std::ostringstream buffer;
    buffer << std::hex << std::setfill('0') << std::setw(16) << entry_id;
    return buffer.str();
}

}  // namespace

namespace {

EndpointRole parse_endpoint_role(const std::string& value) {
    if (value == "sender") {
        return EndpointRole::sender;
    }
    if (value == "receiver") {
        return EndpointRole::receiver;
    }
    throw std::runtime_error("invalid endpoint role in config: " + value);
}

}  // namespace

DataCacherConfig::DataCacherConfig()
    : DataCacherConfig(load_data_cacher_config(ConfigStore{})) {}

DataCacherConfig::DataCacherConfig(EndpointRole role,
                                   double spill_threshold_percent,
                                   double drain_threshold_percent,
                                   std::string cache_path,
                                   std::size_t drive_count,
                                   std::size_t queue_depth_per_drive)
    : role(role),
      spill_threshold_percent(spill_threshold_percent),
      drain_threshold_percent(drain_threshold_percent),
      cache_path(std::move(cache_path)),
      drive_count(drive_count),
      queue_depth_per_drive(queue_depth_per_drive) {}

DataCacherConfig load_data_cacher_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("data_cacher"));
    return DataCacherConfig(parse_endpoint_role(config_string(values, "role")),
                            config_double(values, "spill_threshold_percent"),
                            config_double(values, "drain_threshold_percent"),
                            config_string(values, "cache_path"),
                            config_size_t(values, "drive_count"),
                            config_size_t(values, "queue_depth_per_drive"));
}

DataCacher::DataCacher(DataCacherConfig config)
    : TypedQueueJob("data_cacher", message_kinds::data_chunk), config_(std::move(config)) {}

void DataCacher::cache_chunk(DataChunk chunk) {
    if (config_.cache_path.empty()) {
        throw std::runtime_error("cache path must not be empty");
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (chunk.entry_id == 0 || entries_.find(chunk.entry_id) != entries_.end()) {
            while (entries_.find(next_entry_id_) != entries_.end()) {
                ++next_entry_id_;
            }
            chunk.entry_id = next_entry_id_++;
        }
    }

    chunk.cached = true;
    const std::filesystem::path path = cache_path_for(chunk.entry_id);
    std::filesystem::create_directories(path.parent_path());

    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("failed to open cache file for write: " + path.string());
    }
    write_u64(output, chunk.entry_id);
    write_u64(output, chunk.trailer.file_id);
    write_u64(output, chunk.trailer.folder_hash);
    write_u64(output, chunk.trailer.data_offset);
    write_u64(output, chunk.trailer.data_len);
    write_u64(output, chunk.trailer.file_size);
    write_u64(output, chunk.trailer.data_hash);
    write_u64(output, chunk.trailer.mtime);
    write_u32(output, chunk.trailer.mode);
    write_u32(output, chunk.trailer.uid);
    write_u32(output, chunk.trailer.gid);
    write_u32(output, chunk.trailer.chunk_hash);
    write_u32(output, chunk.trailer.flags);
    write_u32(output, static_cast<std::uint32_t>(chunk.trailer.rel_path.size()));
    output.write(chunk.trailer.rel_path.data(), static_cast<std::streamsize>(chunk.trailer.rel_path.size()));
    output.write(chunk.data.data(), static_cast<std::streamsize>(chunk.data.size()));
    if (!output) {
        throw std::runtime_error("failed to write cache file: " + path.string());
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& progress = manifests_[chunk.trailer.file_id];
        ++progress.completed_chunks;
        if ((chunk.trailer.flags & kFlagLastChunk) != 0U && progress.total_chunks < progress.completed_chunks) {
            progress.total_chunks = progress.completed_chunks;
        }
        entries_by_file_[chunk.trailer.file_id].push_back(chunk.entry_id);
        const std::uint64_t bytes_on_disk = serialized_size(chunk.trailer,
                                                            static_cast<std::uint64_t>(chunk.data.size()));
        entries_[chunk.entry_id] = CacheEntry{path, chunk.trailer.file_id, bytes_on_disk};
        cached_bytes_ += bytes_on_disk;
    }
    publish_item(std::move(chunk));
}

void DataCacher::cache_slot(const DataSlotPool& pool, const DataSlotHandle& handle) {
    if (config_.cache_path.empty()) {
        throw std::runtime_error("cache path must not be empty");
    }

    DataSlotHandle cached_handle = handle;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (cached_handle.entry_id == 0 || entries_.find(cached_handle.entry_id) != entries_.end()) {
            while (entries_.find(next_entry_id_) != entries_.end()) {
                ++next_entry_id_;
            }
            cached_handle.entry_id = next_entry_id_++;
        }
    }
    cached_handle.cached = true;

    const DataBufTrailer& trailer = pool.trailer(handle);
    const std::filesystem::path path = cache_path_for(cached_handle.entry_id);
    std::filesystem::create_directories(path.parent_path());

    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("failed to open cache file for write: " + path.string());
    }
    write_u64(output, cached_handle.entry_id);
    write_u64(output, trailer.file_id);
    write_u64(output, trailer.folder_hash);
    write_u64(output, trailer.data_offset);
    write_u64(output, trailer.data_len);
    write_u64(output, trailer.file_size);
    write_u64(output, trailer.data_hash);
    write_u64(output, trailer.mtime);
    write_u32(output, trailer.mode);
    write_u32(output, trailer.uid);
    write_u32(output, trailer.gid);
    write_u32(output, trailer.chunk_hash);
    write_u32(output, trailer.flags);
    write_u32(output, static_cast<std::uint32_t>(trailer.rel_path.size()));
    output.write(trailer.rel_path.data(), static_cast<std::streamsize>(trailer.rel_path.size()));
    output.write(pool.data(handle), static_cast<std::streamsize>(trailer.data_len));
    if (!output) {
        throw std::runtime_error("failed to write cache file: " + path.string());
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& progress = manifests_[trailer.file_id];
        ++progress.completed_chunks;
        if ((trailer.flags & kFlagLastChunk) != 0U && progress.total_chunks < progress.completed_chunks) {
            progress.total_chunks = progress.completed_chunks;
        }
        entries_by_file_[trailer.file_id].push_back(cached_handle.entry_id);
        const std::uint64_t bytes_on_disk = serialized_size(trailer, trailer.data_len);
        entries_[cached_handle.entry_id] = CacheEntry{path, trailer.file_id, bytes_on_disk};
        cached_bytes_ += bytes_on_disk;
    }
}

bool DataCacher::should_spill(double ram_usage_percent) const {
    return ram_usage_percent >= config_.spill_threshold_percent;
}

bool DataCacher::should_drain(double ram_usage_percent) const {
    return ram_usage_percent <= config_.drain_threshold_percent;
}

ChunkProgress DataCacher::manifest_for(std::uint64_t file_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = manifests_.find(file_id);
    if (it == manifests_.end()) {
        return {};
    }
    return it->second;
}

std::filesystem::path DataCacher::cache_path_for(std::uint64_t entry_id) const {
    const std::string hex = to_hex_path(entry_id);
    return std::filesystem::path(config_.cache_path) / hex.substr(0, 2) / hex.substr(2, 2) / hex;
}

std::vector<std::uint64_t> DataCacher::cached_entries_for(std::uint64_t file_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = entries_by_file_.find(file_id);
    if (it == entries_by_file_.end()) {
        return {};
    }
    return it->second;
}

void DataCacher::set_cached_file_hash(std::uint64_t file_id, std::uint64_t data_hash) {
    std::vector<std::filesystem::path> paths;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto ids_it = entries_by_file_.find(file_id);
        if (ids_it == entries_by_file_.end()) {
            return;
        }
        paths.reserve(ids_it->second.size());
        for (const std::uint64_t entry_id : ids_it->second) {
            const auto entry_it = entries_.find(entry_id);
            if (entry_it != entries_.end()) {
                paths.push_back(entry_it->second.path);
            }
        }
    }

    for (const auto& path : paths) {
        std::fstream cache_file(path, std::ios::binary | std::ios::in | std::ios::out);
        if (!cache_file) {
            throw std::runtime_error("failed to open cache file for hash patch: " + path.string());
        }
        cache_file.seekp(static_cast<std::streamoff>(8U * 6U), std::ios::beg);
        write_u64(cache_file, data_hash);
        if (!cache_file) {
            throw std::runtime_error("failed to patch cache file hash: " + path.string());
        }
    }
}

DataChunk DataCacher::take_chunk(std::uint64_t entry_id) {
    CacheEntry entry;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto entry_it = entries_.find(entry_id);
        if (entry_it == entries_.end()) {
            throw std::runtime_error("unknown cached entry id");
        }
        entry = entry_it->second;
        cached_bytes_ -= entry.bytes_on_disk;
        entries_.erase(entry_it);
        auto file_it = entries_by_file_.find(entry.file_id);
        if (file_it != entries_by_file_.end()) {
            auto& ids = file_it->second;
            ids.erase(std::remove(ids.begin(), ids.end(), entry_id), ids.end());
            if (ids.empty()) {
                entries_by_file_.erase(file_it);
            }
        }
    }

    std::ifstream input(entry.path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open cache file for read: " + entry.path.string());
    }

    DataChunk chunk;
    chunk.entry_id = read_u64(input);
    chunk.trailer.file_id = read_u64(input);
    chunk.trailer.folder_hash = read_u64(input);
    chunk.trailer.data_offset = read_u64(input);
    chunk.trailer.data_len = read_u64(input);
    chunk.trailer.file_size = read_u64(input);
    chunk.trailer.data_hash = read_u64(input);
    chunk.trailer.mtime = read_u64(input);
    chunk.trailer.mode = read_u32(input);
    chunk.trailer.uid = read_u32(input);
    chunk.trailer.gid = read_u32(input);
    chunk.trailer.chunk_hash = read_u32(input);
    chunk.trailer.flags = read_u32(input);
    const std::uint32_t path_length = read_u32(input);
    chunk.trailer.rel_path.resize_for_overwrite(path_length);
    input.read(chunk.trailer.rel_path.data(), static_cast<std::streamsize>(path_length));
    chunk.data.assign(static_cast<std::size_t>(chunk.trailer.data_len), '\0');
    input.read(chunk.data.data(), static_cast<std::streamsize>(chunk.data.size()));
    if (!input) {
        throw std::runtime_error("failed to read cache payload: " + entry.path.string());
    }
    chunk.cached = true;

    std::filesystem::remove(entry.path);
    return chunk;
}

DataSlotHandle DataCacher::take_slot(std::uint64_t entry_id, DataSlotPool& pool) {
    CacheEntry entry;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto entry_it = entries_.find(entry_id);
        if (entry_it == entries_.end()) {
            throw std::runtime_error("unknown cached entry id");
        }
        entry = entry_it->second;
        cached_bytes_ -= entry.bytes_on_disk;
        entries_.erase(entry_it);
        auto file_it = entries_by_file_.find(entry.file_id);
        if (file_it != entries_by_file_.end()) {
            auto& ids = file_it->second;
            ids.erase(std::remove(ids.begin(), ids.end(), entry_id), ids.end());
            if (ids.empty()) {
                entries_by_file_.erase(file_it);
            }
        }
    }

    std::ifstream input(entry.path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open cache file for read: " + entry.path.string());
    }

    const std::uint64_t stored_entry_id = read_u64(input);
    const std::uint64_t file_id = read_u64(input);
    const std::uint64_t folder_hash = read_u64(input);
    const std::uint64_t data_offset = read_u64(input);
    const std::uint64_t data_len = read_u64(input);
    const std::uint64_t file_size = read_u64(input);
    const std::uint64_t data_hash = read_u64(input);
    const std::uint64_t mtime = read_u64(input);
    const std::uint32_t mode = read_u32(input);
    const std::uint32_t uid = read_u32(input);
    const std::uint32_t gid = read_u32(input);
    const std::uint32_t chunk_hash = read_u32(input);
    const std::uint32_t flags = read_u32(input);
    const std::uint32_t path_length = read_u32(input);

    const DataSlotClass slot_class = (flags & kFlagSmallFile) != 0U ? DataSlotClass::small : DataSlotClass::large;
    DataSlotHandle handle = pool.acquire_or_throw(slot_class, static_cast<std::size_t>(data_len));
    handle.entry_id = stored_entry_id;
    handle.cached = true;

    DataBufTrailer& trailer = pool.trailer(handle);
    trailer.file_id = file_id;
    trailer.folder_hash = folder_hash;
    trailer.data_offset = data_offset;
    trailer.data_len = data_len;
    trailer.file_size = file_size;
    trailer.data_hash = data_hash;
    trailer.mtime = mtime;
    trailer.mode = mode;
    trailer.uid = uid;
    trailer.gid = gid;
    trailer.chunk_hash = chunk_hash;
    trailer.flags = flags;
    trailer.rel_path.resize_for_overwrite(path_length);
    trailer.slot_valid = kSlotValid;

    input.read(trailer.rel_path.data(), static_cast<std::streamsize>(path_length));
    input.read(pool.data(handle), static_cast<std::streamsize>(data_len));
    if (!input) {
        pool.release(handle);
        throw std::runtime_error("failed to read cache payload: " + entry.path.string());
    }

    std::filesystem::remove(entry.path);
    return handle;
}

std::uint64_t DataCacher::cached_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cached_bytes_;
}

double DataCacher::cache_usage_percent(std::uint64_t capacity_bytes) const {
    if (capacity_bytes == 0) {
        throw std::invalid_argument("capacity bytes must be positive");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const long double usage = static_cast<long double>(cached_bytes_) * 100.0L /
                              static_cast<long double>(capacity_bytes);
    return static_cast<double>(usage);
}

const DataCacherConfig& DataCacher::config() const {
    return config_;
}

}  // namespace hypersync
