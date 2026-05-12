#include "core/nfs_backend.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>

#if defined(__APPLE__)
extern "C" off_t lseek(int, off_t, int);
extern "C" ssize_t write(int, const void*, size_t);
#endif

#include "common/filesystem_utils.hpp"
#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"
#include "common/socket_utils.hpp"
#include "core/pipeline_buffers.hpp"

#ifndef HYPERSYNC_HAS_LIBNFS
#define HYPERSYNC_HAS_LIBNFS 0
#endif

#if HYPERSYNC_HAS_LIBNFS
#if defined(__has_include)
#if __has_include(<nfsc/libnfs.h>)
#include <nfsc/libnfs.h>
#else
#include <libnfs.h>
#endif
#else
#include <nfsc/libnfs.h>
#endif
#endif

namespace hypersync {

namespace {

std::uint64_t stat_mtime_ns(const struct stat& info) {
#if defined(__APPLE__)
    return static_cast<std::uint64_t>(info.st_mtimespec.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(info.st_mtimespec.tv_nsec);
#else
    return static_cast<std::uint64_t>(info.st_mtim.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(info.st_mtim.tv_nsec);
#endif
}

[[maybe_unused]] std::uint64_t truncate_to_microseconds(std::uint64_t timestamp_ns) {
    return (timestamp_ns / 1000ULL) * 1000ULL;
}

[[maybe_unused]] std::size_t path_depth(std::string_view rel_path) {
    if (rel_path.empty()) {
        return 0;
    }
    return 1U + static_cast<std::size_t>(std::count(rel_path.begin(), rel_path.end(), '/'));
}

std::vector<std::string_view> split_string_view(std::string_view value, char separator) {
    std::vector<std::string_view> parts;
    std::size_t begin = 0;
    while (begin <= value.size()) {
        const std::size_t end = value.find(separator, begin);
        parts.push_back(value.substr(begin, end == std::string_view::npos ? std::string_view::npos : end - begin));
        if (end == std::string_view::npos) {
            break;
        }
        begin = end + 1U;
    }
    return parts;
}

std::string_view trim_view(std::string_view value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
        value.remove_prefix(1U);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
        value.remove_suffix(1U);
    }
    return value;
}

std::optional<std::array<std::uint8_t, 4>> parse_ipv4_address(std::string_view value) {
    std::array<std::uint8_t, 4> octets {};
    const std::vector<std::string_view> parts = split_string_view(value, '.');
    if (parts.size() != octets.size()) {
        return std::nullopt;
    }
    for (std::size_t index = 0; index < parts.size(); ++index) {
        if (parts[index].empty()) {
            return std::nullopt;
        }
        std::uint32_t octet = 0;
        for (char ch : parts[index]) {
            if (std::isdigit(static_cast<unsigned char>(ch)) == 0) {
                return std::nullopt;
            }
            octet = (octet * 10U) + static_cast<std::uint32_t>(ch - '0');
            if (octet > 255U) {
                return std::nullopt;
            }
        }
        octets[index] = static_cast<std::uint8_t>(octet);
    }
    return octets;
}

std::uint32_t ipv4_to_u32(const std::array<std::uint8_t, 4>& octets) {
    return (static_cast<std::uint32_t>(octets[0]) << 24U) |
           (static_cast<std::uint32_t>(octets[1]) << 16U) |
           (static_cast<std::uint32_t>(octets[2]) << 8U) |
           static_cast<std::uint32_t>(octets[3]);
}

std::string ipv4_to_string(std::uint32_t value) {
    std::ostringstream output;
    output << ((value >> 24U) & 0xffU) << '.'
           << ((value >> 16U) & 0xffU) << '.'
           << ((value >> 8U) & 0xffU) << '.'
           << (value & 0xffU);
    return output.str();
}

std::optional<std::uint8_t> parse_ipv4_last_octet(std::string_view value) {
    if (value.empty()) {
        return std::nullopt;
    }
    std::uint32_t octet = 0;
    for (char ch : value) {
        if (std::isdigit(static_cast<unsigned char>(ch)) == 0) {
            return std::nullopt;
        }
        octet = (octet * 10U) + static_cast<std::uint32_t>(ch - '0');
        if (octet > 255U) {
            return std::nullopt;
        }
    }
    return static_cast<std::uint8_t>(octet);
}

std::vector<std::string> expand_server_token(std::string_view token) {
    token = trim_view(token);
    if (token.empty()) {
        return {};
    }

    const std::size_t dash = token.find('-');
    if (dash == std::string_view::npos) {
        return {std::string(token)};
    }

    const std::string_view lhs = trim_view(token.substr(0, dash));
    const std::string_view rhs = trim_view(token.substr(dash + 1U));
    const std::optional<std::array<std::uint8_t, 4>> start_octets = parse_ipv4_address(lhs);
    if (!start_octets.has_value()) {
        return {std::string(token)};
    }

    std::optional<std::array<std::uint8_t, 4>> end_octets = parse_ipv4_address(rhs);
    if (!end_octets.has_value()) {
        const std::optional<std::uint8_t> last_octet = parse_ipv4_last_octet(rhs);
        if (!last_octet.has_value()) {
            return {std::string(token)};
        }
        end_octets = start_octets;
        (*end_octets)[3] = *last_octet;
    }

    const std::uint32_t start = ipv4_to_u32(*start_octets);
    const std::uint32_t end = ipv4_to_u32(*end_octets);
    if (end < start) {
        throw std::invalid_argument("NFS server IP range must be ascending: " + std::string(token));
    }
    constexpr std::uint32_t kMaxExpandedServers = 4096U;
    if ((end - start) + 1U > kMaxExpandedServers) {
        throw std::invalid_argument("NFS server IP range expands to too many endpoints: " + std::string(token));
    }

    std::vector<std::string> servers;
    servers.reserve(static_cast<std::size_t>((end - start) + 1U));
    for (std::uint32_t value = start; value <= end; ++value) {
        servers.push_back(ipv4_to_string(value));
        if (value == end) {
            break;
        }
    }
    return servers;
}

std::vector<std::string> expand_server_expression(std::string_view expression) {
    std::vector<std::string> servers;
    for (std::string_view token : split_string_view(expression, ',')) {
        std::vector<std::string> expanded = expand_server_token(token);
        servers.insert(servers.end(),
                       std::make_move_iterator(expanded.begin()),
                       std::make_move_iterator(expanded.end()));
    }
    if (servers.empty()) {
        servers.push_back(std::string(expression));
    }
    return servers;
}

std::string choose_random_string(const std::vector<std::string>& values) {
    if (values.empty()) {
        throw std::invalid_argument("cannot choose from an empty server list");
    }
    if (values.size() == 1U) {
        return values.front();
    }
    thread_local std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<std::size_t> distribution(0U, values.size() - 1U);
    return values[distribution(rng)];
}

[[maybe_unused]] std::string choose_nfs_connection_url(std::string_view root_url) {
    std::vector<std::string> candidates = expand_nfs_url_server_candidates(root_url);
    return choose_random_string(candidates);
}

void local_pwrite_all(int fd, std::string_view data, std::uint64_t offset) {
    std::size_t written_total = 0;
    while (written_total < data.size()) {
#if defined(__APPLE__)
        if (::lseek(fd, static_cast<off_t>(offset + written_total), SEEK_SET) < 0) {
            throw std::system_error(errno, std::generic_category(), "lseek failed");
        }
        const ssize_t written = ::write(fd, data.data() + written_total, data.size() - written_total);
#else
        const ssize_t written = ::pwrite(fd,
                                         data.data() + written_total,
                                         data.size() - written_total,
                                         static_cast<off_t>(offset + written_total));
#endif
        if (written < 0) {
            throw std::system_error(errno, std::generic_category(), "write failed");
        }
        written_total += static_cast<std::size_t>(written);
    }
}

FileSpec local_spec_from_absolute(const std::filesystem::path& root, const std::filesystem::path& absolute_path) {
    struct stat info {};
    if (::stat(absolute_path.c_str(), &info) != 0) {
        throw std::system_error(errno, std::generic_category(), "stat failed for " + absolute_path.string());
    }

    FileSpec spec;
    spec.rel_path = normalize_path(std::filesystem::relative(absolute_path, root).generic_string());
    spec.mtime = stat_mtime_ns(info);
    spec.mode = static_cast<std::uint32_t>(info.st_mode & 0777U);
    spec.uid = static_cast<std::uint32_t>(info.st_uid);
    spec.gid = static_cast<std::uint32_t>(info.st_gid);
    spec.declared_size = static_cast<std::uint64_t>(info.st_size);
    return spec;
}

class LocalFilesystemBackend final : public NfsBackend {
public:
    explicit LocalFilesystemBackend(std::string root) : root_(std::move(root)) {}

    [[nodiscard]] std::vector<FileSpec> list_files(bool recursive) const override {
        return collect_file_specs(root_, recursive);
    }

    [[nodiscard]] std::vector<FileSpec> list_directories(bool recursive) const override {
        return collect_directory_specs(root_, recursive);
    }

    void visit_metadata_at(std::string_view rel_path,
                           bool recursive,
                           const std::function<void(FileSpec)>& file_visitor,
                           const std::function<void(FileSpec)>& directory_visitor) const override {
        const std::filesystem::path absolute_root = root_ / normalize_path(rel_path);
        const auto emit_entry = [&](const std::filesystem::path& path) {
            struct stat info {};
            if (::stat(path.c_str(), &info) != 0) {
                throw std::system_error(errno, std::generic_category(), "stat failed for " + path.string());
            }
            if (S_ISDIR(info.st_mode)) {
                directory_visitor(local_spec_from_absolute(root_, path));
                return;
            }
            if (S_ISREG(info.st_mode)) {
                file_visitor(local_spec_from_absolute(root_, path));
            }
        };

        if (recursive) {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(absolute_root)) {
                emit_entry(entry.path());
            }
            return;
        }
        for (const auto& entry : std::filesystem::directory_iterator(absolute_root)) {
            emit_entry(entry.path());
        }
    }

    void visit_folder(std::string_view rel_path,
                      const std::function<void(FileSpec)>& file_visitor,
                      const std::function<void(FileSpec)>& directory_visitor) const override {
        visit_metadata_at(rel_path, false, file_visitor, directory_visitor);
    }

    [[nodiscard]] FileSpec load_file(std::string_view rel_path) const override {
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        FileSpec spec = local_spec_from_absolute(root_, absolute_path);
        spec.content = read_file_contents(absolute_path);
        if (spec.declared_size == 0) {
            spec.declared_size = spec.content.size();
        }
        return spec;
    }

    [[nodiscard]] std::uint64_t read_file_discard(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t)>& bytes_visitor) const override {
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::vector<char> buffer(1024U * 1024U);
        std::uint64_t total = 0;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                const auto bytes_read = static_cast<std::uint64_t>(bytes);
                total += bytes_read;
                if (bytes_visitor) {
                    bytes_visitor(bytes_read);
                }
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        if (declared_size != 0 && total < declared_size) {
            throw std::runtime_error("unexpected EOF while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_stream(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::string_view)>& data_visitor) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::vector<char> buffer(1024U * 1024U);
        std::uint64_t total = 0;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                const auto bytes_read = static_cast<std::uint64_t>(bytes);
                total += bytes_read;
                if (data_visitor) {
                    data_visitor(std::string_view(buffer.data(), static_cast<std::size_t>(bytes)));
                }
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_owned_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(OwnedFileChunk&&)>& data_visitor) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::uint64_t total = 0;
        while (input) {
            OwnedFileChunk chunk;
            chunk.offset = total;
            chunk.data.assign(1024U * 1024U, '\0');
            input.read(chunk.data.data(), static_cast<std::streamsize>(chunk.data.size()));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                chunk.data.resize(static_cast<std::size_t>(bytes));
                total += static_cast<std::uint64_t>(bytes);
                if (data_visitor) {
                    data_visitor(std::move(chunk));
                }
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_pooled_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        DataSlotPool& pool,
        const std::function<void(PooledFileChunk&&)>& data_visitor) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::uint64_t total = 0;
        while (input) {
            DataSlotHandle handle = pool.acquire_wait_or_throw(DataSlotClass::large, kLargeChunkBytes);
            input.read(pool.data(handle), static_cast<std::streamsize>(pool.capacity(handle)));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                DataBufTrailer& trailer = pool.trailer(handle);
                trailer.data_offset = total;
                trailer.data_len = static_cast<std::uint64_t>(bytes);
                PooledFileChunk chunk;
                chunk.offset = total;
                chunk.handle = handle;
                total += static_cast<std::uint64_t>(bytes);
                if (data_visitor) {
                    data_visitor(std::move(chunk));
                } else {
                    pool.release(handle);
                }
            } else {
                pool.release(handle);
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_raw_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        RawBufferPool& pool,
        const std::function<void(RawFileChunk&&)>& data_visitor,
        const std::function<bool()>& should_stop,
        bool copy_payload_to_buffer) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::vector<char> scratch;
        if (!copy_payload_to_buffer) {
            scratch.resize(kLargeChunkBytes);
        }
        std::uint64_t total = 0;
        while (input && !(should_stop && should_stop())) {
            BufferHandle handle = pool.acquire_spin();
            DataBuffer& buffer = data_buffer(pool, handle);
            char* read_target = copy_payload_to_buffer ? reinterpret_cast<char*>(buffer.bytes.data())
                                                       : scratch.data();
            const std::size_t read_capacity = copy_payload_to_buffer ? buffer.bytes.size() : scratch.size();
            input.read(read_target, static_cast<std::streamsize>(read_capacity));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                buffer.trailer = {};
                buffer.trailer.data_offset = total;
                buffer.trailer.data_len = static_cast<std::uint64_t>(bytes);
                RawFileChunk chunk;
                chunk.offset = total;
                chunk.handle = handle;
                total += static_cast<std::uint64_t>(bytes);
                if (data_visitor) {
                    data_visitor(std::move(chunk));
                } else {
                    pool.release(handle);
                }
            } else {
                pool.release(handle);
            }
        }
        if (!input.eof() && !(should_stop && should_stop())) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t visit_file_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::string buffer(1024U * 1024U, '\0');
        std::uint64_t total = 0;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                const auto bytes_read = static_cast<std::uint64_t>(bytes);
                if (data_visitor) {
                    data_visitor(total, std::string_view(buffer.data(), static_cast<std::size_t>(bytes)));
                }
                total += bytes_read;
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::optional<FileSpec> stat_path(std::string_view rel_path) const override {
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        if (!std::filesystem::exists(absolute_path)) {
            return std::nullopt;
        }
        return local_spec_from_absolute(root_, absolute_path);
    }

    [[nodiscard]] bool metadata_matches(std::string_view rel_path,
                                        std::uint64_t size,
                                        std::uint64_t mtime) const override {
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        struct stat info {};
        if (::stat(absolute_path.c_str(), &info) != 0) {
            return false;
        }
        return static_cast<std::uint64_t>(info.st_size) == size && stat_mtime_ns(info) == mtime;
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        return file_hash64(absolute_path);
    }

    [[nodiscard]] bool uses_async_api() const override {
        return false;
    }

    [[nodiscard]] std::string description() const override {
        return root_;
    }

private:
    std::filesystem::path root_;
};

class LocalTargetWriterBackend final : public TargetWriterBackend {
public:
    explicit LocalTargetWriterBackend(std::string root) : root_(std::move(root)) {}

    ~LocalTargetWriterBackend() override = default;

    void ensure_directory(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        if (rel_path.empty()) {
            return;
        }
        std::filesystem::create_directories(std::filesystem::path(root_) / rel_path);
    }

    void apply_directory_metadata(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        if (rel_path.empty()) {
            return;
        }
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / rel_path;
        std::filesystem::create_directories(absolute_path);
        FileSpec metadata = spec;
        metadata.rel_path = rel_path;
        hypersync::apply_directory_metadata(absolute_path, metadata);
    }

    void write_chunk(const FileSpec& spec, std::string_view data, std::uint64_t offset) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        ScopedFd& handle = open_handles_[rel_path];
        if (!handle.valid()) {
            const std::filesystem::path absolute_path = std::filesystem::path(root_) / rel_path;
            ensure_parent_directories(absolute_path);
            const int raw_fd =
                ::open(absolute_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, static_cast<mode_t>(spec.mode));
            if (raw_fd < 0) {
                throw std::system_error(errno, std::generic_category(), "open failed for " + absolute_path.string());
            }
            handle.reset(raw_fd);
        }
        local_pwrite_all(handle.get(), data, offset);
    }

    void finish_file(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        auto it = open_handles_.find(rel_path);
        if (it != open_handles_.end()) {
            it->second.reset();
            open_handles_.erase(it);
        }

        FileSpec metadata = spec;
        metadata.rel_path = rel_path;
        apply_file_metadata(std::filesystem::path(root_) / rel_path, metadata);
    }

    void abort_file(std::string_view rel_path) noexcept override {
        const std::string normalized = normalize_path(rel_path);
        open_handles_.erase(normalized);
        try {
            std::filesystem::remove(std::filesystem::path(root_) / normalized);
        } catch (...) {}
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        return file_hash64(std::filesystem::path(root_) / normalize_path(rel_path));
    }

    [[nodiscard]] bool uses_async_api() const override {
        return false;
    }

private:
    std::filesystem::path root_;
    std::unordered_map<std::string, ScopedFd> open_handles_;
};

#if HYPERSYNC_HAS_LIBNFS

struct AsyncCommandState {
    bool done = false;
    int status = 0;
    void* data = nullptr;
    std::string error;
};

struct AsyncStat64State {
    bool done = false;
    int status = 0;
    std::string error;
    struct nfs_stat_64 stat {};
};

struct AsyncDiscardReadState {
    bool done = false;
    int status = 0;
    std::string error;
};

struct AsyncVisitReadState {
    bool done = false;
    int status = 0;
    std::string error;
    std::exception_ptr visitor_error;
    std::uint64_t offset = 0;
    const std::function<void(std::uint64_t offset, std::string_view data)>* visitor = nullptr;
};

struct AsyncPooledReadState {
    bool done = false;
    int status = 0;
    std::string error;
    std::uint64_t offset = 0;
    std::size_t requested = 0;
    DataSlotPool* pool = nullptr;
    DataSlotHandle handle;
};

struct AsyncRawPooledReadState {
    bool done = false;
    int status = 0;
    std::string error;
    std::uint64_t offset = 0;
    std::size_t requested = 0;
    RawBufferPool* pool = nullptr;
    BufferHandle handle;
    bool copy_payload_to_buffer = true;
};

struct PendingDiscardRead {
    AsyncDiscardReadState state;
    std::uint64_t offset = 0;
    std::size_t requested = 0;
};

struct PendingVisitRead {
    AsyncVisitReadState state;
    std::uint64_t offset = 0;
    std::size_t requested = 0;
};

struct PendingPooledRead {
    AsyncPooledReadState state;
    bool in_use = false;
};

struct PendingRawPooledRead {
    AsyncRawPooledReadState state;
    bool in_use = false;
};

struct PendingDirectoryOpen {
    AsyncCommandState state;
    FileSpec folder;
    std::string remote_path;
    std::size_t retry_attempts = 0;
    bool in_use = false;
};

struct RetriedDirectoryOpen {
    FileSpec folder;
    std::size_t retry_attempts = 0;
};

void generic_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncCommandState*>(private_data);
    state->done = true;
    state->status = status;
    state->data = data;
    if (status < 0 && data != nullptr) {
        state->error = static_cast<const char*>(data);
    }
}

void stat64_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncStat64State*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (data != nullptr) {
        state->stat = *static_cast<const struct nfs_stat_64*>(data);
    }
}

void discard_read_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncDiscardReadState*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0 && data != nullptr) {
        state->error = static_cast<const char*>(data);
    }
}

void visit_read_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncVisitReadState*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (status > 0 && data != nullptr && state->visitor != nullptr) {
        try {
            // libnfs owns this buffer; the view is valid only for the callback.
            (*state->visitor)(state->offset,
                              std::string_view(static_cast<const char*>(data), static_cast<std::size_t>(status)));
        } catch (...) {
            state->visitor_error = std::current_exception();
        }
    }
}

void pooled_read_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncPooledReadState*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (status > 0 && data != nullptr) {
        const auto bytes_read = static_cast<std::size_t>(status);
        if (state->pool == nullptr) {
            state->error = "pooled read callback missing buffer pool";
            state->status = -EIO;
            return;
        }
        std::memcpy(state->pool->data(state->handle), data, bytes_read);
        DataBufTrailer& trailer = state->pool->trailer(state->handle);
        trailer.data_offset = state->offset;
        trailer.data_len = bytes_read;
    }
}

void raw_pooled_read_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncRawPooledReadState*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (status > 0) {
        const auto bytes_read = static_cast<std::size_t>(status);
        if (state->pool == nullptr) {
            state->error = "raw pooled read callback missing buffer pool";
            state->status = -EIO;
            return;
        }
        if (state->copy_payload_to_buffer && data == nullptr) {
            state->error = "raw pooled read callback missing data";
            state->status = -EIO;
            return;
        }
        DataBuffer& buffer = data_buffer(*state->pool, state->handle);
        if (state->copy_payload_to_buffer) {
            std::memcpy(buffer.bytes.data(), data, bytes_read);
        }
        buffer.trailer = {};
        buffer.trailer.data_offset = state->offset;
        buffer.trailer.data_len = bytes_read;
    }
}

void pump_nfs_until_done(struct nfs_context* nfs, AsyncCommandState& state) {
    while (!state.done) {
        const int fd = nfs_get_fd(nfs);
        const int events = nfs_which_events(nfs);

        struct pollfd descriptor {
            fd, static_cast<short>(events), 0
        };

        const int poll_result = fd >= 0 ? ::poll(&descriptor, 1, 100) : 0;
        if (poll_result < 0) {
            throw std::system_error(errno, std::generic_category(), "poll failed for libnfs context");
        }

        const int revents = poll_result > 0 ? descriptor.revents : 0;
        if (nfs_service(nfs, revents) < 0) {
            throw std::runtime_error("libnfs service failed: " + std::string(nfs_get_error(nfs)));
        }
    }
}

void pump_nfs_until_done(struct nfs_context* nfs, AsyncStat64State& state) {
    while (!state.done) {
        const int fd = nfs_get_fd(nfs);
        const int events = nfs_which_events(nfs);

        struct pollfd descriptor {
            fd, static_cast<short>(events), 0
        };

        const int poll_result = fd >= 0 ? ::poll(&descriptor, 1, 100) : 0;
        if (poll_result < 0) {
            throw std::system_error(errno, std::generic_category(), "poll failed for libnfs context");
        }

        const int revents = poll_result > 0 ? descriptor.revents : 0;
        if (nfs_service(nfs, revents) < 0) {
            throw std::runtime_error("libnfs service failed: " + std::string(nfs_get_error(nfs)));
        }
    }
}

void service_nfs_context(struct nfs_context* nfs, int timeout_ms) {
    const int fd = nfs_get_fd(nfs);
    const int events = nfs_which_events(nfs);

    struct pollfd descriptor {
        fd, static_cast<short>(events), 0
    };

    const int poll_result = fd >= 0 ? ::poll(&descriptor, 1, timeout_ms) : 0;
    if (poll_result < 0) {
        throw std::system_error(errno, std::generic_category(), "poll failed for libnfs context");
    }

    const int revents = poll_result > 0 ? descriptor.revents : 0;
    if (nfs_service(nfs, revents) < 0) {
        const char* error = nfs_get_error(nfs);
        std::ostringstream message;
        message << "libnfs service failed: " << ((error != nullptr && *error != '\0') ? error : "unknown error")
                << " (fd=" << fd << ", events=" << events << ", revents=" << revents << ")";
        throw std::runtime_error(message.str());
    }
}

template <typename StartFn>
AsyncCommandState run_async_command(struct nfs_context* nfs, StartFn&& start_fn, std::string_view operation) {
    AsyncCommandState state;
    const int queue_result = start_fn(&state);
    if (queue_result != 0) {
        throw std::runtime_error(std::string(operation) + " queue failed: " + std::string(nfs_get_error(nfs)));
    }

    pump_nfs_until_done(nfs, state);
    if (state.status < 0) {
        throw std::runtime_error(std::string(operation) + " failed: " + state.error);
    }
    return state;
}

template <typename StartFn>
AsyncStat64State run_stat64_command(struct nfs_context* nfs, StartFn&& start_fn, std::string_view operation) {
    AsyncStat64State state;
    const int queue_result = start_fn(&state);
    if (queue_result != 0) {
        throw std::runtime_error(std::string(operation) + " queue failed: " + std::string(nfs_get_error(nfs)));
    }

    pump_nfs_until_done(nfs, state);
    if (state.status < 0) {
        throw std::runtime_error(std::string(operation) + " failed: " + state.error);
    }
    return state;
}

template <typename StartFn>
AsyncStat64State execute_stat64_command(struct nfs_context* nfs, StartFn&& start_fn, std::string_view operation) {
    AsyncStat64State state;
    const int queue_result = start_fn(&state);
    if (queue_result != 0) {
        throw std::runtime_error(std::string(operation) + " queue failed: " + std::string(nfs_get_error(nfs)));
    }

    pump_nfs_until_done(nfs, state);
    return state;
}

std::uint64_t mtime_from_nfs_stat(const struct nfs_stat_64& stat) {
    return stat.nfs_mtime * 1'000'000'000ULL + stat.nfs_mtime_nsec;
}

std::uint64_t mtime_from_nfs_dirent(const struct nfsdirent& entry) {
    return static_cast<std::uint64_t>(entry.mtime.tv_sec) * 1'000'000'000ULL + entry.mtime_nsec;
}

std::string join_remote_path(std::string_view base, std::string_view name) {
    if (base.empty() || base == "/") {
        return "/" + std::string(name);
    }
    return std::string(base) + "/" + std::string(name);
}

std::optional<struct nfs_stat_64> try_stat64(struct nfs_context* nfs, std::string_view remote_path) {
    const std::string remote_path_string(remote_path);
    const AsyncStat64State state = execute_stat64_command(
        nfs,
        [&](AsyncStat64State* command_state) {
            return nfs_stat64_async(nfs, remote_path_string.c_str(), stat64_nfs_callback, command_state);
        },
        "nfs_stat64_async");

    if (state.status == -ENOENT) {
        return std::nullopt;
    }
    if (state.status < 0) {
        throw std::runtime_error("nfs_stat64_async failed: " + state.error);
    }
    return state.stat;
}

std::uint64_t nfs_stream_hash(struct nfs_context* nfs, const std::string& remote_path) {
    const AsyncStat64State stat_state = run_stat64_command(
        nfs,
        [&](AsyncStat64State* state) {
            return nfs_stat64_async(nfs, remote_path.c_str(), stat64_nfs_callback, state);
        },
        "nfs_stat64_async");
    const std::uint64_t file_size = stat_state.stat.nfs_size;

    const AsyncCommandState open_state = run_async_command(
        nfs,
        [&](AsyncCommandState* state) {
            return nfs_open_async(nfs, remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
        },
        "nfs_open_async");
    auto* handle = static_cast<struct nfsfh*>(open_state.data);
    Hash64State hasher;
    try {
        std::uint64_t offset = 0;
        while (offset < file_size) {
            const std::size_t chunk_bytes =
                std::min<std::size_t>(static_cast<std::size_t>(file_size - offset), 1024U * 1024U);
            std::string buffer(chunk_bytes, '\0');
            const int status = nfs_pread(nfs, handle, offset, chunk_bytes, buffer.data());
            if (status < 0) {
                const char* error = nfs_get_error(nfs);
                throw std::runtime_error("nfs_pread failed: " + std::string(error != nullptr ? error : "unknown error"));
            }
            if (status == 0 && chunk_bytes != 0U) {
                throw std::runtime_error("unexpected EOF while hashing NFS file: " + remote_path);
            }
            buffer.resize(static_cast<std::size_t>(status));
            hasher.update(buffer);
            offset += static_cast<std::uint64_t>(status);
        }
        run_async_command(
            nfs,
            [&](AsyncCommandState* state) {
                return nfs_close_async(nfs, handle, generic_nfs_callback, state);
            },
            "nfs_close_async");
    } catch (...) {
        try {
            run_async_command(
                nfs,
                [&](AsyncCommandState* state) {
                    return nfs_close_async(nfs, handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {}
        throw;
    }
    return hasher.value();
}

class LibNfsSession {
public:
    explicit LibNfsSession(const std::string& root_url)
        : root_url_(root_url),
          connection_url_(choose_nfs_connection_url(root_url)),
          nfs_(nfs_init_context()) {
        if (nfs_ == nullptr) {
            throw std::runtime_error("failed to initialize libnfs context");
        }

        url_ = nfs_parse_url_dir(nfs_, connection_url_.c_str());
        if (url_ == nullptr) {
            throw std::runtime_error("failed to parse NFS URL: " + connection_url_);
        }
        if (url_->server == nullptr || url_->path == nullptr) {
            throw std::runtime_error("NFS URL must include server and export path: " + connection_url_);
        }

        run_async_command(
            nfs_,
            [&](AsyncCommandState* state) {
                return nfs_mount_async(nfs_, url_->server, url_->path, generic_nfs_callback, state);
            },
            "nfs_mount_async");
    }

    ~LibNfsSession() {
        if (url_ != nullptr) {
            nfs_destroy_url(url_);
            url_ = nullptr;
        }
        if (nfs_ != nullptr) {
            nfs_destroy_context(nfs_);
            nfs_ = nullptr;
        }
    }

    LibNfsSession(const LibNfsSession&) = delete;
    LibNfsSession& operator=(const LibNfsSession&) = delete;

    [[nodiscard]] struct nfs_context* context() const {
        return nfs_;
    }

    [[nodiscard]] const std::string& connection_url() const {
        return connection_url_;
    }

private:
    std::string root_url_;
    std::string connection_url_;
    struct nfs_context* nfs_ = nullptr;
    struct nfs_url* url_ = nullptr;
};

class LibNfsBackend final : public NfsBackend {
public:
    explicit LibNfsBackend(std::string root_url) : root_url_(std::move(root_url)) {}

    [[nodiscard]] std::vector<FileSpec> list_files(bool recursive) const override {
        std::vector<FileSpec> files;
        collect_files(session().context(), "/", "", recursive, files);
        std::sort(files.begin(), files.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
            return lhs.rel_path < rhs.rel_path;
        });
        return files;
    }

    [[nodiscard]] std::vector<FileSpec> list_directories(bool recursive) const override {
        std::vector<FileSpec> directories;
        collect_directories(session().context(), "/", "", recursive, directories);
        std::sort(directories.begin(), directories.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
            if (path_depth(lhs.rel_path) != path_depth(rhs.rel_path)) {
                return path_depth(lhs.rel_path) < path_depth(rhs.rel_path);
            }
            return lhs.rel_path < rhs.rel_path;
        });
        return directories;
    }

    [[nodiscard]] FileSpec load_file(std::string_view rel_path) const override {
        return load_file(rel_path, 1);
    }

    [[nodiscard]] FileSpec load_file(std::string_view rel_path, std::size_t outstanding_requests) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();

        const AsyncStat64State stat_state = run_stat64_command(
            active_session.context(),
            [&](AsyncStat64State* state) {
                return nfs_stat64_async(active_session.context(), remote_path.c_str(), stat64_nfs_callback, state);
            },
            "nfs_stat64_async");

        FileSpec spec;
        spec.rel_path = normalized_path;
        spec.declared_size = stat_state.stat.nfs_size;
        spec.mtime = mtime_from_nfs_stat(stat_state.stat);
        spec.mode = static_cast<std::uint32_t>(stat_state.stat.nfs_mode & 0777U);
        spec.uid = static_cast<std::uint32_t>(stat_state.stat.nfs_uid);
        spec.gid = static_cast<std::uint32_t>(stat_state.stat.nfs_gid);

        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        try {
            if (spec.declared_size != 0) {
                if (spec.declared_size > static_cast<std::uint64_t>(std::string().max_size())) {
                    throw std::runtime_error("NFS file is too large for in-memory buffering: " + normalized_path);
                }
                spec.content.assign(static_cast<std::size_t>(spec.declared_size), '\0');
            }

            (void)outstanding_requests;
            std::uint64_t total_read = 0;
            while (total_read < spec.declared_size) {
                const std::uint64_t remaining = spec.declared_size - total_read;
                const std::size_t requested =
                    static_cast<std::size_t>(std::min<std::uint64_t>(remaining, 1024U * 1024U));
                const int status = nfs_pread(active_session.context(),
                                             handle,
                                             total_read,
                                             requested,
                                             spec.content.data() + static_cast<std::size_t>(total_read));
                if (status < 0) {
                    const char* error = nfs_get_error(active_session.context());
                    throw std::runtime_error("nfs_pread failed: " + std::string(error != nullptr ? error : "unknown error"));
                }
                if (status == 0) {
                    throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                }
                total_read += static_cast<std::uint64_t>(status);
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }

        return spec;
    }

    [[nodiscard]] std::uint64_t read_file_discard(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t)>& bytes_visitor) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        try {
            const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
            std::vector<PendingDiscardRead> pending(max_in_flight);
            std::vector<bool> in_use(max_in_flight, false);
            std::size_t in_flight = 0;
            std::uint64_t next_offset = 0;

            while (next_offset < declared_size || in_flight != 0) {
                while (next_offset < declared_size && in_flight < max_in_flight) {
                    const auto free_it = std::find(in_use.begin(), in_use.end(), false);
                    if (free_it == in_use.end()) {
                        break;
                    }
                    const std::size_t index = static_cast<std::size_t>(std::distance(in_use.begin(), free_it));
                    PendingDiscardRead& read = pending[index];
                    read = {};
                    read.offset = next_offset;
                    const std::size_t remaining = static_cast<std::size_t>(declared_size - next_offset);
                    read.requested = std::min<std::size_t>(remaining, 1024U * 1024U);

                    const int queue_result = nfs_pread_async(active_session.context(),
                                                             handle,
                                                             read.offset,
                                                             read.requested,
                                                             discard_read_nfs_callback,
                                                             &read.state);
                    if (queue_result != 0) {
                        throw std::runtime_error("nfs_pread_async queue failed: " +
                                                 std::string(nfs_get_error(active_session.context())));
                    }
                    in_use[index] = true;
                    ++in_flight;
                    next_offset += read.requested;
                }

                service_nfs_context(active_session.context(), in_flight == 0 ? 0 : 100);

                for (std::size_t index = 0; index < pending.size(); ++index) {
                    if (!in_use[index] || !pending[index].state.done) {
                        continue;
                    }
                    PendingDiscardRead& read = pending[index];
                    if (read.state.status < 0) {
                        throw std::runtime_error("nfs_pread_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.requested != 0U) {
                        throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    total_read += bytes_read;
                    if (bytes_read != 0U && bytes_visitor) {
                        bytes_visitor(static_cast<std::uint64_t>(bytes_read));
                    }
                    if (bytes_read < read.requested) {
                        read.offset += bytes_read;
                        read.requested -= bytes_read;
                        read.state = {};
                        const int queue_result = nfs_pread_async(active_session.context(),
                                                                 handle,
                                                                 read.offset,
                                                                 read.requested,
                                                                 discard_read_nfs_callback,
                                                                 &read.state);
                        if (queue_result != 0) {
                            throw std::runtime_error("nfs_pread_async queue failed: " +
                                                     std::string(nfs_get_error(active_session.context())));
                        }
                        continue;
                    }
                    in_use[index] = false;
                    --in_flight;
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t read_file_stream(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::string_view)>& data_visitor) const override {
        return read_file_owned_chunks(rel_path, declared_size, outstanding_requests, [&](OwnedFileChunk&& chunk) {
            if (data_visitor && !chunk.data.empty()) {
                data_visitor(chunk.data);
            }
        });
    }

    [[nodiscard]] std::uint64_t read_file_owned_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(OwnedFileChunk&&)>& data_visitor) const override {
        (void)outstanding_requests;
        // libnfs async pread returns a borrowed callback buffer, so the owned-buffer
        // path uses sync pread into application-owned storage.
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        try {
            while (total_read < declared_size) {
                OwnedFileChunk chunk;
                chunk.offset = total_read;
                const std::uint64_t remaining = declared_size - total_read;
                const std::size_t requested =
                    static_cast<std::size_t>(std::min<std::uint64_t>(remaining, 1024U * 1024U));
                chunk.data.assign(requested, '\0');

                const int status =
                    nfs_pread(active_session.context(), handle, chunk.offset, requested, chunk.data.data());
                if (status < 0) {
                    const char* error = nfs_get_error(active_session.context());
                    throw std::runtime_error("nfs_pread failed: " + std::string(error != nullptr ? error : "unknown error"));
                }
                if (status == 0) {
                    throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                }

                chunk.data.resize(static_cast<std::size_t>(status));
                total_read += static_cast<std::uint64_t>(status);
                if (data_visitor) {
                    data_visitor(std::move(chunk));
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t read_file_pooled_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        DataSlotPool& pool,
        const std::function<void(PooledFileChunk&&)>& data_visitor) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
        std::vector<PendingPooledRead> pending(max_in_flight);
        std::size_t in_flight = 0;
        const auto drain_pending_reads = [&]() noexcept {
            while (in_flight != 0U) {
                try {
                    service_nfs_context(active_session.context(), 100);
                } catch (...) {
                    return;
                }
                for (auto& read : pending) {
                    if (!read.in_use || !read.state.done) {
                        continue;
                    }
                    try {
                        pool.release(read.state.handle);
                    } catch (...) {
                    }
                    read.in_use = false;
                    --in_flight;
                }
            }
        };
        try {
            std::uint64_t next_offset = 0;
            std::uint64_t next_emit_offset = 0;

            const auto queue_read = [&](PendingPooledRead& read,
                                        std::uint64_t offset,
                                        std::size_t requested) {
                DataSlotHandle slot = pool.acquire_wait_or_throw(DataSlotClass::large, requested);
                read.state = {};
                read.state.offset = offset;
                read.state.requested = requested;
                read.state.pool = &pool;
                read.state.handle = slot;
                pool.trailer(slot) = {};
                const int queue_result = nfs_pread_async(active_session.context(),
                                                         handle,
                                                         offset,
                                                         requested,
                                                         pooled_read_nfs_callback,
                                                         &read.state);
                if (queue_result != 0) {
                    pool.release(slot);
                    throw std::runtime_error("nfs_pread_async queue failed: " +
                                             std::string(nfs_get_error(active_session.context())));
                }
                read.in_use = true;
            };

            while (next_offset < declared_size || in_flight != 0) {
                for (auto& read : pending) {
                    if (next_offset >= declared_size || in_flight >= max_in_flight) {
                        break;
                    }
                    if (read.in_use) {
                        continue;
                    }
                    const std::uint64_t remaining = declared_size - next_offset;
                    const std::size_t requested =
                        static_cast<std::size_t>(std::min<std::uint64_t>(remaining, kLargeChunkBytes));
                    queue_read(read, next_offset, requested);
                    next_offset += requested;
                    ++in_flight;
                }

                service_nfs_context(active_session.context(), in_flight == 0 ? 0 : 100);

                bool drained_ready_read = true;
                while (drained_ready_read) {
                    drained_ready_read = false;
                    auto ready_read = std::find_if(pending.begin(), pending.end(), [&](const PendingPooledRead& read) {
                        return read.in_use && read.state.done && read.state.offset == next_emit_offset;
                    });
                    if (ready_read == pending.end()) {
                        break;
                    }

                    PendingPooledRead& read = *ready_read;
                    if (read.state.status < 0) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("nfs_pread_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.state.requested != 0U) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    const std::uint64_t chunk_offset = read.state.offset;
                    const std::size_t requested = read.state.requested;
                    const DataSlotHandle handle_for_visitor = read.state.handle;
                    total_read += bytes_read;
                    next_emit_offset += bytes_read;

                    read.in_use = false;
                    if (bytes_read != 0U) {
                        PooledFileChunk chunk;
                        chunk.offset = chunk_offset;
                        chunk.handle = handle_for_visitor;
                        if (data_visitor) {
                            data_visitor(std::move(chunk));
                        } else {
                            pool.release(handle_for_visitor);
                        }
                    } else {
                        pool.release(handle_for_visitor);
                    }

                    if (bytes_read < requested) {
                        const std::uint64_t retry_offset = chunk_offset + bytes_read;
                        const std::size_t retry_bytes = requested - bytes_read;
                        queue_read(read, retry_offset, retry_bytes);
                    } else {
                        --in_flight;
                    }
                    drained_ready_read = true;
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            drain_pending_reads();
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t read_file_raw_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        RawBufferPool& pool,
        const std::function<void(RawFileChunk&&)>& data_visitor,
        const std::function<bool()>& should_stop,
        bool copy_payload_to_buffer) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (pool.pool_id() != kDataBufferPoolId) {
            throw std::runtime_error("raw NFS data reader requires the data buffer pool");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
        std::vector<PendingRawPooledRead> pending(max_in_flight);
        std::size_t in_flight = 0;
        const auto drain_pending_reads = [&]() noexcept {
            while (in_flight != 0U) {
                try {
                    service_nfs_context(active_session.context(), 100);
                } catch (...) {
                    return;
                }
                for (auto& read : pending) {
                    if (!read.in_use || !read.state.done) {
                        continue;
                    }
                    try {
                        pool.release(read.state.handle);
                    } catch (...) {
                    }
                    read.in_use = false;
                    --in_flight;
                }
            }
        };
        try {
            std::uint64_t next_offset = 0;
            std::uint64_t next_emit_offset = 0;

            const auto queue_read = [&](PendingRawPooledRead& read,
                                        std::uint64_t offset,
                                        std::size_t requested) {
                BufferHandle slot = pool.acquire_spin();
                DataBuffer& buffer = data_buffer(pool, slot);
                buffer.trailer = {};
                read.state = {};
                read.state.offset = offset;
                read.state.requested = requested;
                read.state.pool = &pool;
                read.state.handle = slot;
                read.state.copy_payload_to_buffer = copy_payload_to_buffer;
                const int queue_result = nfs_pread_async(active_session.context(),
                                                         handle,
                                                         offset,
                                                         requested,
                                                         raw_pooled_read_nfs_callback,
                                                         &read.state);
                if (queue_result != 0) {
                    pool.release(slot);
                    throw std::runtime_error("nfs_pread_async queue failed: " +
                                             std::string(nfs_get_error(active_session.context())));
                }
                read.in_use = true;
            };

            while ((next_offset < declared_size && !(should_stop && should_stop())) || in_flight != 0) {
                for (auto& read : pending) {
                    if (next_offset >= declared_size ||
                        in_flight >= max_in_flight ||
                        (should_stop && should_stop())) {
                        break;
                    }
                    if (read.in_use) {
                        continue;
                    }
                    const std::uint64_t remaining = declared_size - next_offset;
                    const std::size_t requested =
                        static_cast<std::size_t>(std::min<std::uint64_t>(remaining, kLargeChunkBytes));
                    queue_read(read, next_offset, requested);
                    next_offset += requested;
                    ++in_flight;
                }

                service_nfs_context(active_session.context(), in_flight == 0 ? 0 : 100);

                bool drained_ready_read = true;
                while (drained_ready_read) {
                    drained_ready_read = false;
                    auto ready_read =
                        std::find_if(pending.begin(), pending.end(), [&](const PendingRawPooledRead& read) {
                            return read.in_use && read.state.done && read.state.offset == next_emit_offset;
                        });
                    if (ready_read == pending.end()) {
                        break;
                    }

                    PendingRawPooledRead& read = *ready_read;
                    if (read.state.status < 0) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("nfs_pread_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.state.requested != 0U) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    const std::uint64_t chunk_offset = read.state.offset;
                    const std::size_t requested = read.state.requested;
                    const BufferHandle handle_for_visitor = read.state.handle;
                    total_read += bytes_read;
                    next_emit_offset += bytes_read;

                    read.in_use = false;
                    if (bytes_read != 0U) {
                        RawFileChunk chunk;
                        chunk.offset = chunk_offset;
                        chunk.handle = handle_for_visitor;
                        if (data_visitor) {
                            data_visitor(std::move(chunk));
                        } else {
                            pool.release(handle_for_visitor);
                        }
                    } else {
                        pool.release(handle_for_visitor);
                    }

                    if (bytes_read < requested) {
                        const std::uint64_t retry_offset = chunk_offset + bytes_read;
                        const std::size_t retry_bytes = requested - bytes_read;
                        queue_read(read, retry_offset, retry_bytes);
                    } else {
                        --in_flight;
                    }
                    drained_ready_read = true;
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            drain_pending_reads();
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t visit_file_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        try {
            const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
            std::vector<PendingVisitRead> pending(max_in_flight);
            std::vector<bool> in_use(max_in_flight, false);
            std::size_t in_flight = 0;
            std::uint64_t next_offset = 0;

            while (next_offset < declared_size || in_flight != 0) {
                while (next_offset < declared_size && in_flight < max_in_flight) {
                    const auto free_it = std::find(in_use.begin(), in_use.end(), false);
                    if (free_it == in_use.end()) {
                        break;
                    }
                    const std::size_t index = static_cast<std::size_t>(std::distance(in_use.begin(), free_it));
                    PendingVisitRead& read = pending[index];
                    read = {};
                    read.offset = next_offset;
                    read.state.offset = next_offset;
                    read.state.visitor = &data_visitor;
                    const std::size_t remaining = static_cast<std::size_t>(declared_size - next_offset);
                    read.requested = std::min<std::size_t>(remaining, 1024U * 1024U);

                    const int queue_result = nfs_pread_async(active_session.context(),
                                                             handle,
                                                             read.offset,
                                                             read.requested,
                                                             visit_read_nfs_callback,
                                                             &read.state);
                    if (queue_result != 0) {
                        throw std::runtime_error("nfs_pread_async queue failed: " +
                                                 std::string(nfs_get_error(active_session.context())));
                    }
                    in_use[index] = true;
                    ++in_flight;
                    next_offset += read.requested;
                }

                service_nfs_context(active_session.context(), in_flight == 0 ? 0 : 100);

                for (std::size_t index = 0; index < pending.size(); ++index) {
                    if (!in_use[index] || !pending[index].state.done) {
                        continue;
                    }
                    PendingVisitRead& read = pending[index];
                    if (read.state.visitor_error) {
                        std::rethrow_exception(read.state.visitor_error);
                    }
                    if (read.state.status < 0) {
                        throw std::runtime_error("nfs_pread_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.requested != 0U) {
                        throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    total_read += bytes_read;
                    if (bytes_read < read.requested) {
                        read.offset += bytes_read;
                        read.requested -= bytes_read;
                        read.state = {};
                        read.state.offset = read.offset;
                        read.state.visitor = &data_visitor;
                        const int queue_result = nfs_pread_async(active_session.context(),
                                                                 handle,
                                                                 read.offset,
                                                                 read.requested,
                                                                 visit_read_nfs_callback,
                                                                 &read.state);
                        if (queue_result != 0) {
                            throw std::runtime_error("nfs_pread_async queue failed: " +
                                                     std::string(nfs_get_error(active_session.context())));
                        }
                        continue;
                    }
                    in_use[index] = false;
                    --in_flight;
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::optional<FileSpec> stat_path(std::string_view rel_path) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            return std::nullopt;
        }

        const auto stat = try_stat64(session().context(), "/" + normalized_path);
        if (!stat.has_value()) {
            return std::nullopt;
        }

        FileSpec spec;
        spec.rel_path = normalized_path;
        spec.declared_size = stat->nfs_size;
        spec.mtime = mtime_from_nfs_stat(*stat);
        spec.mode = static_cast<std::uint32_t>(stat->nfs_mode & 0777U);
        spec.uid = static_cast<std::uint32_t>(stat->nfs_uid);
        spec.gid = static_cast<std::uint32_t>(stat->nfs_gid);
        return spec;
    }

    [[nodiscard]] bool metadata_matches(std::string_view rel_path,
                                        std::uint64_t size,
                                        std::uint64_t mtime) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            return false;
        }

        const auto stat = try_stat64(session().context(), "/" + normalized_path);
        if (!stat.has_value()) {
            return false;
        }
        return stat->nfs_size == size &&
               truncate_to_microseconds(mtime_from_nfs_stat(*stat)) == truncate_to_microseconds(mtime);
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        return nfs_stream_hash(session().context(), "/" + normalized_path);
    }

    [[nodiscard]] bool uses_async_api() const override {
        return true;
    }

    [[nodiscard]] std::string description() const override {
        return root_url_;
    }

    void visit_files(bool recursive, const std::function<void(FileSpec)>& visitor) const override {
        visit_files_impl(session().context(), "/", "", recursive, visitor);
    }

    void visit_metadata(bool recursive,
                        const std::function<void(FileSpec)>& file_visitor,
                        const std::function<void(FileSpec)>& directory_visitor) const override {
        visit_metadata_impl(session().context(), "/", "", recursive, file_visitor, directory_visitor);
    }

    void visit_metadata_at(std::string_view rel_path,
                           bool recursive,
                           const std::function<void(FileSpec)>& file_visitor,
                           const std::function<void(FileSpec)>& directory_visitor) const override {
        const std::string normalized_path = normalize_path(rel_path);
        const std::string remote_path = normalized_path.empty() ? "/" : "/" + normalized_path;
        visit_metadata_impl(session().context(), remote_path, normalized_path, recursive, file_visitor, directory_visitor);
    }

    void visit_folder(std::string_view rel_path,
                      const std::function<void(FileSpec)>& file_visitor,
                      const std::function<void(FileSpec)>& directory_visitor) const override {
        visit_metadata_at(rel_path, false, file_visitor, directory_visitor);
    }

    void scan_flat_folders(
        std::size_t outstanding_folders,
        const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
        const std::function<bool()>& should_stop,
        const std::function<void(FlatFolderScanBatch)>& folder_visitor) const override {
        struct nfs_context* nfs = session().context();
        const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_folders);
        constexpr std::size_t kMaxTransientFolderRetries = 3;
        std::vector<PendingDirectoryOpen> pending(max_in_flight);
        std::deque<RetriedDirectoryOpen> retry_folders;
        std::size_t in_flight = 0;
        bool provider_exhausted = false;

        auto requeue_or_fail = [&](PendingDirectoryOpen& slot, const std::string& error) {
            if (slot.retry_attempts < kMaxTransientFolderRetries && !should_stop()) {
                retry_folders.push_back(RetriedDirectoryOpen{std::move(slot.folder), slot.retry_attempts + 1U});
                slot = {};
                return;
            }

            FlatFolderScanBatch batch;
            batch.folder = std::move(slot.folder);
            batch.failed = true;
            batch.error = error;
            if (!slot.remote_path.empty()) {
                batch.error += " while scanning " + slot.remote_path;
            }
            slot = {};
            folder_visitor(std::move(batch));
        };

        auto recover_session = [&]() {
            session_.reset();
            if (!should_stop()) {
                nfs = session().context();
            }
        };

        auto recover_pending = [&](const std::string& error) {
            for (PendingDirectoryOpen& slot : pending) {
                if (!slot.in_use) {
                    continue;
                }
                requeue_or_fail(slot, error);
            }

            in_flight = 0;
            recover_session();
        };

        while (in_flight != 0 || (!should_stop() && (!retry_folders.empty() || !provider_exhausted))) {
            while (!should_stop() && in_flight < max_in_flight) {
                std::optional<FileSpec> folder;
                std::size_t retry_attempts = 0;
                if (!retry_folders.empty()) {
                    RetriedDirectoryOpen retry = std::move(retry_folders.front());
                    retry_folders.pop_front();
                    folder = std::move(retry.folder);
                    retry_attempts = retry.retry_attempts;
                } else {
                    folder = folder_provider(in_flight == 0);
                }
                if (!folder.has_value()) {
                    if (in_flight == 0) {
                        provider_exhausted = true;
                    }
                    break;
                }

                auto free_it = std::find_if(pending.begin(), pending.end(), [](const PendingDirectoryOpen& slot) {
                    return !slot.in_use;
                });
                if (free_it == pending.end()) {
                    break;
                }

                PendingDirectoryOpen& slot = *free_it;
                slot = {};
                slot.in_use = true;
                slot.folder = std::move(*folder);
                slot.retry_attempts = retry_attempts;
                const std::string normalized_path = normalize_path(slot.folder.rel_path);
                slot.folder.rel_path = normalized_path;
                slot.remote_path = normalized_path.empty() ? "/" : "/" + normalized_path;

                const int queue_result =
                    nfs_opendir_async(nfs, slot.remote_path.c_str(), generic_nfs_callback, &slot.state);
                if (queue_result != 0) {
                    slot.in_use = false;
                    FlatFolderScanBatch batch;
                    batch.folder = std::move(slot.folder);
                    batch.failed = true;
                    const char* error = nfs_get_error(nfs);
                    batch.error = "nfs_opendir_async queue failed: " +
                                  std::string((error != nullptr && *error != '\0') ? error : "unknown error");
                    if (retry_attempts < kMaxTransientFolderRetries) {
                        retry_folders.push_back(RetriedDirectoryOpen{std::move(batch.folder), retry_attempts + 1U});
                        recover_session();
                    } else {
                        folder_visitor(std::move(batch));
                    }
                    continue;
                }
                ++in_flight;
            }

            if (in_flight == 0) {
                if (should_stop()) {
                    break;
                }
                continue;
            }

            try {
                service_nfs_context(nfs, 100);
            } catch (const std::exception& error) {
                recover_pending(error.what());
                continue;
            }

            for (PendingDirectoryOpen& slot : pending) {
                if (!slot.in_use || !slot.state.done) {
                    continue;
                }

                FlatFolderScanBatch batch;
                batch.folder = std::move(slot.folder);
                if (slot.state.status < 0) {
                    batch.failed = true;
                    batch.error = slot.state.error.empty() ? "nfs_opendir_async failed with unknown error"
                                                           : slot.state.error;
                } else {
                    auto* directory = static_cast<struct nfsdir*>(slot.state.data);
                    try {
                        batch = read_flat_directory_batch(nfs, directory, std::move(batch), should_stop);
                    } catch (const std::exception& error) {
                        batch.failed = true;
                        batch.error = std::string(error.what()) + " while reading " + slot.remote_path;
                    }
                    if (directory != nullptr) {
                        nfs_closedir(nfs, directory);
                    }
                }
                slot.in_use = false;
                --in_flight;
                folder_visitor(std::move(batch));
            }
        }
    }

private:
    [[nodiscard]] LibNfsSession& session() const {
        if (!session_) {
            session_ = std::make_unique<LibNfsSession>(root_url_);
        }
        return *session_;
    }

    static FlatFolderScanBatch read_flat_directory_batch(struct nfs_context* nfs,
                                                         struct nfsdir* directory,
                                                         FlatFolderScanBatch batch,
                                                         const std::function<bool()>& should_stop) {
        if (directory == nullptr) {
            batch.failed = true;
            batch.error = "nfs_opendir_async returned no directory handle";
            return batch;
        }

        const std::string rel_prefix = normalize_path(batch.folder.rel_path);
        while (auto* entry = nfs_readdir(nfs, directory)) {
            if (should_stop()) {
                break;
            }

            const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
            if (entry_name.empty() || entry_name == "." || entry_name == "..") {
                continue;
            }

            const std::string rel_path =
                rel_prefix.empty() ? normalize_path(entry_name) : normalize_path(rel_prefix + "/" + std::string(entry_name));
            const mode_t mode = static_cast<mode_t>(entry->mode);
            if (S_ISDIR(mode)) {
                FileSpec spec;
                spec.rel_path = rel_path;
                spec.mtime = mtime_from_nfs_dirent(*entry);
                spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
                spec.uid = entry->uid;
                spec.gid = entry->gid;
                batch.directories.push_back(std::move(spec));
                continue;
            }
            if (!S_ISREG(mode)) {
                continue;
            }

            FileSpec spec;
            spec.rel_path = rel_path;
            spec.declared_size = entry->size;
            spec.mtime = mtime_from_nfs_dirent(*entry);
            spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
            spec.uid = entry->uid;
            spec.gid = entry->gid;
            batch.files.push_back(std::move(spec));
        }
        return batch;
    }

    static void visit_files_impl(struct nfs_context* nfs,
                                 const std::string& remote_path,
                                 const std::string& rel_prefix,
                                 bool recursive,
                                 const std::function<void(FileSpec)>& visitor) {
        const AsyncCommandState open_state = run_async_command(
            nfs,
            [&](AsyncCommandState* state) {
                return nfs_opendir_async(nfs, remote_path.c_str(), generic_nfs_callback, state);
            },
            "nfs_opendir_async");

        auto* directory = static_cast<struct nfsdir*>(open_state.data);
        while (auto* entry = nfs_readdir(nfs, directory)) {
            const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
            if (entry_name.empty() || entry_name == "." || entry_name == "..") {
                continue;
            }

            const std::string rel_path =
                rel_prefix.empty() ? normalize_path(entry_name) : normalize_path(rel_prefix + "/" + std::string(entry_name));
            const mode_t mode = static_cast<mode_t>(entry->mode);
            if (S_ISDIR(mode)) {
                if (recursive) {
                    visit_files_impl(nfs, join_remote_path(remote_path, entry_name), rel_path, true, visitor);
                }
                continue;
            }
            if (!S_ISREG(mode)) {
                continue;
            }

            FileSpec spec;
            spec.rel_path = rel_path;
            spec.declared_size = entry->size;
            spec.mtime = mtime_from_nfs_dirent(*entry);
            spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
            spec.uid = entry->uid;
            spec.gid = entry->gid;
            visitor(std::move(spec));
        }
        nfs_closedir(nfs, directory);
    }

    static void visit_metadata_impl(struct nfs_context* nfs,
                                    const std::string& remote_path,
                                    const std::string& rel_prefix,
                                    bool recursive,
                                    const std::function<void(FileSpec)>& file_visitor,
                                    const std::function<void(FileSpec)>& directory_visitor) {
        const AsyncCommandState open_state = run_async_command(
            nfs,
            [&](AsyncCommandState* state) {
                return nfs_opendir_async(nfs, remote_path.c_str(), generic_nfs_callback, state);
            },
            "nfs_opendir_async");

        auto* directory = static_cast<struct nfsdir*>(open_state.data);
        while (auto* entry = nfs_readdir(nfs, directory)) {
            const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
            if (entry_name.empty() || entry_name == "." || entry_name == "..") {
                continue;
            }

            const std::string rel_path =
                rel_prefix.empty() ? normalize_path(entry_name) : normalize_path(rel_prefix + "/" + std::string(entry_name));
            const mode_t mode = static_cast<mode_t>(entry->mode);
            if (S_ISDIR(mode)) {
                FileSpec spec;
                spec.rel_path = rel_path;
                spec.mtime = mtime_from_nfs_dirent(*entry);
                spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
                spec.uid = entry->uid;
                spec.gid = entry->gid;
                directory_visitor(std::move(spec));

                if (recursive) {
                    visit_metadata_impl(nfs, join_remote_path(remote_path, entry_name), rel_path, true, file_visitor, directory_visitor);
                }
                continue;
            }
            if (!S_ISREG(mode)) {
                continue;
            }

            FileSpec spec;
            spec.rel_path = rel_path;
            spec.declared_size = entry->size;
            spec.mtime = mtime_from_nfs_dirent(*entry);
            spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
            spec.uid = entry->uid;
            spec.gid = entry->gid;
            file_visitor(std::move(spec));
        }
        nfs_closedir(nfs, directory);
    }

    static void collect_files(struct nfs_context* nfs,
                              const std::string& remote_path,
                              const std::string& rel_prefix,
                              bool recursive,
                              std::vector<FileSpec>& files) {
        visit_files_impl(nfs, remote_path, rel_prefix, recursive,
                         [&files](FileSpec spec) { files.push_back(std::move(spec)); });
    }

    static void collect_directories(struct nfs_context* nfs,
                                    const std::string& remote_path,
                                    const std::string& rel_prefix,
                                    bool recursive,
                                    std::vector<FileSpec>& directories) {
        const AsyncCommandState open_state = run_async_command(
            nfs,
            [&](AsyncCommandState* state) {
                return nfs_opendir_async(nfs, remote_path.c_str(), generic_nfs_callback, state);
            },
            "nfs_opendir_async");

        auto* directory = static_cast<struct nfsdir*>(open_state.data);
        while (auto* entry = nfs_readdir(nfs, directory)) {
            const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
            if (entry_name.empty() || entry_name == "." || entry_name == "..") {
                continue;
            }

            const mode_t mode = static_cast<mode_t>(entry->mode);
            if (!S_ISDIR(mode)) {
                continue;
            }

            FileSpec spec;
            spec.rel_path =
                rel_prefix.empty() ? normalize_path(entry_name) : normalize_path(rel_prefix + "/" + std::string(entry_name));
            spec.mtime = mtime_from_nfs_dirent(*entry);
            spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
            spec.uid = entry->uid;
            spec.gid = entry->gid;
            directories.push_back(spec);

            if (recursive) {
                collect_directories(nfs, join_remote_path(remote_path, entry_name), spec.rel_path, true, directories);
            }
        }
        nfs_closedir(nfs, directory);
    }

    std::string root_url_;
    mutable std::unique_ptr<LibNfsSession> session_;
};

class LibNfsTargetWriterBackend final : public TargetWriterBackend {
public:
    explicit LibNfsTargetWriterBackend(std::string root_url)
        : root_url_(std::move(root_url)), session_(root_url_) {
        known_directories_.insert("");
    }

    ~LibNfsTargetWriterBackend() override {
        for (auto& [_, handle] : open_handles_) {
            if (handle != nullptr) {
                nfs_close(session_.context(), handle);
            }
        }
    }

    void ensure_directory(const FileSpec& spec) override {
        ensure_directory_chain(spec.rel_path);
    }

    void apply_directory_metadata(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        if (rel_path.empty()) {
            return;
        }
        ensure_directory_chain(rel_path);
        apply_remote_metadata("/" + rel_path, spec);
    }

    void write_chunk(const FileSpec& spec, std::string_view data, std::uint64_t offset) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        ensure_directory_chain(parent_path(rel_path));
        struct nfsfh* handle = open_handle(rel_path, spec.mode);

        std::size_t written_total = 0;
        while (written_total < data.size()) {
            const AsyncCommandState write_state = run_async_command(
                session_.context(),
                [&](AsyncCommandState* state) {
                    return nfs_pwrite_async(session_.context(),
                                            handle,
                                            offset + written_total,
                                            data.size() - written_total,
                                            data.data() + written_total,
                                            generic_nfs_callback,
                                            state);
                },
                "nfs_pwrite_async");

            if (write_state.status < 0) {
                throw std::runtime_error("nfs_pwrite_async failed");
            }
            written_total += static_cast<std::size_t>(write_state.status);
        }
    }

    void finish_file(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        const std::string remote_path = "/" + rel_path;
        auto it = open_handles_.find(rel_path);
        if (it == open_handles_.end()) {
            return;
        }

        struct nfsfh* handle = it->second;
        run_async_command(
            session_.context(),
            [&](AsyncCommandState* state) {
                return nfs_fsync_async(session_.context(), handle, generic_nfs_callback, state);
            },
            "nfs_fsync_async");
        run_async_command(
            session_.context(),
            [&](AsyncCommandState* state) {
                return nfs_close_async(session_.context(), handle, generic_nfs_callback, state);
            },
            "nfs_close_async");
        open_handles_.erase(it);
        apply_remote_metadata(remote_path, spec);
    }

    void abort_file(std::string_view rel_path) noexcept override {
        const std::string normalized = normalize_path(rel_path);
        auto it = open_handles_.find(normalized);
        if (it == open_handles_.end()) {
            return;
        }
        if (it->second != nullptr) {
            nfs_close(session_.context(), it->second);
        }
        open_handles_.erase(it);
        try {
            const std::string remote_path = "/" + normalized;
            nfs_unlink(session_.context(), remote_path.c_str());
        } catch (...) {}
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        return nfs_stream_hash(session_.context(), "/" + normalized_path);
    }

    [[nodiscard]] bool uses_async_api() const override {
        return true;
    }

private:
    void apply_remote_metadata(const std::string& remote_path, const FileSpec& spec) {
        const auto stat = try_stat64(session_.context(), remote_path);
        if (!stat.has_value()) {
            throw std::runtime_error("remote path missing while applying metadata: " + remote_path);
        }

        if (static_cast<std::uint32_t>(stat->nfs_uid) != spec.uid ||
            static_cast<std::uint32_t>(stat->nfs_gid) != spec.gid) {
            run_async_command(
                session_.context(),
                [&](AsyncCommandState* state) {
                    return nfs_chown_async(session_.context(),
                                           remote_path.c_str(),
                                           static_cast<int>(spec.uid),
                                           static_cast<int>(spec.gid),
                                           generic_nfs_callback,
                                           state);
                },
                "nfs_chown_async");
        }

        if ((stat->nfs_mode & 0777U) != spec.mode) {
            run_async_command(
                session_.context(),
                [&](AsyncCommandState* state) {
                    return nfs_chmod_async(session_.context(),
                                           remote_path.c_str(),
                                           static_cast<int>(spec.mode),
                                           generic_nfs_callback,
                                           state);
                },
                "nfs_chmod_async");
        }

        const std::uint64_t remote_mtime = truncate_to_microseconds(mtime_from_nfs_stat(*stat));
        if (remote_mtime != truncate_to_microseconds(spec.mtime)) {
            struct timeval times[2];
            times[0].tv_sec = static_cast<time_t>(spec.mtime / 1'000'000'000ULL);
            times[0].tv_usec = static_cast<suseconds_t>((spec.mtime % 1'000'000'000ULL) / 1000ULL);
            times[1] = times[0];
            run_async_command(
                session_.context(),
                [&](AsyncCommandState* state) {
                    return nfs_utimes_async(session_.context(), remote_path.c_str(), times, generic_nfs_callback, state);
                },
                "nfs_utimes_async");
        }
    }

    void ensure_directory_chain(std::string_view rel_path) {
        const std::string normalized = normalize_path(rel_path);
        if (normalized.empty()) {
            return;
        }

        std::string current_path;
        std::string component;
        for (char ch : normalized) {
            if (ch == '/') {
                if (!component.empty()) {
                    if (!current_path.empty()) {
                        current_path.push_back('/');
                    }
                    current_path += component;
                    ensure_single_directory(current_path);
                    component.clear();
                }
            } else {
                component.push_back(ch);
            }
        }
        if (!component.empty()) {
            if (!current_path.empty()) {
                current_path.push_back('/');
            }
            current_path += component;
            ensure_single_directory(current_path);
        }
    }

    void ensure_single_directory(const std::string& rel_path) {
        if (known_directories_.find(rel_path) != known_directories_.end()) {
            return;
        }

        const std::string remote_path = "/" + rel_path;
        const auto stat = try_stat64(session_.context(), remote_path);
        if (!stat.has_value()) {
            run_async_command(
                session_.context(),
                [&](AsyncCommandState* state) {
                    return nfs_mkdir2_async(session_.context(), remote_path.c_str(), 0755, generic_nfs_callback, state);
                },
                "nfs_mkdir2_async");
        } else if (!S_ISDIR(stat->nfs_mode)) {
            throw std::runtime_error("remote path exists but is not a directory: " + remote_path);
        }
        known_directories_.insert(rel_path);
    }

    struct nfsfh* open_handle(const std::string& rel_path, std::uint32_t mode) {
        auto it = open_handles_.find(rel_path);
        if (it != open_handles_.end()) {
            return it->second;
        }

        const std::string remote_path = "/" + rel_path;
        const AsyncCommandState open_state = run_async_command(
            session_.context(),
            [&](AsyncCommandState* state) {
                return nfs_create_async(session_.context(),
                                        remote_path.c_str(),
                                        O_TRUNC,
                                        static_cast<int>(mode),
                                        generic_nfs_callback,
                                        state);
            },
            "nfs_create_async");
        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        open_handles_.emplace(rel_path, handle);
        return handle;
    }

    std::string root_url_;
    LibNfsSession session_;
    std::unordered_map<std::string, struct nfsfh*> open_handles_;
    std::unordered_set<std::string> known_directories_;
};

#endif

}  // namespace

bool is_nfs_url(std::string_view path) {
    return path.rfind("nfs://", 0) == 0;
}

std::vector<std::string> expand_nfs_url_server_candidates(std::string_view root_url) {
    constexpr std::string_view kPrefix = "nfs://";
    if (root_url.rfind(kPrefix, 0) != 0) {
        return {std::string(root_url)};
    }

    const std::size_t server_begin = kPrefix.size();
    const std::size_t server_end = root_url.find('/', server_begin);
    if (server_end == std::string_view::npos || server_end == server_begin) {
        return {std::string(root_url)};
    }

    const std::string_view server_expression = root_url.substr(server_begin, server_end - server_begin);
    const std::vector<std::string> servers = expand_server_expression(server_expression);
    if (servers.size() == 1U && servers.front() == server_expression) {
        return {std::string(root_url)};
    }

    std::vector<std::string> urls;
    urls.reserve(servers.size());
    const std::string_view suffix = root_url.substr(server_end);
    for (const std::string& server : servers) {
        std::string url;
        url.reserve(kPrefix.size() + server.size() + suffix.size());
        url.append(kPrefix);
        url.append(server);
        url.append(suffix);
        urls.push_back(std::move(url));
    }
    return urls;
}

bool libnfs_support_enabled() {
#if HYPERSYNC_HAS_LIBNFS
    return true;
#else
    return false;
#endif
}

void NfsBackend::visit_files(bool recursive, const std::function<void(FileSpec)>& visitor) const {
    for (const auto& spec : list_files(recursive)) {
        visitor(spec);
    }
}

void NfsBackend::visit_metadata(bool recursive,
                                const std::function<void(FileSpec)>& file_visitor,
                                const std::function<void(FileSpec)>& directory_visitor) const {
    for (const auto& directory : list_directories(recursive)) {
        directory_visitor(directory);
    }
    visit_files(recursive, file_visitor);
}

void NfsBackend::visit_metadata_at(std::string_view rel_path,
                                   bool recursive,
                                   const std::function<void(FileSpec)>& file_visitor,
                                   const std::function<void(FileSpec)>& directory_visitor) const {
    const std::string normalized_path = normalize_path(rel_path);
    visit_metadata(recursive, [&](FileSpec spec) {
        if (normalized_path.empty() || spec.rel_path == normalized_path ||
            spec.rel_path.rfind(normalized_path + "/", 0) == 0) {
            file_visitor(std::move(spec));
        }
    }, [&](FileSpec spec) {
        if (normalized_path.empty() || spec.rel_path == normalized_path ||
            spec.rel_path.rfind(normalized_path + "/", 0) == 0) {
            directory_visitor(std::move(spec));
        }
    });
}

void NfsBackend::visit_folder(std::string_view rel_path,
                              const std::function<void(FileSpec)>& file_visitor,
                              const std::function<void(FileSpec)>& directory_visitor) const {
    visit_metadata_at(rel_path, false, file_visitor, directory_visitor);
}

void NfsBackend::scan_flat_folders(
    std::size_t outstanding_folders,
    const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
    const std::function<bool()>& should_stop,
    const std::function<void(FlatFolderScanBatch)>& folder_visitor) const {
    (void)outstanding_folders;
    while (!should_stop()) {
        std::optional<FileSpec> folder = folder_provider(true);
        if (!folder.has_value()) {
            return;
        }

        FlatFolderScanBatch batch;
        batch.folder = std::move(*folder);
        try {
            visit_metadata_at(batch.folder.rel_path,
                              false,
                              [&batch, &should_stop](FileSpec spec) {
                                  if (!should_stop()) {
                                      batch.files.push_back(std::move(spec));
                                  }
                              },
                              [&batch, &should_stop](FileSpec spec) {
                                  if (!should_stop()) {
                                      batch.directories.push_back(std::move(spec));
                                  }
                              });
        } catch (const std::exception& ex) {
            batch.failed = true;
            batch.error = ex.what();
        }
        folder_visitor(std::move(batch));
    }
}

FileSpec NfsBackend::load_file(std::string_view rel_path, std::size_t outstanding_requests) const {
    (void)outstanding_requests;
    return load_file(rel_path);
}

std::uint64_t NfsBackend::read_file_discard(std::string_view rel_path,
                                            std::uint64_t declared_size,
                                            std::size_t outstanding_requests) const {
    return read_file_discard(rel_path, declared_size, outstanding_requests, {});
}

std::uint64_t NfsBackend::read_file_discard(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    const std::function<void(std::uint64_t)>& bytes_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    const std::uint64_t bytes_read = file.content.size();
    if (bytes_read != 0 && bytes_visitor) {
        bytes_visitor(bytes_read);
    }
    return bytes_read;
}

std::uint64_t NfsBackend::read_file_stream(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    const std::function<void(std::string_view)>& data_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    if (!file.content.empty() && data_visitor) {
        data_visitor(file.content);
    }
    return file.content.size();
}

std::uint64_t NfsBackend::read_file_owned_chunks(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    const std::function<void(OwnedFileChunk&&)>& data_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    const std::uint64_t bytes_read = file.content.size();
    if (!file.content.empty() && data_visitor) {
        OwnedFileChunk chunk;
        chunk.offset = 0;
        chunk.data = std::move(file.content);
        data_visitor(std::move(chunk));
    }
    return bytes_read;
}

std::uint64_t NfsBackend::read_file_pooled_chunks(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    DataSlotPool& pool,
    const std::function<void(PooledFileChunk&&)>& data_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    std::uint64_t total = 0;
    while (total < file.content.size()) {
        const std::size_t chunk_size =
            std::min<std::size_t>(kLargeChunkBytes, file.content.size() - static_cast<std::size_t>(total));
        DataSlotHandle handle = pool.acquire_wait_or_throw(DataSlotClass::large, chunk_size);
        std::memcpy(pool.data(handle), file.content.data() + total, chunk_size);
        DataBufTrailer& trailer = pool.trailer(handle);
        trailer.data_offset = total;
        trailer.data_len = chunk_size;

        PooledFileChunk chunk;
        chunk.offset = total;
        chunk.handle = handle;
        total += chunk_size;
        if (data_visitor) {
            data_visitor(std::move(chunk));
        } else {
            pool.release(handle);
        }
    }
    return total;
}

std::uint64_t NfsBackend::read_file_raw_chunks(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    RawBufferPool& pool,
    const std::function<void(RawFileChunk&&)>& data_visitor,
    const std::function<bool()>& should_stop,
    bool copy_payload_to_buffer) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    std::uint64_t total = 0;
    while (total < file.content.size() && !(should_stop && should_stop())) {
        const std::size_t chunk_size =
            std::min<std::size_t>(kLargeChunkBytes, file.content.size() - static_cast<std::size_t>(total));
        BufferHandle handle = pool.acquire_spin();
        DataBuffer& buffer = data_buffer(pool, handle);
        if (copy_payload_to_buffer) {
            std::memcpy(buffer.bytes.data(), file.content.data() + total, chunk_size);
        }
        buffer.trailer = {};
        buffer.trailer.data_offset = total;
        buffer.trailer.data_len = chunk_size;

        RawFileChunk chunk;
        chunk.offset = total;
        chunk.handle = handle;
        total += chunk_size;
        if (data_visitor) {
            data_visitor(std::move(chunk));
        } else {
            pool.release(handle);
        }
    }
    return total;
}

std::uint64_t NfsBackend::visit_file_chunks(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    if (!file.content.empty() && data_visitor) {
        data_visitor(0, file.content);
    }
    return file.content.size();
}

std::unique_ptr<NfsBackend> make_nfs_backend(std::string root) {
    if (is_nfs_url(root)) {
#if HYPERSYNC_HAS_LIBNFS
        return std::make_unique<LibNfsBackend>(std::move(root));
#else
        throw std::runtime_error("libnfs support is not available in this build; install libnfs and rebuild");
#endif
    }
    return std::make_unique<LocalFilesystemBackend>(std::move(root));
}

std::unique_ptr<TargetWriterBackend> make_target_writer_backend(std::string root) {
    if (is_nfs_url(root)) {
#if HYPERSYNC_HAS_LIBNFS
        return std::make_unique<LibNfsTargetWriterBackend>(std::move(root));
#else
        throw std::runtime_error("libnfs support is not available in this build; install libnfs and rebuild");
#endif
    }
    return std::make_unique<LocalTargetWriterBackend>(std::move(root));
}

}  // namespace hypersync
