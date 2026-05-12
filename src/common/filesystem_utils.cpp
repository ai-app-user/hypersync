#include "common/filesystem_utils.hpp"

#include <array>
#include <cerrno>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"
#include "common/privilege_utils.hpp"

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

FileSpec spec_from_path(const std::filesystem::path& root, const std::filesystem::path& path) {
    struct stat info {};
    if (::stat(path.c_str(), &info) != 0) {
        throw std::system_error(errno, std::generic_category(), "stat failed for " + path.string());
    }

    const std::filesystem::path rel = std::filesystem::relative(path, root);
    FileSpec spec;
    spec.rel_path = normalize_path(rel.generic_string());
    spec.mtime = stat_mtime_ns(info);
    spec.mode = static_cast<std::uint32_t>(info.st_mode & 0777U);
    spec.uid = static_cast<std::uint32_t>(info.st_uid);
    spec.gid = static_cast<std::uint32_t>(info.st_gid);
    spec.declared_size = static_cast<std::uint64_t>(info.st_size);
    return spec;
}

std::vector<FileSpec> collect_specs_matching(const std::filesystem::path& source_root,
                                             bool recursive,
                                             bool want_directories) {
    if (!std::filesystem::exists(source_root)) {
        throw std::runtime_error("source root does not exist: " + source_root.string());
    }
    if (!std::filesystem::is_directory(source_root)) {
        throw std::runtime_error("source root is not a directory: " + source_root.string());
    }

    std::vector<FileSpec> specs;
    if (recursive) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(source_root)) {
            if (entry.is_directory() != want_directories) {
                continue;
            }
            if (want_directories || entry.is_regular_file()) {
                specs.push_back(spec_from_path(source_root, entry.path()));
            }
        }
        return specs;
    }

    for (const auto& entry : std::filesystem::directory_iterator(source_root)) {
        if (entry.is_directory() != want_directories) {
            continue;
        }
        if (want_directories || entry.is_regular_file()) {
            specs.push_back(spec_from_path(source_root, entry.path()));
        }
    }
    return specs;
}

void apply_path_metadata(const std::filesystem::path& path, const FileSpec& spec, std::string_view entry_kind) {
    struct stat info {};
    if (::stat(path.c_str(), &info) != 0) {
        throw std::system_error(errno, std::generic_category(), "stat failed for " + path.string());
    }

    if (static_cast<std::uint32_t>(info.st_uid) != spec.uid || static_cast<std::uint32_t>(info.st_gid) != spec.gid) {
        require_root_for_owner_change(spec.rel_path,
                                      entry_kind,
                                      static_cast<std::uint32_t>(info.st_uid),
                                      static_cast<std::uint32_t>(info.st_gid),
                                      spec.uid,
                                      spec.gid);
        if (::chown(path.c_str(), static_cast<uid_t>(spec.uid), static_cast<gid_t>(spec.gid)) != 0) {
            throw std::system_error(errno, std::generic_category(), "chown failed for " + path.string());
        }
    }

    if (::chmod(path.c_str(), static_cast<mode_t>(spec.mode)) != 0) {
        throw std::system_error(errno, std::generic_category(), "chmod failed for " + path.string());
    }

    struct timespec times[2];
    times[0].tv_sec = static_cast<time_t>(spec.mtime / 1'000'000'000ULL);
    times[0].tv_nsec = static_cast<long>(spec.mtime % 1'000'000'000ULL);
    times[1] = times[0];
    if (::utimensat(AT_FDCWD, path.c_str(), times, 0) != 0) {
        throw std::system_error(errno, std::generic_category(), "utimensat failed for " + path.string());
    }
}

}  // namespace

std::vector<FileSpec> collect_file_specs(const std::filesystem::path& source_root, bool recursive) {
    return collect_specs_matching(source_root, recursive, false);
}

std::vector<FileSpec> collect_directory_specs(const std::filesystem::path& source_root, bool recursive) {
    return collect_specs_matching(source_root, recursive, true);
}

std::string read_file_contents(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open file for read: " + path.string());
    }
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

std::uint64_t file_hash64(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open file for hashing: " + path.string());
    }

    Hash64State hasher;
    std::array<char, 64 * 1024> buffer {};
    while (input) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const std::streamsize read_count = input.gcount();
        if (read_count > 0) {
            hasher.update(std::string_view(buffer.data(), static_cast<std::size_t>(read_count)));
        }
    }

    if (!input.eof()) {
        throw std::runtime_error("failed while hashing file: " + path.string());
    }
    return hasher.value();
}

void ensure_parent_directories(const std::filesystem::path& path) {
    const auto parent = path.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
}

void apply_file_metadata(const std::filesystem::path& path, const FileSpec& spec) {
    apply_path_metadata(path, spec, "file");
}

void apply_directory_metadata(const std::filesystem::path& path, const FileSpec& spec) {
    apply_path_metadata(path, spec, "directory");
}

}  // namespace hypersync
