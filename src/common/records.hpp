#ifndef HYPERSYNC_COMMON_RECORDS_HPP
#define HYPERSYNC_COMMON_RECORDS_HPP

#include <cstdint>

#include "common/types.hpp"

namespace hypersync {

RecBuf make_recbuf(const FileSpec& file);
FileSnapshot make_snapshot(const FileSpec& file, std::uint64_t data_hash, char scan_side);

}  // namespace hypersync

#endif
