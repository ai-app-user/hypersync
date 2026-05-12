#ifndef HYPERSYNC_COMMON_PRIVILEGE_UTILS_HPP
#define HYPERSYNC_COMMON_PRIVILEGE_UTILS_HPP

#include <cstdint>
#include <string_view>

namespace hypersync {

[[nodiscard]] bool running_as_root();

void require_root_for_owner_change(std::string_view rel_path,
                                   std::string_view entry_kind,
                                   std::uint32_t current_uid,
                                   std::uint32_t current_gid,
                                   std::uint32_t requested_uid,
                                   std::uint32_t requested_gid);

}  // namespace hypersync

#endif
