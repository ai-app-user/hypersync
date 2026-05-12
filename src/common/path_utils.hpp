#ifndef HYPERSYNC_COMMON_PATH_UTILS_HPP
#define HYPERSYNC_COMMON_PATH_UTILS_HPP

#include <string>
#include <string_view>

namespace hypersync {

std::string normalize_path(std::string_view path);
std::string parent_path(std::string_view path);
std::string base_name(std::string_view path);

}  // namespace hypersync

#endif
