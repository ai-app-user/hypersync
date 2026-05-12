#include "common/path_utils.hpp"

namespace hypersync {

std::string normalize_path(std::string_view path) {
    std::string out;
    out.reserve(path.size());
    bool last_was_slash = false;
    for (char ch : path) {
        if (ch == '/') {
            if (!out.empty() && !last_was_slash) {
                out.push_back('/');
            }
            last_was_slash = true;
            continue;
        }
        out.push_back(ch);
        last_was_slash = false;
    }
    while (!out.empty() && out.back() == '/') {
        out.pop_back();
    }
    return out;
}

std::string parent_path(std::string_view path) {
    const std::string normalized = normalize_path(path);
    const auto pos = normalized.rfind('/');
    if (pos == std::string::npos) {
        return "";
    }
    return normalized.substr(0, pos);
}

std::string base_name(std::string_view path) {
    const std::string normalized = normalize_path(path);
    const auto pos = normalized.rfind('/');
    if (pos == std::string::npos) {
        return normalized;
    }
    return normalized.substr(pos + 1);
}

}  // namespace hypersync
