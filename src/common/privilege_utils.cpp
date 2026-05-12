#include "common/privilege_utils.hpp"

#include <sstream>
#include <stdexcept>
#include <string>

#include <sys/types.h>
#include <unistd.h>

namespace hypersync {

bool running_as_root() {
    return ::geteuid() == 0;
}

void require_root_for_owner_change(std::string_view rel_path,
                                   std::string_view entry_kind,
                                   std::uint32_t current_uid,
                                   std::uint32_t current_gid,
                                   std::uint32_t requested_uid,
                                   std::uint32_t requested_gid) {
    if (running_as_root()) {
        return;
    }

    std::ostringstream message;
    message << "receiver must run as root to restore " << entry_kind << " owner/group for "
            << (rel_path.empty() ? "<root>" : std::string(rel_path))
            << " (current uid=" << current_uid
            << ", gid=" << current_gid
            << ", requested uid=" << requested_uid
            << ", gid=" << requested_gid
            << ", receiver euid=" << static_cast<std::uint32_t>(::geteuid())
            << ", egid=" << static_cast<std::uint32_t>(::getegid())
            << "); run `sudo hypersync receive ...`";
    throw std::runtime_error(message.str());
}

}  // namespace hypersync
