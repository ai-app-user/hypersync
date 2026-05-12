#ifndef HYPERSYNC_COMMON_STATE_MACHINE_HPP
#define HYPERSYNC_COMMON_STATE_MACHINE_HPP

#include "common/types.hpp"

namespace hypersync {

bool can_transition_file(EndpointRole role, FileState from, FileState to);
bool can_transition_folder(EndpointRole role, FolderState from, FolderState to);
void transition_file(RecBuf& record, EndpointRole role, FileState to);
void transition_folder(FolderRecord& record, EndpointRole role, FolderState to);

}  // namespace hypersync

#endif
