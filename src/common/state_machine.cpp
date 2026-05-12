#include "common/state_machine.hpp"

#include <stdexcept>

namespace hypersync {

bool can_transition_file(EndpointRole role, FileState from, FileState to) {
    if (from == to) {
        return true;
    }

    if (role == EndpointRole::sender) {
        switch (from) {
            case FileState::pending:
                return to == FileState::checking || to == FileState::reading;
            case FileState::checking:
                return to == FileState::skipped || to == FileState::reading;
            case FileState::skipped:
                return to == FileState::done;
            case FileState::reading:
                return to == FileState::transferring || to == FileState::done || to == FileState::failed;
            case FileState::transferring:
                return to == FileState::done || to == FileState::failed;
            case FileState::failed:
                return to == FileState::reading;
            case FileState::done:
            case FileState::receiving:
            case FileState::writing:
                return false;
        }
    }

    switch (from) {
        case FileState::pending:
            return to == FileState::checking || to == FileState::receiving;
        case FileState::checking:
            return to == FileState::receiving || to == FileState::done;
        case FileState::receiving:
            return to == FileState::writing || to == FileState::failed;
        case FileState::writing:
            return to == FileState::done || to == FileState::failed;
        case FileState::failed:
            return to == FileState::receiving;
        case FileState::done:
        case FileState::skipped:
        case FileState::reading:
        case FileState::transferring:
            return false;
    }
    return false;
}

bool can_transition_folder(EndpointRole role, FolderState from, FolderState to) {
    if (from == to) {
        return true;
    }

    if (role == EndpointRole::sender) {
        switch (from) {
            case FolderState::pending:
                return to == FolderState::reading;
            case FolderState::reading:
                return to == FolderState::transferring || to == FolderState::awaiting_ack || to == FolderState::done;
            case FolderState::transferring:
                return to == FolderState::awaiting_ack;
            case FolderState::awaiting_ack:
                return to == FolderState::done;
            case FolderState::done:
            case FolderState::receiving:
            case FolderState::writing:
                return false;
        }
    }

    switch (from) {
        case FolderState::pending:
            return to == FolderState::receiving;
        case FolderState::receiving:
            return to == FolderState::writing || to == FolderState::done;
        case FolderState::writing:
            return to == FolderState::done;
        case FolderState::done:
        case FolderState::reading:
        case FolderState::transferring:
        case FolderState::awaiting_ack:
            return false;
    }
    return false;
}

void transition_file(RecBuf& record, EndpointRole role, FileState to) {
    if (!can_transition_file(role, record.state, to)) {
        throw std::logic_error("invalid file transition for " + to_string(role) + ": " +
                               to_string(record.state) + " -> " + to_string(to));
    }
    record.state = to;
}

void transition_folder(FolderRecord& record, EndpointRole role, FolderState to) {
    FolderState& state = (role == EndpointRole::sender) ? record.state : record.remote_state;
    if (!can_transition_folder(role, state, to)) {
        throw std::logic_error("invalid folder transition for " + to_string(role) + ": " +
                               to_string(state) + " -> " + to_string(to));
    }
    state = to;
}

}  // namespace hypersync
