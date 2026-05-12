#include "common/types.hpp"

#include <stdexcept>
#include <utility>

#include "common/config.hpp"

namespace hypersync {

namespace {

Mode parse_mode(const std::string& value) {
    if (value == "transfer") {
        return Mode::transfer;
    }
    if (value == "scan") {
        return Mode::scan;
    }
    if (value == "dry-run" || value == "dry_run") {
        return Mode::dry_run;
    }
    throw std::runtime_error("invalid mode in config: " + value);
}

}  // namespace

EngineConfig::EngineConfig()
    : EngineConfig(load_engine_config(ConfigStore{})) {}

EngineConfig::EngineConfig(Mode mode,
                           std::size_t small_file_threshold,
                           std::size_t large_chunk_bytes,
                           std::size_t small_pool_slots,
                           std::size_t large_pool_slots,
                           std::size_t max_retries,
                           bool skip_verify,
                           std::unordered_map<std::string, std::size_t> forced_failures)
    : mode(mode),
      small_file_threshold(small_file_threshold),
      large_chunk_bytes(large_chunk_bytes),
      small_pool_slots(small_pool_slots),
      large_pool_slots(large_pool_slots),
      max_retries(max_retries),
      skip_verify(skip_verify),
      forced_failures(std::move(forced_failures)) {}

EngineConfig load_engine_config(const ConfigStore& config) {
    const ConfigSection values = config.section("engine");
    EngineConfig resolved(parse_mode(config_string(values, "mode")),
                          config_size_t(values, "small_file_threshold"),
                          config_size_t(values, "large_chunk_bytes"),
                          config_size_t(values, "small_pool_slots"),
                          config_size_t(values, "large_pool_slots"),
                          config_size_t(values, "max_retries"),
                          config_bool(values, "skip_verify"));
    return resolved;
}

std::string to_string(Mode mode) {
    switch (mode) {
        case Mode::transfer:
            return "transfer";
        case Mode::scan:
            return "scan";
        case Mode::dry_run:
            return "dry-run";
    }
    return "unknown";
}

std::string to_string(EndpointRole role) {
    switch (role) {
        case EndpointRole::sender:
            return "sender";
        case EndpointRole::receiver:
            return "receiver";
    }
    return "unknown";
}

std::string to_string(FileState state) {
    switch (state) {
        case FileState::pending:
            return "pending";
        case FileState::checking:
            return "checking";
        case FileState::skipped:
            return "skipped";
        case FileState::reading:
            return "reading";
        case FileState::transferring:
            return "transferring";
        case FileState::receiving:
            return "receiving";
        case FileState::writing:
            return "writing";
        case FileState::failed:
            return "failed";
        case FileState::done:
            return "done";
    }
    return "unknown";
}

std::string to_string(FolderState state) {
    switch (state) {
        case FolderState::pending:
            return "pending";
        case FolderState::reading:
            return "reading";
        case FolderState::transferring:
            return "transferring";
        case FolderState::awaiting_ack:
            return "awaiting_ack";
        case FolderState::receiving:
            return "receiving";
        case FolderState::writing:
            return "writing";
        case FolderState::done:
            return "done";
    }
    return "unknown";
}

std::string to_string(DiffKind diff) {
    switch (diff) {
        case DiffKind::skip:
            return "skip";
        case DiffKind::new_file:
            return "new";
        case DiffKind::changed:
            return "changed";
        case DiffKind::failed:
            return "failed";
    }
    return "unknown";
}

}  // namespace hypersync
