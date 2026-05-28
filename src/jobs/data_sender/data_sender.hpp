#ifndef HYPERSYNC_JOBS_DATA_SENDER_HPP
#define HYPERSYNC_JOBS_DATA_SENDER_HPP

#include <cstddef>
#include <cstdint>

#include "common/types.hpp"

namespace hypersync {

class ConfigStore;

struct DataSenderConfig {
    std::size_t connection_count;
    std::uint64_t socket_buffer_bytes;
    bool zero_copy;

    DataSenderConfig();
    DataSenderConfig(std::size_t connection_count, std::uint64_t socket_buffer_bytes, bool zero_copy);
};

[[nodiscard]] DataSenderConfig load_data_sender_config(const ConfigStore& config);

class DataSender {
public:
    explicit DataSender(DataSenderConfig config = {});

    [[nodiscard]] std::size_t dispatch_connection(const DataChunk& chunk) const;
    [[nodiscard]] const DataSenderConfig& config() const;

private:
    DataSenderConfig config_;
};

}  // namespace hypersync

#endif
