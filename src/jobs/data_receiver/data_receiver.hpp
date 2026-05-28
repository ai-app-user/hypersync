#ifndef HYPERSYNC_JOBS_DATA_RECEIVER_HPP
#define HYPERSYNC_JOBS_DATA_RECEIVER_HPP

#include <cstddef>

#include "common/types.hpp"

namespace hypersync {

class ConfigStore;

struct DataReceiverConfig {
    std::size_t connection_count;
    double refill_below_percent;

    DataReceiverConfig();
    DataReceiverConfig(std::size_t connection_count, double refill_below_percent);
};

[[nodiscard]] DataReceiverConfig load_data_receiver_config(const ConfigStore& config);

class DataReceiver {
public:
    explicit DataReceiver(DataReceiverConfig config = {});

    [[nodiscard]] bool should_refill(double pool_usage_percent) const;
    [[nodiscard]] const DataReceiverConfig& config() const;

private:
    DataReceiverConfig config_;
};

}  // namespace hypersync

#endif
