#include "jobs/data_receiver/data_receiver.hpp"

#include "common/config.hpp"

namespace hypersync {

DataReceiverConfig::DataReceiverConfig()
    : DataReceiverConfig(load_data_receiver_config(ConfigStore{})) {}

DataReceiverConfig::DataReceiverConfig(std::size_t connection_count, double refill_below_percent)
    : connection_count(connection_count),
      refill_below_percent(refill_below_percent) {}

DataReceiverConfig load_data_receiver_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("data_receiver"));
    return DataReceiverConfig(config_size_t(values, "connection_count"),
                              config_double(values, "refill_below_percent"));
}

DataReceiver::DataReceiver(DataReceiverConfig config)
    : TypedQueueJob("data_receiver", message_kinds::data_chunk), config_(std::move(config)) {}

void DataReceiver::receive_chunk(DataChunk chunk) {
    publish_item(std::move(chunk));
}

bool DataReceiver::should_refill(double pool_usage_percent) const {
    return pool_usage_percent < config_.refill_below_percent;
}

const DataReceiverConfig& DataReceiver::config() const {
    return config_;
}

}  // namespace hypersync
