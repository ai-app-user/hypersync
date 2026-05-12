#include "jobs/data_sender/data_sender.hpp"

#include "common/config.hpp"

namespace hypersync {

DataSenderConfig::DataSenderConfig()
    : DataSenderConfig(load_data_sender_config(ConfigStore{})) {}

DataSenderConfig::DataSenderConfig(std::size_t connection_count, std::uint64_t socket_buffer_bytes, bool zero_copy)
    : connection_count(connection_count),
      socket_buffer_bytes(socket_buffer_bytes),
      zero_copy(zero_copy) {}

DataSenderConfig load_data_sender_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("data_sender"));
    return DataSenderConfig(config_size_t(values, "connection_count"),
                            config_u64(values, "socket_buffer_bytes"),
                            config_bool(values, "zero_copy"));
}

DataSender::DataSender(DataSenderConfig config)
    : TypedQueueJob("data_sender", message_kinds::data_chunk), config_(std::move(config)) {}

void DataSender::queue_chunk(DataChunk chunk) {
    publish_item(std::move(chunk));
}

std::size_t DataSender::dispatch_connection(const DataChunk& chunk) const {
    if (config_.connection_count == 0) {
        return 0;
    }
    return static_cast<std::size_t>(chunk.trailer.file_id % config_.connection_count);
}

const DataSenderConfig& DataSender::config() const {
    return config_;
}

}  // namespace hypersync
