#include "jobs/input_provider/input_provider.hpp"

#include <utility>

#include "common/config.hpp"

namespace hypersync {

InputProviderConfig::InputProviderConfig()
    : InputProviderConfig(load_input_provider_config(ConfigStore{})) {}

InputProviderConfig::InputProviderConfig(std::size_t max_queue_entries,
                                         std::size_t refill_threshold,
                                         std::size_t refill_batch_size,
                                         std::string input_csv_path,
                                         std::string overflow_csv_path)
    : max_queue_entries(max_queue_entries),
      refill_threshold(refill_threshold),
      refill_batch_size(refill_batch_size),
      input_csv_path(std::move(input_csv_path)),
      overflow_csv_path(std::move(overflow_csv_path)) {}

InputProviderConfig load_input_provider_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("input_provider"));
    return InputProviderConfig(config_size_t(values, "max_queue_entries"),
                               config_size_t(values, "refill_threshold"),
                               config_size_t(values, "refill_batch_size"),
                               config_string(values, "input_csv_path"),
                               config_string(values, "overflow_csv_path"));
}

InputProvider::InputProvider(InputProviderConfig config)
    : config_(std::move(config)) {}

bool InputProvider::submit_folder(FolderRecord folder) {
    if (ready_.size() >= config_.max_queue_entries) {
        overflow_.push_back(std::move(folder));
        return false;
    }
    ready_.push_back(std::move(folder));
    ++accepted_;
    return true;
}

void InputProvider::submit_folders(const std::vector<FolderRecord>& folders) {
    for (const auto& folder : folders) {
        (void)submit_folder(folder);
    }
}

std::optional<FolderRecord> InputProvider::take_folder() {
    if (ready_.empty()) {
        return std::nullopt;
    }
    FolderRecord folder = std::move(ready_.front());
    ready_.erase(ready_.begin());
    return folder;
}

const InputProviderConfig& InputProvider::config() const {
    return config_;
}

std::size_t InputProvider::accepted_entries() const {
    return accepted_;
}

std::size_t InputProvider::overflow_entries() const {
    return overflow_.size();
}

std::size_t InputProvider::ready_entries() const {
    return ready_.size();
}

const std::vector<FolderRecord>& InputProvider::overflow_queue() const {
    return overflow_;
}

}  // namespace hypersync
