#ifndef HYPERSYNC_JOBS_INPUT_PROVIDER_HPP
#define HYPERSYNC_JOBS_INPUT_PROVIDER_HPP

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "common/types.hpp"

namespace hypersync {

class ConfigStore;

struct InputProviderConfig {
    std::size_t max_queue_entries;
    std::size_t refill_threshold;
    std::size_t refill_batch_size;
    std::string input_csv_path;
    std::string overflow_csv_path;

    InputProviderConfig();
    InputProviderConfig(std::size_t max_queue_entries,
                        std::size_t refill_threshold,
                        std::size_t refill_batch_size,
                        std::string input_csv_path,
                        std::string overflow_csv_path);
};

[[nodiscard]] InputProviderConfig load_input_provider_config(const ConfigStore& config);

class InputProvider {
public:
    explicit InputProvider(InputProviderConfig config = {});

    [[nodiscard]] bool submit_folder(FolderRecord folder);
    void submit_folders(const std::vector<FolderRecord>& folders);
    [[nodiscard]] std::optional<FolderRecord> take_folder();

    [[nodiscard]] const InputProviderConfig& config() const;
    [[nodiscard]] std::size_t accepted_entries() const;
    [[nodiscard]] std::size_t overflow_entries() const;
    [[nodiscard]] std::size_t ready_entries() const;
    [[nodiscard]] const std::vector<FolderRecord>& overflow_queue() const;

private:
    InputProviderConfig config_;
    std::vector<FolderRecord> ready_;
    std::vector<FolderRecord> overflow_;
    std::size_t accepted_ = 0;
};

}  // namespace hypersync

#endif
