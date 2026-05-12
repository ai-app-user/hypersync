#include "common/watermarks.hpp"

#include <stdexcept>

namespace hypersync {

WatermarkStatus evaluate_watermarks(double ram_usage_percent, double nvme_usage_percent) {
    if (ram_usage_percent < 0.0 || ram_usage_percent > 100.0 ||
        nvme_usage_percent < 0.0 || nvme_usage_percent > 100.0) {
        throw std::invalid_argument("watermark percentages must be in [0, 100]");
    }

    WatermarkStatus status;
    if (ram_usage_percent >= 70.0) {
        status.soft_throttle = true;
    }
    if (ram_usage_percent > 85.0) {
        status.spill_to_nvme = true;
    }
    if (ram_usage_percent > 95.0) {
        status.hard_stop = true;
    }
    if (nvme_usage_percent < 20.0) {
        status.prefetch_to_ram = true;
    }
    if (nvme_usage_percent > 80.0) {
        status.pause_reader = true;
    }
    if (nvme_usage_percent > 95.0) {
        status.hard_stop = true;
    }
    return status;
}

}  // namespace hypersync
