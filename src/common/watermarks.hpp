#ifndef HYPERSYNC_COMMON_WATERMARKS_HPP
#define HYPERSYNC_COMMON_WATERMARKS_HPP

#include "common/types.hpp"

namespace hypersync {

WatermarkStatus evaluate_watermarks(double ram_usage_percent, double nvme_usage_percent);

}  // namespace hypersync

#endif
