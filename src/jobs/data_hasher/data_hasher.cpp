#include "jobs/data_hasher/data_hasher.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "common/config.hpp"
#include "common/hash_utils.hpp"
#include "core/data_buffer_codec.hpp"
#include "core/pipeline_buffers.hpp"

namespace hypersync {
namespace {

struct PayloadView {
    char* data = nullptr;
    std::size_t size = 0;
    DataBufTrailer* trailer = nullptr;
};

[[nodiscard]] PayloadView payload_view(RawBufferPool& pool, const BufferHandle& handle) {
    if (handle.pool_id == kDataBufferPoolId) {
        DataBuffer& buffer = data_buffer(pool, handle);
        const std::size_t data_len = std::min<std::size_t>(buffer.bytes.size(),
                                                           static_cast<std::size_t>(buffer.trailer.data_len));
        return PayloadView{reinterpret_cast<char*>(buffer.bytes.data()), data_len, &buffer.trailer};
    }

    return PayloadView{reinterpret_cast<char*>(pool.data(handle)), pool.buffer_size_bytes(), nullptr};
}

[[nodiscard]] std::uint64_t mix_hash_marker(std::uint64_t seed, std::uint64_t value) {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
    return seed;
}

[[nodiscard]] std::uint64_t hash_payload_once(ContentHashAlgorithm algorithm, std::string_view data) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5: {
            Md5State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            std::uint64_t marker = 0;
            std::memcpy(&marker, digest.data(), sizeof(marker));
            return marker;
        }
        case ContentHashAlgorithm::sha256: {
            Sha256State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            std::uint64_t marker = 0;
            std::memcpy(&marker, digest.data(), sizeof(marker));
            return marker;
        }
        case ContentHashAlgorithm::xxh64: {
            Hash64State hasher;
            hasher.update(data);
            return hasher.value();
        }
        case ContentHashAlgorithm::xxh3_64: {
            Xxh3_64State hasher;
            hasher.update(data);
            return hasher.value();
        }
        case ContentHashAlgorithm::xxh3_128: {
            Xxh3_128State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            std::uint64_t marker = 0;
            std::memcpy(&marker, digest.data(), sizeof(marker));
            return marker;
        }
    }
    throw std::logic_error("unknown data hasher algorithm");
}

[[nodiscard]] std::uint64_t hash_payload(ContentHashAlgorithm algorithm,
                                         std::string_view data,
                                         std::size_t work_factor) {
    std::uint64_t marker = 0;
    // The work factor deliberately burns CPU inside the hasher job without
    // changing queue ownership or byte accounting for upstream/downstream jobs.
    for (std::size_t round = 0; round < work_factor; ++round) {
        marker = mix_hash_marker(marker, hash_payload_once(algorithm, data) ^ round);
    }
    return marker;
}

}  // namespace

DataHasherConfig::DataHasherConfig()
    : DataHasherConfig(load_data_hasher_config(ConfigStore{})) {}

DataHasherConfig::DataHasherConfig(std::size_t worker_count,
                                   ContentHashAlgorithm algorithm,
                                   std::size_t work_factor)
    : worker_count(worker_count),
      algorithm(algorithm),
      work_factor(work_factor) {
    if (this->worker_count == 0U) {
        throw std::invalid_argument("data hasher worker count must be positive");
    }
    if (this->work_factor == 0U) {
        throw std::invalid_argument("data hasher work factor must be positive");
    }
}

DataHasherConfig load_data_hasher_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("data_hasher"));
    return DataHasherConfig(config_size_t_or(values, "worker_count", 1),
                            parse_content_hash_algorithm(config_string_or(values, "algorithm", "xxh64")),
                            config_size_t_or(values, "work_factor", 1));
}

DataHasherJob::DataHasherJob(DataHasherConfig config,
                             BufQueue& input,
                             BufQueue& output,
                             const BufferPoolRegistry& registry)
    : BufferTransformJob(config.worker_count, input, output, registry),
      config_(std::move(config)) {}

DataHasherJob::~DataHasherJob() {
    stop();
}

DataHasherStats DataHasherJob::stats() const {
    const BufferTransformStats transformed = transform_stats();
    DataHasherStats result;
    result.running = running();
    result.worker_count = config_.worker_count;
    result.algorithm = to_string(config_.algorithm);
    result.work_factor = config_.work_factor;
    result.buffers_hashed = transformed.buffers_transformed;
    result.bytes_hashed = transformed.bytes_transformed;
    result.digest_marker = digest_marker_.load(std::memory_order_acquire);
    return result;
}

std::uint64_t DataHasherJob::process_buffer(const BufferHandle& handle, RawBufferPool& pool) {
    if (handle.pool_id == kDataBufferPoolId) {
        DataBuffer& buffer = data_buffer(pool, handle);
        if (is_packed_small_file_buffer(buffer)) {
            std::uint64_t marker = 0;
            std::uint64_t bytes_hashed = 0;
            const bool ok = visit_packed_small_files(buffer, [&](const PackedSmallFileView& file) {
                const std::uint64_t file_marker = hash_payload(config_.algorithm, file.data, config_.work_factor);
                marker = mix_hash_marker(marker, file_marker ^ file.file_id);
                bytes_hashed += file.data.size();
            });
            if (!ok) {
                throw std::runtime_error("malformed packed small-file data buffer");
            }

            digest_marker_.fetch_xor(marker, std::memory_order_relaxed);
            buffer.trailer.chunk_hash = static_cast<std::uint32_t>(marker ^ (marker >> 32U));
            buffer.trailer.flags |= kFlagHashValid;
            return bytes_hashed;
        }
    }

    PayloadView payload = payload_view(pool, handle);
    const std::string_view data(payload.data, payload.size);
    const std::uint64_t marker = hash_payload(config_.algorithm, data, config_.work_factor);
    digest_marker_.fetch_xor(marker, std::memory_order_relaxed);

    // For true data buffers, preserve a cheap per-chunk marker for downstream
    // diagnostics without copying or allocating digest strings on the hot path.
    if (payload.trailer != nullptr) {
        payload.trailer->chunk_hash = static_cast<std::uint32_t>(marker ^ (marker >> 32U));
        payload.trailer->flags |= kFlagHashValid;
    }

    return payload.size;
}

}  // namespace hypersync
