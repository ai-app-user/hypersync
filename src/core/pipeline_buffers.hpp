#ifndef HYPERSYNC_CORE_PIPELINE_BUFFERS_HPP
#define HYPERSYNC_CORE_PIPELINE_BUFFERS_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "common/buffer_pool.hpp"
#include "common/types.hpp"

namespace hypersync {

inline constexpr BufferPoolId kMetadataBufferPoolId = 1;
inline constexpr BufferPoolId kDataBufferPoolId = 2;
inline constexpr BufferPoolId kMetadataBatchBufferPoolId = 3;
inline constexpr std::size_t kMetadataBufferBytes = 4096;
inline constexpr std::size_t kMetadataBatchBufferBytes = kLargeChunkBytes;

enum class MetadataBufferRecordKind : std::uint32_t {
    empty = 0,
    folder = 1,
    file = 2,
    folder_complete = 3,
    ack = 4,
    error = 5,
    scan_record = 6,
};

struct MetadataBuffer {
    MetadataBufferRecordKind record_kind = MetadataBufferRecordKind::empty;
    std::uint32_t bytes_used = 0;
    std::uint64_t file_id = 0;
    std::uint64_t folder_hash = 0;
    std::array<std::byte, kMetadataBufferBytes> bytes {};
};

struct DataBuffer {
    std::array<std::byte, kLargeChunkBytes> bytes {};
    DataBufTrailer trailer;
};

struct MetadataBatchBuffer {
    std::uint32_t bytes_used = 0;
    std::uint32_t record_count = 0;
    std::array<std::byte, kMetadataBatchBufferBytes> bytes {};
};

static_assert(std::is_trivially_destructible_v<MetadataBuffer>);
static_assert(std::is_trivially_destructible_v<DataBuffer>);
static_assert(std::is_trivially_destructible_v<MetadataBatchBuffer>);

[[nodiscard]] inline RawBufferPool make_metadata_buffer_pool(std::size_t capacity) {
    return RawBufferPool(kMetadataBufferPoolId, capacity, sizeof(MetadataBuffer), alignof(MetadataBuffer));
}

[[nodiscard]] inline RawBufferPool make_data_buffer_pool(std::size_t capacity) {
    return RawBufferPool(kDataBufferPoolId, capacity, sizeof(DataBuffer), alignof(DataBuffer));
}

[[nodiscard]] inline RawBufferPool make_metadata_batch_buffer_pool(std::size_t capacity) {
    return RawBufferPool(kMetadataBatchBufferPoolId, capacity, sizeof(MetadataBatchBuffer), alignof(MetadataBatchBuffer));
}

[[nodiscard]] inline MetadataBuffer& metadata_buffer(RawBufferPool& pool, const BufferHandle& handle) {
    return buffer_as<MetadataBuffer>(pool, handle);
}

[[nodiscard]] inline const MetadataBuffer& metadata_buffer(const RawBufferPool& pool, const BufferHandle& handle) {
    return buffer_as<MetadataBuffer>(pool, handle);
}

[[nodiscard]] inline DataBuffer& data_buffer(RawBufferPool& pool, const BufferHandle& handle) {
    return buffer_as<DataBuffer>(pool, handle);
}

[[nodiscard]] inline const DataBuffer& data_buffer(const RawBufferPool& pool, const BufferHandle& handle) {
    return buffer_as<DataBuffer>(pool, handle);
}

[[nodiscard]] inline MetadataBatchBuffer& metadata_batch_buffer(RawBufferPool& pool, const BufferHandle& handle) {
    return buffer_as<MetadataBatchBuffer>(pool, handle);
}

[[nodiscard]] inline const MetadataBatchBuffer& metadata_batch_buffer(const RawBufferPool& pool, const BufferHandle& handle) {
    return buffer_as<MetadataBatchBuffer>(pool, handle);
}

}  // namespace hypersync

#endif
