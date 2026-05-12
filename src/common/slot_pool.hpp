#ifndef HYPERSYNC_COMMON_SLOT_POOL_HPP
#define HYPERSYNC_COMMON_SLOT_POOL_HPP

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string_view>

#include "common/buffer_pool.hpp"
#include "common/types.hpp"

namespace hypersync {

enum class DataSlotClass {
    small,
    large,
};

struct DataSlotHandle {
    DataSlotClass slot_class = DataSlotClass::small;
    std::uint32_t slot_index = 0;
    BufferHandle buffer_handle {};
    std::uint64_t entry_id = 0;
    bool cached = false;
};

template <std::size_t DataBytes>
struct DataSlot {
    std::array<char, DataBytes> data {};
    DataBufTrailer trailer;
};

class DataSlotPool {
public:
    using SmallSlot = DataSlot<kSmallFileThreshold>;
    using LargeSlot = DataSlot<kLargeChunkBytes>;
    static constexpr BufferPoolId kSmallSlotPoolId = 101;
    static constexpr BufferPoolId kLargeSlotPoolId = 102;

    DataSlotPool(std::size_t small_slots, std::size_t large_slots)
        : small_pool_(kSmallSlotPoolId, small_slots, sizeof(SmallSlot), alignof(SmallSlot)),
          large_pool_(kLargeSlotPoolId, large_slots, sizeof(LargeSlot), alignof(LargeSlot)) {}

    [[nodiscard]] std::optional<DataSlotHandle> acquire(DataSlotClass slot_class, std::size_t required_bytes) {
        switch (slot_class) {
            case DataSlotClass::small:
                if (required_bytes > kSmallFileThreshold) {
                    return std::nullopt;
                }
                if (const auto slot = small_pool_.try_acquire(); slot.has_value()) {
                    return make_handle(DataSlotClass::small, *slot);
                }
                return std::nullopt;
            case DataSlotClass::large:
                if (required_bytes > kLargeChunkBytes) {
                    return std::nullopt;
                }
                if (const auto slot = large_pool_.try_acquire(); slot.has_value()) {
                    return make_handle(DataSlotClass::large, *slot);
                }
                return std::nullopt;
        }
        return std::nullopt;
    }

    [[nodiscard]] DataSlotHandle acquire_or_throw(DataSlotClass slot_class, std::size_t required_bytes) {
        const auto handle = acquire(slot_class, required_bytes);
        if (!handle.has_value()) {
            throw std::runtime_error("no free data slots available");
        }
        return *handle;
    }

    [[nodiscard]] DataSlotHandle acquire_wait_or_throw(DataSlotClass slot_class, std::size_t required_bytes) {
        switch (slot_class) {
            case DataSlotClass::small:
                if (required_bytes > kSmallFileThreshold) {
                    throw std::runtime_error("requested data exceeds small slot capacity");
                }
                if (small_pool_.capacity() == 0) {
                    throw std::runtime_error("small data slot pool has no slots");
                }
                return make_handle(DataSlotClass::small, small_pool_.acquire_spin());
            case DataSlotClass::large:
                if (required_bytes > kLargeChunkBytes) {
                    throw std::runtime_error("requested data exceeds large slot capacity");
                }
                if (large_pool_.capacity() == 0) {
                    throw std::runtime_error("large data slot pool has no slots");
                }
                return make_handle(DataSlotClass::large, large_pool_.acquire_spin());
        }
        throw std::logic_error("unknown slot class");
    }

    void release(const DataSlotHandle& handle) {
        trailer(handle) = {};
        switch (handle.slot_class) {
            case DataSlotClass::small:
                small_pool_.release(handle.buffer_handle);
                return;
            case DataSlotClass::large:
                large_pool_.release(handle.buffer_handle);
                return;
        }
        throw std::logic_error("unknown slot class");
    }

    [[nodiscard]] std::size_t capacity(const DataSlotHandle& handle) const {
        switch (handle.slot_class) {
            case DataSlotClass::small:
                return kSmallFileThreshold;
            case DataSlotClass::large:
                return kLargeChunkBytes;
        }
        throw std::logic_error("unknown slot class");
    }

    [[nodiscard]] char* data(const DataSlotHandle& handle) {
        switch (handle.slot_class) {
            case DataSlotClass::small:
                return small_slot(handle).data.data();
            case DataSlotClass::large:
                return large_slot(handle).data.data();
        }
        throw std::logic_error("unknown slot class");
    }

    [[nodiscard]] const char* data(const DataSlotHandle& handle) const {
        switch (handle.slot_class) {
            case DataSlotClass::small:
                return small_slot(handle).data.data();
            case DataSlotClass::large:
                return large_slot(handle).data.data();
        }
        throw std::logic_error("unknown slot class");
    }

    [[nodiscard]] DataBufTrailer& trailer(const DataSlotHandle& handle) {
        switch (handle.slot_class) {
            case DataSlotClass::small:
                return small_slot(handle).trailer;
            case DataSlotClass::large:
                return large_slot(handle).trailer;
        }
        throw std::logic_error("unknown slot class");
    }

    [[nodiscard]] const DataBufTrailer& trailer(const DataSlotHandle& handle) const {
        switch (handle.slot_class) {
            case DataSlotClass::small:
                return small_slot(handle).trailer;
            case DataSlotClass::large:
                return large_slot(handle).trailer;
        }
        throw std::logic_error("unknown slot class");
    }

    [[nodiscard]] std::string_view data_view(const DataSlotHandle& handle) const {
        return std::string_view(data(handle), static_cast<std::size_t>(trailer(handle).data_len));
    }

    [[nodiscard]] std::size_t available_small() const {
        return small_pool_.available();
    }

    [[nodiscard]] std::size_t available_large() const {
        return large_pool_.available();
    }

    [[nodiscard]] std::size_t peak_small_in_use() const {
        return small_pool_.peak_in_use();
    }

    [[nodiscard]] std::size_t peak_large_in_use() const {
        return large_pool_.peak_in_use();
    }

private:
    [[nodiscard]] DataSlotHandle make_handle(DataSlotClass slot_class, const BufferHandle& handle) {
        return DataSlotHandle{slot_class, handle.index, handle, next_entry_id_.fetch_add(1), false};
    }

    [[nodiscard]] SmallSlot& small_slot(const DataSlotHandle& handle) {
        return buffer_as<SmallSlot>(small_pool_, handle.buffer_handle);
    }

    [[nodiscard]] const SmallSlot& small_slot(const DataSlotHandle& handle) const {
        return buffer_as<SmallSlot>(small_pool_, handle.buffer_handle);
    }

    [[nodiscard]] LargeSlot& large_slot(const DataSlotHandle& handle) {
        return buffer_as<LargeSlot>(large_pool_, handle.buffer_handle);
    }

    [[nodiscard]] const LargeSlot& large_slot(const DataSlotHandle& handle) const {
        return buffer_as<LargeSlot>(large_pool_, handle.buffer_handle);
    }

    RawBufferPool small_pool_;
    RawBufferPool large_pool_;
    std::atomic<std::uint64_t> next_entry_id_ {1};
};

}  // namespace hypersync

#endif
