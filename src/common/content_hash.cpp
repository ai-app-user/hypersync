#include "common/content_hash.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>

#if defined(__unix__) || defined(__APPLE__)
#include <dlfcn.h>
#endif

#include "common/hash_utils.hpp"

#define XXH_INLINE_ALL
#define XXH_NAMESPACE HYPERSYNC_XXH_
#include "third_party/xxhash/xxhash.h"

namespace hypersync {

namespace {

struct EvpMdCtx;
struct EvpMd;

struct OpenSslDigestApi {
    using CtxNewFn = EvpMdCtx* (*)();
    using CtxFreeFn = void (*)(EvpMdCtx*);
    using CtxCopyFn = int (*)(EvpMdCtx*, const EvpMdCtx*);
    using DigestInitFn = int (*)(EvpMdCtx*, const EvpMd*, void*);
    using DigestUpdateFn = int (*)(EvpMdCtx*, const void*, std::size_t);
    using DigestFinalFn = int (*)(EvpMdCtx*, unsigned char*, unsigned int*);
    using MdFn = const EvpMd* (*)();

    void* handle = nullptr;
    CtxNewFn ctx_new = nullptr;
    CtxFreeFn ctx_free = nullptr;
    CtxCopyFn ctx_copy = nullptr;
    DigestInitFn digest_init = nullptr;
    DigestUpdateFn digest_update = nullptr;
    DigestFinalFn digest_final = nullptr;
    MdFn md5 = nullptr;
    MdFn sha256 = nullptr;
    bool available = false;
};

#if defined(__unix__) || defined(__APPLE__)
template <typename Fn>
Fn load_crypto_symbol(void* handle, const char* name) {
    return reinterpret_cast<Fn>(dlsym(handle, name));
}
#endif

const OpenSslDigestApi& openssl_digest_api() {
    static const OpenSslDigestApi api = [] {
        OpenSslDigestApi loaded;
#if defined(__unix__) || defined(__APPLE__)
        const char* const library_names[] = {
#if defined(__APPLE__)
            "/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib",
            "/opt/homebrew/opt/openssl/lib/libcrypto.3.dylib",
            "/usr/local/opt/openssl@3/lib/libcrypto.3.dylib",
            "/usr/local/opt/openssl/lib/libcrypto.3.dylib",
            "libcrypto.3.dylib",
            "libcrypto.dylib",
#else
            "libcrypto.so.3",
            "libcrypto.so",
#endif
        };

        for (const char* name : library_names) {
            loaded.handle = dlopen(name, RTLD_NOW | RTLD_LOCAL);
            if (loaded.handle != nullptr) {
                break;
            }
        }
        if (loaded.handle == nullptr) {
            return loaded;
        }

        loaded.ctx_new = load_crypto_symbol<OpenSslDigestApi::CtxNewFn>(loaded.handle, "EVP_MD_CTX_new");
        loaded.ctx_free = load_crypto_symbol<OpenSslDigestApi::CtxFreeFn>(loaded.handle, "EVP_MD_CTX_free");
        loaded.ctx_copy = load_crypto_symbol<OpenSslDigestApi::CtxCopyFn>(loaded.handle, "EVP_MD_CTX_copy_ex");
        loaded.digest_init = load_crypto_symbol<OpenSslDigestApi::DigestInitFn>(loaded.handle, "EVP_DigestInit_ex");
        loaded.digest_update = load_crypto_symbol<OpenSslDigestApi::DigestUpdateFn>(loaded.handle, "EVP_DigestUpdate");
        loaded.digest_final = load_crypto_symbol<OpenSslDigestApi::DigestFinalFn>(loaded.handle, "EVP_DigestFinal_ex");
        loaded.md5 = load_crypto_symbol<OpenSslDigestApi::MdFn>(loaded.handle, "EVP_md5");
        loaded.sha256 = load_crypto_symbol<OpenSslDigestApi::MdFn>(loaded.handle, "EVP_sha256");
        loaded.available = loaded.ctx_new != nullptr && loaded.ctx_free != nullptr && loaded.ctx_copy != nullptr &&
                           loaded.digest_init != nullptr && loaded.digest_update != nullptr &&
                           loaded.digest_final != nullptr && loaded.md5 != nullptr && loaded.sha256 != nullptr;
        if (!loaded.available) {
            dlclose(loaded.handle);
            loaded = {};
        }
#endif
        return loaded;
    }();
    return api;
}

class OpenSslDigestState {
public:
    OpenSslDigestState(const OpenSslDigestApi& api, const EvpMd* digest) : api_(&api), digest_(digest) {
        ctx_ = api_->ctx_new();
        if (ctx_ == nullptr || api_->digest_init(ctx_, digest_, nullptr) != 1) {
            if (ctx_ != nullptr) {
                api_->ctx_free(ctx_);
            }
            throw std::runtime_error("failed to initialize OpenSSL digest context");
        }
    }

    OpenSslDigestState(const OpenSslDigestState& other) : api_(other.api_), digest_(other.digest_) {
        ctx_ = api_->ctx_new();
        if (ctx_ == nullptr || api_->ctx_copy(ctx_, other.ctx_) != 1) {
            if (ctx_ != nullptr) {
                api_->ctx_free(ctx_);
            }
            throw std::runtime_error("failed to copy OpenSSL digest context");
        }
    }

    OpenSslDigestState& operator=(const OpenSslDigestState& other) {
        if (this == &other) {
            return *this;
        }
        OpenSslDigestState copy(other);
        swap(copy);
        return *this;
    }

    OpenSslDigestState(OpenSslDigestState&& other) noexcept
        : api_(other.api_),
          digest_(other.digest_),
          ctx_(std::exchange(other.ctx_, nullptr)) {}

    OpenSslDigestState& operator=(OpenSslDigestState&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        reset();
        api_ = other.api_;
        digest_ = other.digest_;
        ctx_ = std::exchange(other.ctx_, nullptr);
        return *this;
    }

    ~OpenSslDigestState() {
        reset();
    }

    void update(std::string_view input) {
        if (!input.empty() && api_->digest_update(ctx_, input.data(), input.size()) != 1) {
            throw std::runtime_error("failed to update OpenSSL digest context");
        }
    }

    template <std::size_t N>
    std::array<std::uint8_t, N> digest() const {
        OpenSslDigestState finalized(*this);
        std::array<std::uint8_t, N> out {};
        unsigned int out_size = 0;
        if (api_->digest_final(finalized.ctx_, out.data(), &out_size) != 1 || out_size != N) {
            throw std::runtime_error("failed to finalize OpenSSL digest context");
        }
        return out;
    }

private:
    void reset() noexcept {
        if (ctx_ != nullptr) {
            api_->ctx_free(ctx_);
            ctx_ = nullptr;
        }
    }

    void swap(OpenSslDigestState& other) noexcept {
        std::swap(api_, other.api_);
        std::swap(digest_, other.digest_);
        std::swap(ctx_, other.ctx_);
    }

    const OpenSslDigestApi* api_ = nullptr;
    const EvpMd* digest_ = nullptr;
    EvpMdCtx* ctx_ = nullptr;
};

constexpr std::array<std::uint32_t, 64> kSha256RoundConstants {
    0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
    0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U, 0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
    0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU, 0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
    0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
    0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U, 0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
    0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U, 0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
    0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
    0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U, 0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U,
};

constexpr std::array<std::uint32_t, 64> kMd5RoundConstants {
    0xd76aa478U, 0xe8c7b756U, 0x242070dbU, 0xc1bdceeeU, 0xf57c0fafU, 0x4787c62aU, 0xa8304613U, 0xfd469501U,
    0x698098d8U, 0x8b44f7afU, 0xffff5bb1U, 0x895cd7beU, 0x6b901122U, 0xfd987193U, 0xa679438eU, 0x49b40821U,
    0xf61e2562U, 0xc040b340U, 0x265e5a51U, 0xe9b6c7aaU, 0xd62f105dU, 0x02441453U, 0xd8a1e681U, 0xe7d3fbc8U,
    0x21e1cde6U, 0xc33707d6U, 0xf4d50d87U, 0x455a14edU, 0xa9e3e905U, 0xfcefa3f8U, 0x676f02d9U, 0x8d2a4c8aU,
    0xfffa3942U, 0x8771f681U, 0x6d9d6122U, 0xfde5380cU, 0xa4beea44U, 0x4bdecfa9U, 0xf6bb4b60U, 0xbebfbc70U,
    0x289b7ec6U, 0xeaa127faU, 0xd4ef3085U, 0x04881d05U, 0xd9d4d039U, 0xe6db99e5U, 0x1fa27cf8U, 0xc4ac5665U,
    0xf4292244U, 0x432aff97U, 0xab9423a7U, 0xfc93a039U, 0x655b59c3U, 0x8f0ccc92U, 0xffeff47dU, 0x85845dd1U,
    0x6fa87e4fU, 0xfe2ce6e0U, 0xa3014314U, 0x4e0811a1U, 0xf7537e82U, 0xbd3af235U, 0x2ad7d2bbU, 0xeb86d391U,
};

constexpr std::array<std::uint32_t, 64> kMd5RoundShifts {
    7U, 12U, 17U, 22U, 7U, 12U, 17U, 22U, 7U, 12U, 17U, 22U, 7U, 12U, 17U, 22U,
    5U, 9U, 14U, 20U, 5U, 9U, 14U, 20U, 5U, 9U, 14U, 20U, 5U, 9U, 14U, 20U,
    4U, 11U, 16U, 23U, 4U, 11U, 16U, 23U, 4U, 11U, 16U, 23U, 4U, 11U, 16U, 23U,
    6U, 10U, 15U, 21U, 6U, 10U, 15U, 21U, 6U, 10U, 15U, 21U, 6U, 10U, 15U, 21U,
};

std::uint32_t rotate_right(std::uint32_t value, unsigned shift) {
    return (value >> shift) | (value << (32U - shift));
}

std::uint32_t rotate_left(std::uint32_t value, unsigned shift) {
    return (value << shift) | (value >> (32U - shift));
}

std::uint32_t read_be32(const std::uint8_t* data) {
    return (static_cast<std::uint32_t>(data[0]) << 24U) |
           (static_cast<std::uint32_t>(data[1]) << 16U) |
           (static_cast<std::uint32_t>(data[2]) << 8U) |
           static_cast<std::uint32_t>(data[3]);
}

std::uint32_t read_le32(const std::uint8_t* data) {
    return static_cast<std::uint32_t>(data[0]) |
           (static_cast<std::uint32_t>(data[1]) << 8U) |
           (static_cast<std::uint32_t>(data[2]) << 16U) |
           (static_cast<std::uint32_t>(data[3]) << 24U);
}

void write_be32(std::uint32_t value, std::uint8_t* out) {
    out[0] = static_cast<std::uint8_t>((value >> 24U) & 0xffU);
    out[1] = static_cast<std::uint8_t>((value >> 16U) & 0xffU);
    out[2] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
    out[3] = static_cast<std::uint8_t>(value & 0xffU);
}

void write_le32(std::uint32_t value, std::uint8_t* out) {
    out[0] = static_cast<std::uint8_t>(value & 0xffU);
    out[1] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
    out[2] = static_cast<std::uint8_t>((value >> 16U) & 0xffU);
    out[3] = static_cast<std::uint8_t>((value >> 24U) & 0xffU);
}

void write_be64(std::uint64_t value, std::uint8_t* out) {
    for (std::size_t index = 0; index < 8U; ++index) {
        out[index] = static_cast<std::uint8_t>((value >> (56U - index * 8U)) & 0xffU);
    }
}

void write_le64(std::uint64_t value, std::uint8_t* out) {
    for (std::size_t index = 0; index < 8U; ++index) {
        out[index] = static_cast<std::uint8_t>((value >> (index * 8U)) & 0xffU);
    }
}

template <std::size_t N>
std::string hex_encode(const std::array<std::uint8_t, N>& bytes) {
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (const std::uint8_t byte : bytes) {
        out << std::setw(2) << static_cast<unsigned>(byte);
    }
    return out.str();
}

std::string hex_u64(std::uint64_t value) {
    std::ostringstream out;
    out << std::hex << std::setfill('0') << std::setw(16) << value;
    return out.str();
}

std::unique_ptr<OpenSslDigestState> make_openssl_digest(const EvpMd* (*digest_factory)()) {
    const OpenSslDigestApi& api = openssl_digest_api();
    if (!api.available) {
        return nullptr;
    }
    try {
        return std::make_unique<OpenSslDigestState>(api, digest_factory());
    } catch (const std::runtime_error&) {
        return nullptr;
    }
}

}  // namespace

struct Md5State::Impl {
    explicit Impl(std::unique_ptr<OpenSslDigestState> digest_state) : digest(std::move(digest_state)) {}
    Impl(const Impl& other) : digest(other.digest ? std::make_unique<OpenSslDigestState>(*other.digest) : nullptr) {}

    std::unique_ptr<OpenSslDigestState> digest;
};

struct Sha256State::Impl {
    explicit Impl(std::unique_ptr<OpenSslDigestState> digest_state) : digest(std::move(digest_state)) {}
    Impl(const Impl& other) : digest(other.digest ? std::make_unique<OpenSslDigestState>(*other.digest) : nullptr) {}

    std::unique_ptr<OpenSslDigestState> digest;
};

Md5State::Md5State()
    : state_ {0x67452301U, 0xefcdab89U, 0x98badcfeU, 0x10325476U} {
    const OpenSslDigestApi& api = openssl_digest_api();
    if (api.available) {
        if (auto digest = make_openssl_digest(api.md5); digest) {
            impl_ = std::make_unique<Impl>(std::move(digest));
        }
    }
}

Md5State::Md5State(const Md5State& other)
    : impl_(other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr),
      buffer_(other.buffer_),
      state_(other.state_),
      total_bytes_(other.total_bytes_),
      buffered_bytes_(other.buffered_bytes_) {}

Md5State& Md5State::operator=(const Md5State& other) {
    if (this == &other) {
        return *this;
    }
    impl_ = other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr;
    buffer_ = other.buffer_;
    state_ = other.state_;
    total_bytes_ = other.total_bytes_;
    buffered_bytes_ = other.buffered_bytes_;
    return *this;
}

Md5State::Md5State(Md5State&& other) noexcept = default;

Md5State& Md5State::operator=(Md5State&& other) noexcept = default;

Md5State::~Md5State() = default;

void Md5State::process_block(const std::uint8_t* block) {
    std::array<std::uint32_t, 16> words {};
    for (std::size_t index = 0; index < words.size(); ++index) {
        words[index] = read_le32(block + index * 4U);
    }

    std::uint32_t a = state_[0];
    std::uint32_t b = state_[1];
    std::uint32_t c = state_[2];
    std::uint32_t d = state_[3];

    for (std::size_t index = 0; index < kMd5RoundConstants.size(); ++index) {
        std::uint32_t f = 0;
        std::size_t g = 0;
        if (index < 16U) {
            f = (b & c) | ((~b) & d);
            g = index;
        } else if (index < 32U) {
            f = (d & b) | ((~d) & c);
            g = (5U * index + 1U) % 16U;
        } else if (index < 48U) {
            f = b ^ c ^ d;
            g = (3U * index + 5U) % 16U;
        } else {
            f = c ^ (b | (~d));
            g = (7U * index) % 16U;
        }

        const std::uint32_t next_b =
            b + rotate_left(a + f + kMd5RoundConstants[index] + words[g], kMd5RoundShifts[index]);
        a = d;
        d = c;
        c = b;
        b = next_b;
    }

    state_[0] += a;
    state_[1] += b;
    state_[2] += c;
    state_[3] += d;
}

void Md5State::update(std::string_view input) {
    if (impl_ && impl_->digest) {
        impl_->digest->update(input);
        return;
    }

    const auto* bytes = reinterpret_cast<const std::uint8_t*>(input.data());
    std::size_t remaining = input.size();
    total_bytes_ += remaining;

    if (buffered_bytes_ != 0U) {
        const std::size_t fill = std::min<std::size_t>(buffer_.size() - buffered_bytes_, remaining);
        std::memcpy(buffer_.data() + buffered_bytes_, bytes, fill);
        buffered_bytes_ += fill;
        bytes += fill;
        remaining -= fill;
        if (buffered_bytes_ == buffer_.size()) {
            process_block(buffer_.data());
            buffered_bytes_ = 0;
        }
    }

    while (remaining >= buffer_.size()) {
        process_block(bytes);
        bytes += buffer_.size();
        remaining -= buffer_.size();
    }

    if (remaining != 0U) {
        std::memcpy(buffer_.data(), bytes, remaining);
        buffered_bytes_ = remaining;
    }
}

std::array<std::uint8_t, 16> Md5State::digest() const {
    if (impl_ && impl_->digest) {
        return impl_->digest->digest<16>();
    }

    Md5State finalized = *this;
    const std::uint64_t bit_length = finalized.total_bytes_ * 8U;

    finalized.buffer_[finalized.buffered_bytes_++] = 0x80U;
    if (finalized.buffered_bytes_ > 56U) {
        std::fill(finalized.buffer_.begin() + static_cast<std::ptrdiff_t>(finalized.buffered_bytes_),
                  finalized.buffer_.end(),
                  0U);
        finalized.process_block(finalized.buffer_.data());
        finalized.buffered_bytes_ = 0;
    }

    std::fill(finalized.buffer_.begin() + static_cast<std::ptrdiff_t>(finalized.buffered_bytes_),
              finalized.buffer_.begin() + 56,
              0U);
    write_le64(bit_length, finalized.buffer_.data() + 56);
    finalized.process_block(finalized.buffer_.data());

    std::array<std::uint8_t, 16> out {};
    for (std::size_t index = 0; index < finalized.state_.size(); ++index) {
        write_le32(finalized.state_[index], out.data() + index * 4U);
    }
    return out;
}

std::string Md5State::hex_digest() const {
    return hex_encode(digest());
}

Sha256State::Sha256State()
    : state_ {0x6a09e667U,
              0xbb67ae85U,
              0x3c6ef372U,
              0xa54ff53aU,
              0x510e527fU,
              0x9b05688cU,
              0x1f83d9abU,
              0x5be0cd19U} {
    const OpenSslDigestApi& api = openssl_digest_api();
    if (api.available) {
        if (auto digest = make_openssl_digest(api.sha256); digest) {
            impl_ = std::make_unique<Impl>(std::move(digest));
        }
    }
}

Sha256State::Sha256State(const Sha256State& other)
    : impl_(other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr),
      buffer_(other.buffer_),
      state_(other.state_),
      total_bytes_(other.total_bytes_),
      buffered_bytes_(other.buffered_bytes_) {}

Sha256State& Sha256State::operator=(const Sha256State& other) {
    if (this == &other) {
        return *this;
    }
    impl_ = other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr;
    buffer_ = other.buffer_;
    state_ = other.state_;
    total_bytes_ = other.total_bytes_;
    buffered_bytes_ = other.buffered_bytes_;
    return *this;
}

Sha256State::Sha256State(Sha256State&& other) noexcept = default;

Sha256State& Sha256State::operator=(Sha256State&& other) noexcept = default;

Sha256State::~Sha256State() = default;

void Sha256State::process_block(const std::uint8_t* block) {
    std::array<std::uint32_t, 64> words {};
    for (std::size_t index = 0; index < 16U; ++index) {
        words[index] = read_be32(block + index * 4U);
    }
    for (std::size_t index = 16U; index < words.size(); ++index) {
        const std::uint32_t s0 = rotate_right(words[index - 15U], 7U) ^
                                 rotate_right(words[index - 15U], 18U) ^
                                 (words[index - 15U] >> 3U);
        const std::uint32_t s1 = rotate_right(words[index - 2U], 17U) ^
                                 rotate_right(words[index - 2U], 19U) ^
                                 (words[index - 2U] >> 10U);
        words[index] = words[index - 16U] + s0 + words[index - 7U] + s1;
    }

    std::uint32_t a = state_[0];
    std::uint32_t b = state_[1];
    std::uint32_t c = state_[2];
    std::uint32_t d = state_[3];
    std::uint32_t e = state_[4];
    std::uint32_t f = state_[5];
    std::uint32_t g = state_[6];
    std::uint32_t h = state_[7];

    for (std::size_t index = 0; index < words.size(); ++index) {
        const std::uint32_t big_s1 = rotate_right(e, 6U) ^ rotate_right(e, 11U) ^ rotate_right(e, 25U);
        const std::uint32_t ch = (e & f) ^ ((~e) & g);
        const std::uint32_t temp1 = h + big_s1 + ch + kSha256RoundConstants[index] + words[index];
        const std::uint32_t big_s0 = rotate_right(a, 2U) ^ rotate_right(a, 13U) ^ rotate_right(a, 22U);
        const std::uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
        const std::uint32_t temp2 = big_s0 + maj;

        h = g;
        g = f;
        f = e;
        e = d + temp1;
        d = c;
        c = b;
        b = a;
        a = temp1 + temp2;
    }

    state_[0] += a;
    state_[1] += b;
    state_[2] += c;
    state_[3] += d;
    state_[4] += e;
    state_[5] += f;
    state_[6] += g;
    state_[7] += h;
}

void Sha256State::update(std::string_view input) {
    if (impl_ && impl_->digest) {
        impl_->digest->update(input);
        return;
    }

    const auto* bytes = reinterpret_cast<const std::uint8_t*>(input.data());
    std::size_t remaining = input.size();
    total_bytes_ += remaining;

    if (buffered_bytes_ != 0U) {
        const std::size_t fill = std::min<std::size_t>(buffer_.size() - buffered_bytes_, remaining);
        std::memcpy(buffer_.data() + buffered_bytes_, bytes, fill);
        buffered_bytes_ += fill;
        bytes += fill;
        remaining -= fill;
        if (buffered_bytes_ == buffer_.size()) {
            process_block(buffer_.data());
            buffered_bytes_ = 0;
        }
    }

    while (remaining >= buffer_.size()) {
        process_block(bytes);
        bytes += buffer_.size();
        remaining -= buffer_.size();
    }

    if (remaining != 0U) {
        std::memcpy(buffer_.data(), bytes, remaining);
        buffered_bytes_ = remaining;
    }
}

std::array<std::uint8_t, 32> Sha256State::digest() const {
    if (impl_ && impl_->digest) {
        return impl_->digest->digest<32>();
    }

    Sha256State finalized = *this;
    const std::uint64_t bit_length = finalized.total_bytes_ * 8U;

    finalized.buffer_[finalized.buffered_bytes_++] = 0x80U;
    if (finalized.buffered_bytes_ > 56U) {
        std::fill(finalized.buffer_.begin() + static_cast<std::ptrdiff_t>(finalized.buffered_bytes_),
                  finalized.buffer_.end(),
                  0U);
        finalized.process_block(finalized.buffer_.data());
        finalized.buffered_bytes_ = 0;
    }

    std::fill(finalized.buffer_.begin() + static_cast<std::ptrdiff_t>(finalized.buffered_bytes_),
              finalized.buffer_.begin() + 56,
              0U);
    write_be64(bit_length, finalized.buffer_.data() + 56);
    finalized.process_block(finalized.buffer_.data());

    std::array<std::uint8_t, 32> out {};
    for (std::size_t index = 0; index < finalized.state_.size(); ++index) {
        write_be32(finalized.state_[index], out.data() + index * 4U);
    }
    return out;
}

std::string Sha256State::hex_digest() const {
    return hex_encode(digest());
}

struct Xxh3_64State::Impl {
    Impl() : state(XXH3_createState()) {
        if (state == nullptr) {
            throw std::runtime_error("failed to initialize XXH3 64-bit state");
        }
        if (XXH3_64bits_reset(state) == XXH_ERROR) {
            (void)XXH3_freeState(state);
            state = nullptr;
            throw std::runtime_error("failed to initialize XXH3 64-bit state");
        }
    }

    Impl(const Impl& other) : Impl() {
        XXH3_copyState(state, other.state);
    }

    ~Impl() {
        (void)XXH3_freeState(state);
    }

    XXH3_state_t* state = nullptr;
};

Xxh3_64State::Xxh3_64State() : impl_(std::make_unique<Impl>()) {}

Xxh3_64State::Xxh3_64State(const Xxh3_64State& other)
    : impl_(other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr) {}

Xxh3_64State& Xxh3_64State::operator=(const Xxh3_64State& other) {
    if (this == &other) {
        return *this;
    }
    impl_ = other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr;
    return *this;
}

Xxh3_64State::Xxh3_64State(Xxh3_64State&& other) noexcept = default;

Xxh3_64State& Xxh3_64State::operator=(Xxh3_64State&& other) noexcept = default;

Xxh3_64State::~Xxh3_64State() = default;

void Xxh3_64State::update(std::string_view input) {
    if (!input.empty() && XXH3_64bits_update(impl_->state, input.data(), input.size()) == XXH_ERROR) {
        throw std::runtime_error("failed to update XXH3 64-bit state");
    }
}

std::uint64_t Xxh3_64State::value() const {
    return XXH3_64bits_digest(impl_->state);
}

std::string Xxh3_64State::hex_digest() const {
    return hex_u64(value());
}

struct Xxh3_128State::Impl {
    Impl() : state(XXH3_createState()) {
        if (state == nullptr) {
            throw std::runtime_error("failed to initialize XXH3 128-bit state");
        }
        if (XXH3_128bits_reset(state) == XXH_ERROR) {
            (void)XXH3_freeState(state);
            state = nullptr;
            throw std::runtime_error("failed to initialize XXH3 128-bit state");
        }
    }

    Impl(const Impl& other) : Impl() {
        XXH3_copyState(state, other.state);
    }

    ~Impl() {
        (void)XXH3_freeState(state);
    }

    XXH3_state_t* state = nullptr;
};

Xxh3_128State::Xxh3_128State() : impl_(std::make_unique<Impl>()) {}

Xxh3_128State::Xxh3_128State(const Xxh3_128State& other)
    : impl_(other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr) {}

Xxh3_128State& Xxh3_128State::operator=(const Xxh3_128State& other) {
    if (this == &other) {
        return *this;
    }
    impl_ = other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr;
    return *this;
}

Xxh3_128State::Xxh3_128State(Xxh3_128State&& other) noexcept = default;

Xxh3_128State& Xxh3_128State::operator=(Xxh3_128State&& other) noexcept = default;

Xxh3_128State::~Xxh3_128State() = default;

void Xxh3_128State::update(std::string_view input) {
    if (!input.empty() && XXH3_128bits_update(impl_->state, input.data(), input.size()) == XXH_ERROR) {
        throw std::runtime_error("failed to update XXH3 128-bit state");
    }
}

std::array<std::uint8_t, 16> Xxh3_128State::digest() const {
    const XXH128_hash_t hash = XXH3_128bits_digest(impl_->state);
    XXH128_canonical_t canonical {};
    XXH128_canonicalFromHash(&canonical, hash);
    std::array<std::uint8_t, 16> out {};
    std::copy(canonical.digest, canonical.digest + out.size(), out.begin());
    return out;
}

std::string Xxh3_128State::hex_digest() const {
    return hex_encode(digest());
}

ContentHashAlgorithm parse_content_hash_algorithm(const std::string& value) {
    if (value == "md5" || value == "md5sum" || value == "MD5") {
        return ContentHashAlgorithm::md5;
    }
    if (value == "sha256" || value == "sha-256" || value == "sha256sum" ||
        value == "SHA256" || value == "SHA-256") {
        return ContentHashAlgorithm::sha256;
    }
    if (value == "xxh64" || value == "xxhash64" || value == "xxhsum" || value == "XXH64") {
        return ContentHashAlgorithm::xxh64;
    }
    if (value == "xxh3" || value == "xxh3_64" || value == "xxh3-64" ||
        value == "xxh3_64bits" || value == "XXH3" || value == "XXH3_64") {
        return ContentHashAlgorithm::xxh3_64;
    }
    if (value == "xxh128" || value == "xxh3_128" || value == "xxh3-128" ||
        value == "xxh3_128bits" || value == "XXH128" || value == "XXH3_128") {
        return ContentHashAlgorithm::xxh3_128;
    }
    throw std::invalid_argument("hash algorithm must be md5, sha256, xxh64, xxh3_64, or xxh3_128");
}

std::string to_string(ContentHashAlgorithm algorithm) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5:
            return "md5";
        case ContentHashAlgorithm::sha256:
            return "sha256";
        case ContentHashAlgorithm::xxh64:
            return "xxh64";
        case ContentHashAlgorithm::xxh3_64:
            return "xxh3_64";
        case ContentHashAlgorithm::xxh3_128:
            return "xxh3_128";
    }
    return "unknown";
}

std::string content_hash_hex(ContentHashAlgorithm algorithm, std::string_view content) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5: {
            Md5State hasher;
            hasher.update(content);
            return hasher.hex_digest();
        }
        case ContentHashAlgorithm::sha256: {
            Sha256State hasher;
            hasher.update(content);
            return hasher.hex_digest();
        }
        case ContentHashAlgorithm::xxh64:
            return hex_u64(hash64(content));
        case ContentHashAlgorithm::xxh3_64: {
            Xxh3_64State hasher;
            hasher.update(content);
            return hasher.hex_digest();
        }
        case ContentHashAlgorithm::xxh3_128: {
            Xxh3_128State hasher;
            hasher.update(content);
            return hasher.hex_digest();
        }
    }
    throw std::invalid_argument("unsupported hash algorithm");
}

}  // namespace hypersync
