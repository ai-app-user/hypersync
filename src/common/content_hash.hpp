#ifndef HYPERSYNC_COMMON_CONTENT_HASH_HPP
#define HYPERSYNC_COMMON_CONTENT_HASH_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace hypersync {

enum class ContentHashAlgorithm {
    md5,
    sha256,
    xxh64,
    xxh3_64,
    xxh3_128,
};

class Md5State {
public:
    Md5State();
    Md5State(const Md5State& other);
    Md5State& operator=(const Md5State& other);
    Md5State(Md5State&& other) noexcept;
    Md5State& operator=(Md5State&& other) noexcept;
    ~Md5State();

    void update(std::string_view input);
    [[nodiscard]] std::array<std::uint8_t, 16> digest() const;
    [[nodiscard]] std::string hex_digest() const;

private:
    struct Impl;

    void process_block(const std::uint8_t* block);

    std::unique_ptr<Impl> impl_;
    std::array<std::uint8_t, 64> buffer_ {};
    std::array<std::uint32_t, 4> state_ {};
    std::uint64_t total_bytes_ = 0;
    std::size_t buffered_bytes_ = 0;
};

class Sha256State {
public:
    Sha256State();
    Sha256State(const Sha256State& other);
    Sha256State& operator=(const Sha256State& other);
    Sha256State(Sha256State&& other) noexcept;
    Sha256State& operator=(Sha256State&& other) noexcept;
    ~Sha256State();

    void update(std::string_view input);
    [[nodiscard]] std::array<std::uint8_t, 32> digest() const;
    [[nodiscard]] std::string hex_digest() const;

private:
    struct Impl;

    void process_block(const std::uint8_t* block);

    std::unique_ptr<Impl> impl_;
    std::array<std::uint8_t, 64> buffer_ {};
    std::array<std::uint32_t, 8> state_ {};
    std::uint64_t total_bytes_ = 0;
    std::size_t buffered_bytes_ = 0;
};

class Xxh3_64State {
public:
    Xxh3_64State();
    Xxh3_64State(const Xxh3_64State& other);
    Xxh3_64State& operator=(const Xxh3_64State& other);
    Xxh3_64State(Xxh3_64State&& other) noexcept;
    Xxh3_64State& operator=(Xxh3_64State&& other) noexcept;
    ~Xxh3_64State();

    void update(std::string_view input);
    [[nodiscard]] std::uint64_t value() const;
    [[nodiscard]] std::string hex_digest() const;

private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
};

class Xxh3_128State {
public:
    Xxh3_128State();
    Xxh3_128State(const Xxh3_128State& other);
    Xxh3_128State& operator=(const Xxh3_128State& other);
    Xxh3_128State(Xxh3_128State&& other) noexcept;
    Xxh3_128State& operator=(Xxh3_128State&& other) noexcept;
    ~Xxh3_128State();

    void update(std::string_view input);
    [[nodiscard]] std::array<std::uint8_t, 16> digest() const;
    [[nodiscard]] std::string hex_digest() const;

private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] ContentHashAlgorithm parse_content_hash_algorithm(const std::string& value);
[[nodiscard]] std::string to_string(ContentHashAlgorithm algorithm);
[[nodiscard]] std::string content_hash_hex(ContentHashAlgorithm algorithm, std::string_view content);

}  // namespace hypersync

#endif
