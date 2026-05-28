#include "hypersync.hpp"
#include "jobs/threaded_job.hpp"

#include <atomic>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <functional>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

using hypersync::BufferPoolRegistry;
using hypersync::BufQueue;
using hypersync::BufferHandle;
using hypersync::BufferDiscarderConfig;
using hypersync::BufferDiscarderJob;
using hypersync::BufferGeneratorConfig;
using hypersync::BufferGeneratorJob;
using hypersync::BufferGeneratorPattern;
using hypersync::BufferReceiverJob;
using hypersync::BufferSenderJob;
using hypersync::BufferTransportEndpoint;
using hypersync::AutoScaleDecision;
using hypersync::AutoScaleMetrics;
using hypersync::AutoScalePolicy;
using hypersync::AutoScaleProfileStore;
using hypersync::AutoScaler;
using hypersync::JobAutoScaleRunner;
using hypersync::PipelineAutoScaleRunner;
using hypersync::RawBufferPool;
using hypersync::ShardedBufQueue;
using hypersync::Checker;
using hypersync::CheckerConfig;
using hypersync::DataCacher;
using hypersync::DataChunk;
using hypersync::DataHasherConfig;
using hypersync::DataHasherJob;
using hypersync::DataReceiver;
using hypersync::DataSender;
using hypersync::DataWriter;
using hypersync::DiffKind;
using hypersync::EndpointRole;
using hypersync::EngineConfig;
using hypersync::FileSnapshot;
using hypersync::FileSpec;
using hypersync::FileState;
using hypersync::FolderRecord;
using hypersync::FolderState;
using hypersync::InputProvider;
using hypersync::JobMessage;
using hypersync::JobStats;
using hypersync::MetadataFolderRecord;
using hypersync::MetadataBufferRecordKind;
using hypersync::MetadataRecordFormat;
using hypersync::MetadataRecordWriter;
using hypersync::MetadataRecordWriterConfig;
using hypersync::MetadataRecordWriterJob;
using hypersync::MetadataStatsDiscarder;
using hypersync::Mode;
using hypersync::NfsDataBufferReaderJob;
using hypersync::NfsDataReader;
using hypersync::NfsMetaReader;
using hypersync::RecBuf;
using hypersync::ScanIndex;
using hypersync::SplitBucketPriorityDecision;
using hypersync::SplitBucketPriorityInput;
using hypersync::BucketPathOverloadInput;
using hypersync::BucketPathOverloadScores;
using hypersync::SplitScannerCapacityDecision;
using hypersync::SyntheticFileView;
using hypersync::SyntheticObservedFile;
using hypersync::SyntheticPayloadPattern;
using hypersync::SyntheticPayloadPool;
using hypersync::SyntheticPayloadView;
using hypersync::SyntheticPhaseProfile;
using hypersync::SyntheticProfileBuilder;
using hypersync::SyntheticProfileCaptureConfig;
using hypersync::SyntheticReplayConfig;
using hypersync::SyntheticReplayCursor;
using hypersync::SyntheticWorkloadProfile;
using hypersync::TargetDataWriterConfig;
using hypersync::TargetDataWriterJob;
using hypersync::TargetMetaWriterConfig;
using hypersync::TargetMetaWriterJob;
using hypersync::ScanWriter;
using hypersync::SpscRing;
using hypersync::TransferEngine;
using hypersync::ThreadedJob;
using hypersync::kDataBufferPoolId;
using hypersync::kMetadataBufferPoolId;
using hypersync::choose_split_bucket_priority_workers;
using hypersync::choose_split_scanner_capacity;
using hypersync::evaluate_bucket_path_overload;
using hypersync::synthetic_size_bucket_index;

namespace {

namespace fs = std::filesystem;

struct TestFailure : std::runtime_error {
    using std::runtime_error::runtime_error;
};

#define EXPECT_TRUE(cond)                                                                                               \
    do {                                                                                                                \
        if (!(cond)) {                                                                                                  \
            throw TestFailure(std::string("EXPECT_TRUE failed at line ") + std::to_string(__LINE__) + ": " #cond);     \
        }                                                                                                               \
    } while (false)

#define EXPECT_FALSE(cond) EXPECT_TRUE(!(cond))

#define EXPECT_EQ(lhs, rhs)                                                                                                     \
    do {                                                                                                                        \
        const auto& lhs_eval = (lhs);                                                                                           \
        const auto& rhs_eval = (rhs);                                                                                           \
        if (!(lhs_eval == rhs_eval)) {                                                                                          \
            throw TestFailure(std::string("EXPECT_EQ failed at line ") + std::to_string(__LINE__) + ": " #lhs " != " #rhs);   \
        }                                                                                                                       \
    } while (false)

#define EXPECT_THROW(stmt)                                                                                              \
    do {                                                                                                                \
        bool threw = false;                                                                                              \
        try {                                                                                                             \
            (void)(stmt);                                                                                                 \
        } catch (...) {                                                                                                   \
            threw = true;                                                                                                 \
        }                                                                                                                 \
        if (!threw) {                                                                                                     \
            throw TestFailure(std::string("EXPECT_THROW failed at line ") + std::to_string(__LINE__) + ": " #stmt);    \
        }                                                                                                                 \
    } while (false)

struct TempDir {
    fs::path path;

    explicit TempDir(std::string prefix)
        : path(fs::temp_directory_path() /
               (std::move(prefix) + "_" +
                std::to_string(static_cast<long long>(
                    std::chrono::steady_clock::now().time_since_epoch().count())))) {
        fs::create_directories(path);
    }

    ~TempDir() {
        std::error_code ignored;
        fs::remove_all(path, ignored);
    }
};

void write_file(const fs::path& path, const std::string& content) {
    fs::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw TestFailure("failed to create test file");
    }
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
}

bool command_succeeds(const std::string& command) {
    return std::system(command.c_str()) == 0;
}

std::uint64_t stat_mtime_ns(const struct stat& info) {
#if defined(__APPLE__)
    return static_cast<std::uint64_t>(info.st_mtimespec.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(info.st_mtimespec.tv_nsec);
#else
    return static_cast<std::uint64_t>(info.st_mtim.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(info.st_mtim.tv_nsec);
#endif
}

struct StatSnapshot {
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint32_t mode = 0;
    std::uint64_t mtime = 0;
};

StatSnapshot read_stat_snapshot(const fs::path& path) {
    struct stat info {};
    if (::stat(path.c_str(), &info) != 0) {
        throw TestFailure("failed to stat path: " + path.string());
    }

    StatSnapshot snapshot;
    snapshot.uid = static_cast<std::uint32_t>(info.st_uid);
    snapshot.gid = static_cast<std::uint32_t>(info.st_gid);
    snapshot.mode = static_cast<std::uint32_t>(info.st_mode & 0777U);
    snapshot.mtime = stat_mtime_ns(info);
    return snapshot;
}

void set_path_metadata(const fs::path& path, std::uint32_t mode, std::uint64_t mtime) {
    if (::chmod(path.c_str(), static_cast<mode_t>(mode)) != 0) {
        throw TestFailure("chmod failed for " + path.string());
    }

    struct timespec times[2];
    times[0].tv_sec = static_cast<time_t>(mtime / 1'000'000'000ULL);
    times[0].tv_nsec = static_cast<long>(mtime % 1'000'000'000ULL);
    times[1] = times[0];
    if (::utimensat(AT_FDCWD, path.c_str(), times, 0) != 0) {
        throw TestFailure("utimensat failed for " + path.string());
    }
}

bool sudo_available() {
    static const bool available = command_succeeds("sudo -n true >/dev/null 2>&1");
    return available;
}

std::size_t count_regular_files(const fs::path& root) {
    if (!fs::exists(root)) {
        return 0;
    }
    std::size_t count = 0;
    for (const auto& entry : fs::recursive_directory_iterator(root)) {
        if (entry.is_regular_file()) {
            ++count;
        }
    }
    return count;
}

struct ScopedNfsExport {
    fs::path export_path;
    std::string export_host = "127.0.0.1";
    bool active = false;
    bool stop_rpcbind_on_teardown = false;
    bool stop_nfs_server_on_teardown = false;

    explicit ScopedNfsExport(fs::path path) : export_path(std::move(path)) {
        if (!hypersync::libnfs_support_enabled()) {
            return;
        }

        const std::string path_string = export_path.string();
        stop_rpcbind_on_teardown = !command_succeeds("systemctl -q is-active rpcbind >/dev/null 2>&1");
        stop_nfs_server_on_teardown = !command_succeeds("systemctl -q is-active nfs-server >/dev/null 2>&1");
        if (!command_succeeds("sudo systemctl start rpcbind nfs-server >/dev/null 2>&1")) {
            throw TestFailure("failed to start local NFS services");
        }
        if (!command_succeeds("sudo exportfs -i -o rw,sync,no_subtree_check,no_root_squash,insecure " + export_host +
                              ":" + path_string + " >/dev/null 2>&1")) {
            throw TestFailure("failed to export local NFS path");
        }
        if (!command_succeeds("showmount -e " + export_host + " | grep -F \"" + path_string + "\" >/dev/null 2>&1")) {
            teardown();
            throw TestFailure("local NFS export did not become visible");
        }
        active = true;
    }

    ~ScopedNfsExport() {
        teardown();
    }

    [[nodiscard]] std::string url() const {
        return "nfs://" + export_host + export_path.string();
    }

    void teardown() {
        if (!active) {
            return;
        }
        const std::string path_string = export_path.string();
        command_succeeds("sudo exportfs -u " + export_host + ":" + path_string + " >/dev/null 2>&1");
        if (stop_nfs_server_on_teardown) {
            command_succeeds("sudo systemctl stop nfs-server >/dev/null 2>&1");
        }
        if (stop_rpcbind_on_teardown) {
            command_succeeds("sudo systemctl stop rpcbind.service rpcbind.socket >/dev/null 2>&1");
        }
        active = false;
    }
};

struct ScopedChildProcess {
    pid_t pid = -1;
    fs::path log_path;

    explicit ScopedChildProcess(std::vector<std::string> command, const fs::path& log_dir) {
        log_path = log_dir / ("receiver_" +
                              std::to_string(static_cast<long long>(
                                  std::chrono::steady_clock::now().time_since_epoch().count())) +
                              ".log");

        pid = ::fork();
        if (pid < 0) {
            throw TestFailure("fork failed");
        }
        if (pid == 0) {
            const int log_fd = ::open(log_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0600);
            if (log_fd < 0) {
                _exit(127);
            }
            ::dup2(log_fd, STDOUT_FILENO);
            ::dup2(log_fd, STDERR_FILENO);
            ::close(log_fd);

            std::vector<char*> argv;
            argv.reserve(command.size() + 1U);
            for (auto& arg : command) {
                argv.push_back(arg.data());
            }
            argv.push_back(nullptr);
            ::execvp(argv.front(), argv.data());
            _exit(127);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    ~ScopedChildProcess() {
        if (pid > 0) {
            int status = 0;
            ::kill(pid, SIGTERM);
            ::waitpid(pid, &status, 0);
        }
        std::error_code ignored;
        fs::remove(log_path, ignored);
    }

    [[nodiscard]] int wait() {
        if (pid <= 0) {
            return 0;
        }
        int status = 0;
        if (::waitpid(pid, &status, 0) < 0) {
            throw TestFailure("waitpid failed");
        }
        pid = -1;
        return status;
    }

    [[nodiscard]] std::string read_log() const {
        if (!fs::exists(log_path)) {
            return {};
        }
        std::ifstream input(log_path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
};

ScopedChildProcess launch_receiver_process(const std::string& target,
                                           std::uint16_t priority_port,
                                           std::uint16_t data_port,
                                           const fs::path& log_dir,
                                           bool use_sudo) {
    std::vector<std::string> command;
    if (use_sudo) {
        const fs::path gcov_prefix = log_dir / "gcov";
        fs::create_directories(gcov_prefix);
        command.push_back("sudo");
        command.push_back("-n");
        command.push_back("env");
        command.push_back("GCOV_PREFIX=" + gcov_prefix.string());
        command.push_back("GCOV_PREFIX_STRIP=0");
    }
    command.push_back((fs::current_path() / "build" / "hypersync").string());
    command.push_back("receive");
    command.push_back("--target");
    command.push_back(target);
    command.push_back("--bind-host");
    command.push_back("127.0.0.1");
    command.push_back("--priority-port");
    command.push_back(std::to_string(priority_port));
    command.push_back("--data-port");
    command.push_back(std::to_string(data_port));
    return ScopedChildProcess(std::move(command), log_dir);
}

void expect_child_success(ScopedChildProcess& process, std::string_view label) {
    const int status = process.wait();
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        throw TestFailure(std::string(label) + " failed: " + process.read_log());
    }
}

std::uint16_t pick_unused_port() {
    auto listener = hypersync::listen_tcp("127.0.0.1", 0);
    return hypersync::socket_port(listener.get());
}

void test_raw_buffer_pool_reuses_slots_and_detects_errors() {
    RawBufferPool pool(88U, 2U, sizeof(int), alignof(int));
    EXPECT_EQ(pool.capacity(), 2U);
    EXPECT_EQ(pool.buffer_size_bytes(), sizeof(int));
    EXPECT_EQ(pool.total_size_bytes(), 2U * pool.stride_size_bytes());
    EXPECT_EQ(pool.available(), 2U);

    const auto first = pool.try_acquire();
    const auto second = pool.try_acquire();
    EXPECT_TRUE(first.has_value());
    EXPECT_TRUE(second.has_value());
    EXPECT_FALSE(pool.try_acquire().has_value());
    EXPECT_EQ(pool.in_use(), 2U);
    EXPECT_EQ(pool.peak_in_use(), 2U);

    hypersync::buffer_as<int>(pool, *first) = 41;
    pool.release(*first);
    EXPECT_EQ(pool.available(), 1U);
    const auto recycled = pool.try_acquire();
    EXPECT_TRUE(recycled.has_value());
    EXPECT_EQ(recycled->index, first->index);

    EXPECT_THROW(pool.release(BufferHandle {88U, 99U, 1U}));
    pool.release(*second);
    pool.release(*recycled);
    EXPECT_THROW(pool.release(*recycled));
    EXPECT_THROW(static_cast<void>(pool.data(BufferHandle {88U, 99U, 1U})));
}

void test_raw_buffer_pool_is_thread_safe_under_parallel_acquire_release() {
    constexpr std::size_t kWorkers = 8;
    constexpr std::size_t kRounds = 32;

    RawBufferPool pool(89U, kWorkers, sizeof(int), alignof(int));
    EXPECT_EQ(pool.capacity(), kWorkers);

    for (std::size_t round = 0; round < kRounds; ++round) {
        std::vector<std::uint32_t> slots(kWorkers, 0U);
        std::atomic<bool> start{false};
        std::atomic<std::size_t> acquired{0};
        std::atomic<std::size_t> release_barrier{0};
        std::vector<std::thread> workers;
        workers.reserve(kWorkers);

        for (std::size_t index = 0; index < kWorkers; ++index) {
            workers.emplace_back([&, index] {
                while (!start.load()) {
                    std::this_thread::yield();
                }

                std::optional<BufferHandle> handle;
                while (!(handle = pool.try_acquire()).has_value()) {
                    std::this_thread::yield();
                }

                slots[index] = handle->index;
                hypersync::buffer_as<int>(pool, *handle) = static_cast<int>(index);
                acquired.fetch_add(1);
                while (acquired.load() != kWorkers) {
                    std::this_thread::yield();
                }

                release_barrier.fetch_add(1);
                while (release_barrier.load() != kWorkers) {
                    std::this_thread::yield();
                }

                pool.release(*handle);
            });
        }

        start.store(true);
        for (auto& worker : workers) {
            worker.join();
        }

        const std::set<std::uint32_t> unique_slots(slots.begin(), slots.end());
        EXPECT_EQ(unique_slots.size(), kWorkers);
        EXPECT_EQ(pool.available(), kWorkers);
        EXPECT_EQ(pool.in_use(), 0U);
    }

    EXPECT_EQ(pool.peak_in_use(), kWorkers);
}

void test_buf_queue_transfers_handles_without_payload_allocation() {
    BufQueue queue(3);
    EXPECT_EQ(queue.capacity(), 3U);
    EXPECT_EQ(queue.ring_capacity(), 4U);
    EXPECT_EQ(queue.available_slots(), 3U);
    EXPECT_TRUE(queue.empty());

    const BufferHandle first {kMetadataBufferPoolId, 1U, 7U};
    const BufferHandle second {kMetadataBufferPoolId, 2U, 9U};
    const BufferHandle third {kDataBufferPoolId, 3U, 11U};
    const BufferHandle fourth {kDataBufferPoolId, 4U, 13U};

    EXPECT_TRUE(queue.try_push(first));
    EXPECT_TRUE(queue.try_push(second));
    EXPECT_TRUE(queue.try_push(third));
    EXPECT_TRUE(queue.full());
    EXPECT_EQ(queue.available_slots(), 0U);
    EXPECT_FALSE(queue.try_push(fourth));
    EXPECT_EQ(queue.size(), 3U);
    EXPECT_EQ(queue.high_watermark(), 3U);

    BufferHandle popped;
    EXPECT_TRUE(queue.try_pop(popped));
    EXPECT_EQ(popped, first);
    EXPECT_EQ(queue.available_slots(), 1U);
    EXPECT_TRUE(queue.try_pop(popped));
    EXPECT_EQ(popped, second);
    EXPECT_TRUE(queue.try_pop(popped));
    EXPECT_EQ(popped, third);
    EXPECT_FALSE(queue.try_pop(popped));
    EXPECT_EQ(queue.push_count(), 3U);
    EXPECT_EQ(queue.pop_count(), 3U);

    queue.close();
    EXPECT_TRUE(queue.closed());
    EXPECT_FALSE(queue.try_push(first));
}

void test_buf_queue_push_wait_blocks_until_space_is_available() {
    BufQueue queue(1);
    const BufferHandle first {kMetadataBufferPoolId, 1U, 1U};
    const BufferHandle second {kMetadataBufferPoolId, 2U, 1U};

    EXPECT_TRUE(queue.try_push(first));
    EXPECT_TRUE(queue.full());
    EXPECT_FALSE(queue.try_push(second));

    std::atomic<bool> worker_entered {false};
    std::atomic<bool> worker_returned {false};
    std::thread worker([&] {
        worker_entered.store(true, std::memory_order_release);
        worker_returned.store(queue.push_wait(second), std::memory_order_release);
    });

    while (!worker_entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(worker_returned.load(std::memory_order_acquire));

    BufferHandle popped;
    EXPECT_TRUE(queue.try_pop(popped));
    EXPECT_EQ(popped, first);
    worker.join();
    EXPECT_TRUE(worker_returned.load(std::memory_order_acquire));

    EXPECT_TRUE(queue.try_pop(popped));
    EXPECT_EQ(popped, second);
    EXPECT_TRUE(queue.empty());
}

void test_status_monitor_renders_jobs_queues_and_socket_requests() {
    TempDir temp("hypersync_status_monitor");
    const fs::path socket_path = temp.path / "status.sock";

    std::atomic<std::uint64_t> processed {12};
    std::atomic<std::uint64_t> bytes {64U * 1024U * 1024U};

    hypersync::StatusRegistry registry;
    registry.register_job("example_job", [&]() {
        hypersync::MonitorJobSnapshot snapshot;
        snapshot.name = "example_job";
        snapshot.running = true;
        snapshot.worker_count = 2;
        snapshot.processed_count = processed.load(std::memory_order_relaxed);
        snapshot.byte_count = bytes.load(std::memory_order_relaxed);
        snapshot.count_unit = "buffers";
        snapshot.detail = "phase=test";
        return snapshot;
    });
    registry.register_queue("example_queue", []() {
        hypersync::MonitorQueueSnapshot snapshot;
        snapshot.name = "example_queue";
        snapshot.capacity = 8;
        snapshot.depth = 4;
        snapshot.high_watermark = 6;
        snapshot.pushed = 12;
        snapshot.popped = 8;
        return snapshot;
    });

    const std::string rendered = registry.render_human();
    EXPECT_TRUE(rendered.find("example_job") != std::string::npos);
    EXPECT_TRUE(rendered.find("example_queue") != std::string::npos);
    EXPECT_TRUE(rendered.find("GB/s") != std::string::npos);
    EXPECT_TRUE(rendered.find("full=50.0%") != std::string::npos);

    hypersync::StatusServer server(socket_path, registry);
    server.start();
    const std::string response = hypersync::request_status(socket_path);
    server.stop();

    EXPECT_TRUE(response.find("Status elapsed=") != std::string::npos);
    EXPECT_TRUE(response.find("example_job") != std::string::npos);
    EXPECT_TRUE(response.find("phase=test") != std::string::npos);
    EXPECT_FALSE(fs::exists(socket_path));
}

void test_threaded_job_runtime_metrics_report_wait_states() {
    RawBufferPool raw_pool(77U, 1U, 128U);
    BufferPoolRegistry pool_registry;
    pool_registry.register_pool(raw_pool);
    BufQueue input(2U);

    BufferDiscarderJob discarder(BufferDiscarderConfig(1U), input, pool_registry);
    discarder.start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    hypersync::RuntimeMetricsSnapshot snapshot;
    do {
        snapshot = discarder.runtime_metrics().snapshot();
        if (snapshot.current_workers[hypersync::runtime_state_index(hypersync::RuntimeState::wait_input_empty)] == 1U) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);

    EXPECT_EQ(snapshot.current_workers[hypersync::runtime_state_index(hypersync::RuntimeState::wait_input_empty)],
              1U);
    EXPECT_TRUE(snapshot.total_wall_ns > 0U);

    hypersync::StatusRegistry status_registry;
    status_registry.register_job("runtime_wait_job", [&]() {
        hypersync::MonitorJobSnapshot job;
        job.name = "runtime_wait_job";
        job.running = discarder.running();
        job.worker_count = discarder.worker_count();
        job.processed_count = discarder.stats().buffers_discarded;
        job.count_unit = "buffers";
        job.has_runtime_metrics = true;
        job.runtime_metrics = discarder.runtime_metrics().snapshot();
        return job;
    });
    const std::string rendered = status_registry.render_human();
    EXPECT_TRUE(rendered.find("process_cpu=") != std::string::npos);
    EXPECT_TRUE(rendered.find("current=") != std::string::npos);
    EXPECT_TRUE(rendered.find("start=") != std::string::npos);
    EXPECT_TRUE(rendered.find("peak=") != std::string::npos);
    EXPECT_TRUE(rendered.find("wait_in=") != std::string::npos);
    EXPECT_TRUE(rendered.find("now=wait_input:1") != std::string::npos);

    discarder.stop();
}

void test_autoscaler_recommends_cooperative_worker_limits() {
    AutoScalePolicy policy;
    policy.enabled = true;
    policy.min_workers = 2;
    policy.max_workers = 16;
    policy.initial_workers = 4;
    policy.cooldown_samples = 1;
    policy.max_cooldown_samples = 1;
    policy.backoff_confirmation_samples = 1;
    AutoScaler scaler(policy);
    EXPECT_EQ(scaler.active_workers(), 4U);

    AutoScaleMetrics pressure;
    pressure.input_fullness = 0.95;
    pressure.output_fullness = 0.10;
    pressure.busy_ratio = 0.90;
    pressure.throughput_per_second = 1000.0;
    AutoScaleDecision decision = scaler.update(pressure);
    EXPECT_TRUE(decision.changed);
    EXPECT_EQ(decision.active_workers, 8U);

    pressure.throughput_per_second = 400.0;
    decision = scaler.update(pressure);
    EXPECT_TRUE(decision.changed);
    EXPECT_EQ(decision.active_workers, 16U);

    AutoScaleMetrics output_blocked;
    output_blocked.input_fullness = 0.90;
    output_blocked.output_fullness = 0.98;
    output_blocked.busy_ratio = 0.90;
    output_blocked.throughput_per_second = 300.0;
    decision = scaler.update(output_blocked);
    EXPECT_TRUE(decision.changed);
    EXPECT_TRUE(decision.active_workers < 16U);

    RawBufferPool raw_pool(81U, 1U, 128U);
    BufferPoolRegistry registry;
    registry.register_pool(raw_pool);
    BufQueue input(2U);
    BufferDiscarderJob discarder(BufferDiscarderConfig(4U), input, registry);
    EXPECT_EQ(discarder.worker_count(), 4U);
    EXPECT_EQ(discarder.set_active_worker_limit(2U), 2U);
    EXPECT_EQ(discarder.active_worker_limit(), 2U);
    EXPECT_EQ(discarder.set_active_worker_limit(100U), 4U);
    EXPECT_EQ(discarder.set_active_worker_limit(0U), 1U);

    AutoScalePolicy runner_policy = policy;
    runner_policy.initial_workers = 2;
    std::atomic<bool> callback_seen {false};
    JobAutoScaleRunner runner(discarder,
                              runner_policy,
                              [] {
                                  AutoScaleMetrics metrics;
                                  metrics.input_fullness = 0.95;
                                  metrics.output_fullness = 0.0;
                                  metrics.busy_ratio = 0.95;
                                  metrics.throughput_per_second = 10.0;
                                  return metrics;
                              },
                              std::chrono::milliseconds(5));
    runner.set_decision_callback([&callback_seen](const AutoScaleDecision& decision) {
        if (decision.changed) {
            callback_seen.store(true, std::memory_order_release);
        }
    });
    runner.start();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!callback_seen.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    runner.stop();
    EXPECT_TRUE(callback_seen.load(std::memory_order_acquire));
    EXPECT_TRUE(discarder.active_worker_limit() > 2U);

    AutoScalePolicy overload_policy;
    overload_policy.enabled = true;
    overload_policy.min_workers = 4;
    overload_policy.max_workers = 32;
    overload_policy.initial_workers = 8;
    overload_policy.cooldown_samples = 1;
    overload_policy.max_cooldown_samples = 1;
    overload_policy.initial_probe_step_ratio = 0.5;
    overload_policy.min_probe_step_ratio = 0.5;
    AutoScaler overload_scaler(overload_policy);
    AutoScaleMetrics overload_metrics;
    overload_metrics.overload_score = 1.50;
    overload_metrics.output_fullness = 0.0;
    decision = overload_scaler.update(overload_metrics);
    EXPECT_TRUE(decision.changed);
    EXPECT_EQ(decision.active_workers, 12U);
    EXPECT_TRUE(std::string(decision.reason) == "overload_pressure");

    overload_metrics.overload_score = 0.50;
    decision = overload_scaler.update(overload_metrics);
    EXPECT_TRUE(decision.changed);
    EXPECT_EQ(decision.active_workers, 6U);
    EXPECT_TRUE(std::string(decision.reason) == "overload_underload");
}

void test_pipeline_autoscaler_tunes_one_stage_then_advances() {
    RawBufferPool raw_pool(82U, 1U, 128U);
    BufferPoolRegistry registry;
    registry.register_pool(raw_pool);
    BufQueue input_a(2U);
    BufQueue input_b(2U);
    BufferDiscarderJob first(BufferDiscarderConfig(8U), input_a, registry);
    BufferDiscarderJob second(BufferDiscarderConfig(8U), input_b, registry);

    AutoScalePolicy policy;
    policy.enabled = true;
    policy.min_workers = 2;
    policy.max_workers = 8;
    policy.initial_workers = 2;
    policy.cooldown_samples = 1;
    policy.scale_up_input_fullness = 0.25;
    policy.busy_scale_up = 0.25;
    policy.min_improvement_ratio = 0.05;

    std::atomic<std::uint64_t> first_samples {0};
    std::atomic<std::uint64_t> second_samples {0};
    std::atomic<bool> advanced_to_second {false};
    std::atomic<bool> second_scaled {false};

    PipelineAutoScaleRunner runner(
        std::vector<PipelineAutoScaleRunner::Stage> {
            PipelineAutoScaleRunner::Stage {
                "first",
                &first,
                policy,
                [&first_samples] {
                    const std::uint64_t sample = first_samples.fetch_add(1, std::memory_order_relaxed);
                    AutoScaleMetrics metrics;
                    metrics.input_fullness = 0.95;
                    metrics.output_fullness = 0.0;
                    metrics.busy_ratio = 0.95;
                    metrics.throughput_per_second = sample == 0U ? 100.0 : 50.0;
                    return metrics;
                },
            },
            PipelineAutoScaleRunner::Stage {
                "second",
                &second,
                policy,
                [&second_samples] {
                    const std::uint64_t sample = second_samples.fetch_add(1, std::memory_order_relaxed);
                    AutoScaleMetrics metrics;
                    metrics.input_fullness = 0.95;
                    metrics.output_fullness = 0.0;
                    metrics.busy_ratio = 0.95;
                    metrics.throughput_per_second = 100.0 + static_cast<double>(sample) * 10.0;
                    return metrics;
                },
            },
        },
        std::chrono::milliseconds(5));

    runner.set_decision_callback([&](const PipelineAutoScaleRunner::StageDecision& decision) {
        if (decision.stage_advanced) {
            advanced_to_second.store(true, std::memory_order_release);
        }
        if (decision.stage_index == 1U && decision.decision.changed &&
            decision.decision.active_workers > 2U) {
            second_scaled.store(true, std::memory_order_release);
        }
    });
    runner.start();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while ((!advanced_to_second.load(std::memory_order_acquire) ||
            !second_scaled.load(std::memory_order_acquire)) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    runner.stop();

    EXPECT_TRUE(advanced_to_second.load(std::memory_order_acquire));
    EXPECT_TRUE(second_scaled.load(std::memory_order_acquire));
    EXPECT_EQ(runner.active_stage_index(), 1U);
    EXPECT_EQ(first.active_worker_limit(), 8U);
    EXPECT_TRUE(second.active_worker_limit() > 2U);
}

void test_split_bucket_priority_balances_eta() {
    SplitBucketPriorityInput input;
    input.small_total = 10'000'000;
    input.large_total = 200'000;
    input.large_total_bytes = 20'000'000'000;
    input.small_done = 1'000'000;
    input.large_done = 100'000;
    input.large_done_bytes = 10'000'000'000;
    input.small_files_per_second = 30'000.0;
    input.large_files_per_second = 2'000.0;
    input.large_bytes_per_second = 200'000'000.0;
    input.current_small_workers = 96;
    input.current_large_workers = 64;
    input.max_small_workers = 128;
    input.max_large_workers = 64;

    SplitBucketPriorityDecision decision = choose_split_bucket_priority_workers(input);
    EXPECT_TRUE(decision.small_eta_seconds > decision.large_eta_seconds);
    EXPECT_TRUE(decision.small_workers > input.current_small_workers);
    EXPECT_TRUE(decision.large_workers < input.current_large_workers);
    EXPECT_TRUE(decision.large_reader_small_priority_percent > 0U);

    input.small_total = 1'100'000;
    input.large_total = 10'000'000;
    input.large_total_bytes = 10'000'000'000'000;
    input.small_done = 1'000'000;
    input.large_done = 1'000'000;
    input.large_done_bytes = 1'000'000'000'000;
    input.small_files_per_second = 50'000.0;
    input.large_files_per_second = 500.0;
    input.large_bytes_per_second = 500'000'000.0;
    input.current_small_workers = 120;
    input.current_large_workers = 40;
    decision = choose_split_bucket_priority_workers(input);
    EXPECT_TRUE(decision.large_eta_seconds > decision.small_eta_seconds);
    EXPECT_TRUE(decision.large_workers > input.current_large_workers);
    EXPECT_TRUE(decision.small_workers < input.current_small_workers);
    EXPECT_EQ(decision.large_reader_small_priority_percent, 0U);

    input.small_total = 5'000'000;
    input.large_total = 1'000'000;
    input.large_total_bytes = 1'000'000'000'000;
    input.small_done = 1'000'000;
    input.large_done = 1'000'000;
    input.large_done_bytes = 1'000'000'000'000;
    input.small_files_per_second = 20'000.0;
    input.large_files_per_second = 1'000.0;
    input.large_bytes_per_second = 1'000'000'000.0;
    input.current_small_workers = 96;
    input.current_large_workers = 64;
    decision = choose_split_bucket_priority_workers(input);
    EXPECT_EQ(decision.large_workers, 1U);
    EXPECT_TRUE(decision.small_workers > 96U);
    EXPECT_EQ(decision.large_reader_small_priority_percent, 100U);
}

void test_split_scanner_capacity_follows_reader_borrowing() {
    SplitScannerCapacityDecision decision =
        choose_split_scanner_capacity(96, 16, 8, 0);
    EXPECT_EQ(decision.small_scanners, 96U);
    EXPECT_EQ(decision.large_scanners, 16U);

    decision = choose_split_scanner_capacity(96, 16, 8, 1);
    EXPECT_EQ(decision.small_scanners, 104U);
    EXPECT_EQ(decision.large_scanners, 8U);

    decision = choose_split_scanner_capacity(96, 6, 8, 75);
    EXPECT_EQ(decision.small_scanners, 96U);
    EXPECT_EQ(decision.large_scanners, 6U);
}

void test_bucket_path_overload_scores_reflect_eta_reservoir_and_wire_pressure() {
    BucketPathOverloadInput input;
    input.small_eta_seconds = 1'200.0;
    input.large_eta_seconds = 900.0;
    input.queued_small_files = 3'500'000;
    input.small_low_watermark_files = 4'000'000;
    input.small_high_watermark_files = 5'000'000;
    input.small_low_watermark_intervals = 3;
    input.queued_large_files = 50'000;
    input.large_queue_capacity_files = 50'000;
    input.total_gigabits_per_second = 120.0;
    input.large_gigabits_per_second = 110.0;
    BucketPathOverloadScores scores = evaluate_bucket_path_overload(input);
    EXPECT_TRUE(scores.small_score > 1.0);
    EXPECT_TRUE(scores.large_score > 1.0);

    input.small_eta_seconds = 800.0;
    input.large_eta_seconds = 1'200.0;
    input.queued_small_files = 5'000'000;
    input.small_low_watermark_intervals = 0;
    input.small_scanner_sleep_ratio = 0.75;
    input.queued_large_files = 1'000;
    input.total_gigabits_per_second = 196.0;
    input.large_gigabits_per_second = 180.0;
    scores = evaluate_bucket_path_overload(input);
    EXPECT_TRUE(scores.small_score < 1.0);
    EXPECT_TRUE(scores.large_score < 1.0);
}

void test_synthetic_profile_builder_detects_chronological_phases() {
    SyntheticProfileCaptureConfig config;
    config.block_file_count = 10;
    config.small_ratio_shift_threshold = 0.20;
    config.small_file_threshold_bytes = 128U * 1024U;
    SyntheticProfileBuilder builder(config);

    for (std::size_t index = 0; index < 10U; ++index) {
        builder.observe_file(SyntheticObservedFile {
            4U * 1024U,
            1000,
            1000,
            0644,
            12,
            3,
            10,
        });
    }
    for (std::size_t index = 0; index < 10U; ++index) {
        builder.observe_file(SyntheticObservedFile {
            16U * 1024U * 1024U,
            1000,
            1000,
            0644,
            12,
            3,
            10,
        });
    }

    const SyntheticWorkloadProfile profile = builder.finish();
    EXPECT_EQ(profile.phases.size(), 2U);
    EXPECT_EQ(profile.phases[0].small_file_count, 10U);
    EXPECT_EQ(profile.phases[0].large_file_count, 0U);
    EXPECT_EQ(profile.phases[1].small_file_count, 0U);
    EXPECT_EQ(profile.phases[1].large_file_count, 10U);
    EXPECT_EQ(profile.phases[0].size_file_counts[synthetic_size_bucket_index(4U * 1024U)], 10U);
    EXPECT_EQ(profile.phases[1].size_file_counts[synthetic_size_bucket_index(16U * 1024U * 1024U)], 10U);
}

void test_synthetic_replay_cursor_generates_gapless_deterministic_views() {
    SyntheticPhaseProfile phase;
    phase.name = "phase_0";
    phase.file_count = 4;
    phase.folder_count = 2;
    phase.size_file_counts[synthetic_size_bucket_index(128U * 1024U)] = 2;
    phase.size_file_counts[synthetic_size_bucket_index(1024U * 1024U)] = 2;
    phase.small_read_latency.p50_us = 11;
    phase.small_read_latency.p90_us = 22;
    phase.small_read_latency.p99_us = 33;
    phase.small_read_latency.max_us = 44;
    phase.large_read_latency.p50_us = 55;
    phase.large_read_latency.p90_us = 66;
    phase.large_read_latency.p99_us = 77;
    phase.large_read_latency.max_us = 88;

    SyntheticWorkloadProfile profile;
    profile.seed = 42;
    profile.small_file_threshold_bytes = 128U * 1024U;
    profile.phases.push_back(phase);

    SyntheticReplayConfig config;
    config.profile = profile;
    config.latency_enabled = true;
    SyntheticReplayCursor cursor(config);

    SyntheticFileView first;
    EXPECT_TRUE(cursor.next_file(first));
    EXPECT_TRUE(!first.path_view().empty());
    EXPECT_EQ(first.nfs_handle.size(), hypersync::kSyntheticHandleBytes);

    SyntheticReplayCursor repeat(config);
    SyntheticFileView repeated_first;
    EXPECT_TRUE(repeat.next_file(repeated_first));
    EXPECT_EQ(first.path_view(), repeated_first.path_view());
    EXPECT_TRUE(first.nfs_handle == repeated_first.nfs_handle);
    EXPECT_EQ(first.size_bytes, repeated_first.size_bytes);
    EXPECT_EQ(first.latency_us, repeated_first.latency_us);

    std::size_t small_count = first.small ? 1U : 0U;
    std::size_t large_count = first.small ? 0U : 1U;
    for (std::size_t index = 1; index < 4U; ++index) {
        SyntheticFileView file;
        EXPECT_TRUE(cursor.next_file(file));
        if (file.size_bytes <= profile.small_file_threshold_bytes) {
            EXPECT_TRUE(file.small);
            ++small_count;
        } else {
            EXPECT_TRUE(!file.small);
            ++large_count;
        }
        EXPECT_TRUE(file.latency_us != 0U);
    }
    EXPECT_EQ(small_count + large_count, 4U);
    EXPECT_TRUE(small_count > 0U);
    EXPECT_TRUE(large_count > 0U);
    SyntheticFileView done;
    EXPECT_TRUE(!cursor.next_file(done));
}

void test_synthetic_profile_backend_streams_metadata_and_data() {
    TempDir root("synthetic_profile_backend");
    const fs::path profile_path = root.path / "profile.txt";
    write_file(profile_path,
               "synthetic_profile_benchmark files_observed=4 phases=1 elapsed_s=0 files_per_second=0 "
               "logical_size_bytes=1183744 small_files=2 large_files=2\n"
               "phase index=0 name=phase_0 files=4 folders=2 small=2 large=2 "
               "logical_size_bytes=1183744 "
               "size_buckets=<=0:0/0,<=4096:1/4096,<=16384:1/8192,<=65536:0/0,"
               "<=131072:1/65536,<=1048576:1/1048576,<=16777216:0/0,"
               "<=134217728:0/0,<=1073741824:0/0,<=inf:0/0\n");

    const std::string url = "synthetic-profile://" + profile_path.string();
    auto backend = hypersync::make_nfs_backend(url);

    std::vector<FileSpec> files;
    std::deque<FileSpec> folders;
    folders.push_back(FileSpec {});
    std::uint64_t folders_seen = 0;
    while (!folders.empty()) {
        backend->scan_flat_folders(
            4,
            [&](bool) -> std::optional<FileSpec> {
                if (folders.empty()) {
                    return std::nullopt;
                }
                FileSpec folder = std::move(folders.front());
                folders.pop_front();
                return folder;
            },
            [] {
                return false;
            },
            [&](hypersync::FlatFolderScanBatch batch) {
                ++folders_seen;
                for (auto& child : batch.directories) {
                    folders.push_back(std::move(child));
                }
                for (auto& file : batch.files) {
                    files.push_back(std::move(file));
                }
            });
    }

    EXPECT_EQ(folders_seen, 2U);
    EXPECT_EQ(files.size(), 4U);
    EXPECT_TRUE(!files.front().rel_path.empty());
    EXPECT_TRUE(!files.front().nfs_handle.empty());
    EXPECT_TRUE(files.front().declared_size != 0U);

    RawBufferPool pool(kDataBufferPoolId,
                       4,
                       sizeof(hypersync::DataBuffer),
                       alignof(hypersync::DataBuffer));
    std::uint64_t bytes_read = 0;
    const std::uint64_t streamed = backend->read_file_raw_chunks_by_handle(
        files.front(),
        1,
        pool,
        [&](hypersync::RawFileChunk&& chunk) {
            bytes_read += hypersync::data_buffer(pool, chunk.handle).trailer.data_len;
            pool.release(chunk.handle);
        },
        [] {
            return false;
        },
        false);

    EXPECT_EQ(streamed, files.front().declared_size);
    EXPECT_EQ(bytes_read, files.front().declared_size);
}

void test_synthetic_profile_backend_feeds_batch_folders_recursively() {
    TempDir root("synthetic_profile_backend_recursive_batches");
    const fs::path profile_path = root.path / "profile.txt";
    write_file(profile_path,
               "synthetic_profile_benchmark files_observed=5000 phases=1 elapsed_s=0 files_per_second=0 "
               "logical_size_bytes=20480000 small_files=5000 large_files=0\n"
               "phase index=0 name=phase_0 files=5000 folders=2 small=5000 large=0 "
               "logical_size_bytes=20480000 "
               "size_buckets=<=0:0/0,<=4096:5000/20480000,<=16384:0/0,<=65536:0/0,"
               "<=131072:0/0,<=1048576:0/0,<=16777216:0/0,"
               "<=134217728:0/0,<=1073741824:0/0,<=inf:0/0\n");

    auto backend = hypersync::make_nfs_backend("synthetic-profile://" + profile_path.string());

    std::deque<FileSpec> folders;
    folders.push_back(FileSpec {});
    std::uint64_t files_seen = 0;
    std::uint64_t folders_seen = 0;
    while (!folders.empty()) {
        backend->scan_flat_folders(
            4,
            [&](bool) -> std::optional<FileSpec> {
                if (folders.empty()) {
                    return std::nullopt;
                }
                FileSpec folder = std::move(folders.front());
                folders.pop_front();
                return folder;
            },
            [] {
                return false;
            },
            [&](hypersync::FlatFolderScanBatch batch) {
                ++folders_seen;
                files_seen += batch.files.size();
                for (auto& child : batch.directories) {
                    folders.push_back(std::move(child));
                }
            });
    }

    EXPECT_EQ(folders_seen, 3U);
    EXPECT_EQ(files_seen, 5000U);
}

void test_synthetic_profile_backend_can_emulate_profile_latency() {
    TempDir root("synthetic_profile_backend_latency");
    const fs::path profile_path = root.path / "profile.txt";
    write_file(profile_path,
               "synthetic_profile_benchmark files_observed=1 phases=1 elapsed_s=0 files_per_second=0 "
               "logical_size_bytes=4096 small_files=1 large_files=0\n"
               "phase index=0 name=phase_0 files=1 folders=1 small=1 large=0 "
               "logical_size_bytes=4096 "
               "readdirplus_page_latency_p50_us=5000 p90_us=5000 p99_us=5000 max_us=5000 "
               "sampled_small_read_latency_p50_us=5000 p90_us=5000 p99_us=5000 "
               "sampled_large_read_latency_p50_us=0 p90_us=0 p99_us=0 "
               "size_buckets=<=0:0/0,<=4096:1/4096,<=16384:0/0,<=65536:0/0,"
               "<=131072:0/0,<=1048576:0/0,<=16777216:0/0,"
               "<=134217728:0/0,<=1073741824:0/0,<=inf:0/0\n");

    const std::string url = "synthetic-profile://" + profile_path.string() +
                            "?latency=all&latency-scale=1.0";
    auto backend = hypersync::make_nfs_backend(url);

    std::vector<FileSpec> files;
    std::deque<FileSpec> folders;
    folders.push_back(FileSpec {});
    const auto scan_started = std::chrono::steady_clock::now();
    while (!folders.empty()) {
        backend->scan_flat_folders(
            1,
            [&](bool) -> std::optional<FileSpec> {
                if (folders.empty()) {
                    return std::nullopt;
                }
                FileSpec folder = std::move(folders.front());
                folders.pop_front();
                return folder;
            },
            [] {
                return false;
            },
            [&](hypersync::FlatFolderScanBatch batch) {
                for (auto& child : batch.directories) {
                    folders.push_back(std::move(child));
                }
                for (auto& file : batch.files) {
                    files.push_back(std::move(file));
                }
            });
    }
    const double scan_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - scan_started).count();
    EXPECT_TRUE(scan_seconds >= 0.003);
    EXPECT_EQ(files.size(), 1U);

    RawBufferPool pool(kDataBufferPoolId,
                       2,
                       sizeof(hypersync::DataBuffer),
                       alignof(hypersync::DataBuffer));
    const auto read_started = std::chrono::steady_clock::now();
    const std::uint64_t streamed = backend->read_file_raw_chunks_by_handle(
        files.front(),
        1,
        pool,
        [&](hypersync::RawFileChunk&& chunk) {
            pool.release(chunk.handle);
        },
        [] {
            return false;
        },
        false);
    const double read_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - read_started).count();
    EXPECT_EQ(streamed, files.front().declared_size);
    EXPECT_TRUE(read_seconds >= 0.003);
}

void test_synthetic_profile_backend_can_fill_fast_prng_payload() {
    TempDir root("synthetic_profile_backend_prng_payload");
    const fs::path profile_path = root.path / "profile.txt";
    write_file(profile_path,
               "synthetic_profile_benchmark files_observed=1 phases=1 elapsed_s=0 files_per_second=0 "
               "logical_size_bytes=4096 small_files=1 large_files=0 seed=12345\n"
               "phase index=0 name=phase_0 files=1 folders=1 small=1 large=0 "
               "logical_size_bytes=4096 "
               "size_buckets=<=0:0/0,<=4096:1/4096,<=16384:0/0,<=65536:0/0,"
               "<=131072:0/0,<=1048576:0/0,<=16777216:0/0,"
               "<=134217728:0/0,<=1073741824:0/0,<=inf:0/0\n");

    auto backend = hypersync::make_nfs_backend("synthetic-profile://" + profile_path.string() +
                                               "?payload=prng");
    std::vector<std::byte> first(4096);
    std::vector<std::byte> second(4096);
    const std::uint64_t first_bytes =
        backend->read_file_into("phase_0/folder_0/file_0.bin", 4096, first.data(), first.size());
    const std::uint64_t second_bytes =
        backend->read_file_into("phase_0/folder_0/file_0.bin", 4096, second.data(), second.size());

    EXPECT_EQ(first_bytes, 4096U);
    EXPECT_EQ(second_bytes, 4096U);
    EXPECT_EQ(first, second);
    EXPECT_TRUE(std::any_of(first.begin(), first.end(), [](std::byte value) {
        return value != std::byte {0};
    }));

    std::vector<std::byte> other(4096);
    const std::uint64_t other_bytes =
        backend->read_file_into("phase_0/folder_0/file_1.bin", 4096, other.data(), other.size());
    EXPECT_EQ(other_bytes, 4096U);
    EXPECT_TRUE(first != other);

    hypersync::DataSlotPool slot_pool(1, 2);
    std::uint64_t pooled_bytes = 0;
    const std::uint64_t streamed = backend->read_file_pooled_chunks(
        "phase_0/folder_0/file_0.bin",
        4096,
        1,
        slot_pool,
        [&](hypersync::PooledFileChunk&& chunk) {
            const hypersync::DataBufTrailer& trailer = slot_pool.trailer(chunk.handle);
            EXPECT_EQ(chunk.offset, trailer.data_offset);
            pooled_bytes += trailer.data_len;
            slot_pool.release(chunk.handle);
        });
    EXPECT_EQ(streamed, 4096U);
    EXPECT_EQ(pooled_bytes, 4096U);
}

void test_synthetic_payload_pool_returns_preallocated_blocks() {
    SyntheticPayloadPool pool(4096, 1024 * 1024, SyntheticPayloadPattern::repeated, 7);
    EXPECT_EQ(pool.small_block_bytes(), 4096U);
    EXPECT_EQ(pool.large_block_bytes(), 1024U * 1024U);

    SyntheticFileView small;
    small.small = true;
    small.size_bytes = 1234;
    SyntheticPayloadView small_payload = pool.payload_for(small);
    EXPECT_TRUE(small_payload.data != nullptr);
    EXPECT_EQ(small_payload.size, 1234U);
    EXPECT_EQ(small_payload.capacity, 4096U);

    SyntheticFileView large;
    large.small = false;
    large.size_bytes = 8U * 1024U * 1024U;
    SyntheticPayloadView large_payload = pool.payload_for(large);
    EXPECT_TRUE(large_payload.data != nullptr);
    EXPECT_EQ(large_payload.size, 1024U * 1024U);
    EXPECT_EQ(large_payload.capacity, 1024U * 1024U);

    SyntheticPayloadView small_payload_again = pool.payload_for(small);
    EXPECT_TRUE(small_payload.data == small_payload_again.data);
}

void test_autoscale_profile_store_defaults_and_persists_learned_workers() {
    TempDir root("autoscale_profile_store");
    const fs::path profile_path = root.path / "autoscale.yaml";

    AutoScaleProfileStore store(profile_path);
    AutoScalePolicy unknown_policy = store.job_policy("scan_pipeline", "unknown_reader", 0U);
    EXPECT_TRUE(unknown_policy.enabled);
    EXPECT_EQ(unknown_policy.min_workers, 1U);
    EXPECT_EQ(unknown_policy.initial_workers, 1U);
    EXPECT_TRUE(unknown_policy.max_workers >= 1U);

    AutoScalePolicy capped_policy = store.job_policy("scan_pipeline", "capped_reader", 4U);
    EXPECT_EQ(capped_policy.max_workers, 4U);

    store.update_learned_workers("scan_pipeline", "unknown_reader", 17U);
    store.save();

    AutoScaleProfileStore reloaded(profile_path);
    AutoScalePolicy learned_policy = reloaded.job_policy("scan_pipeline", "unknown_reader", 64U);
    EXPECT_EQ(learned_policy.initial_workers, 17U);
    EXPECT_TRUE(learned_policy.enabled);

    std::ifstream input(profile_path);
    std::stringstream contents;
    contents << input.rdbuf();
    EXPECT_TRUE(contents.str().find("autoscale_profiles:") != std::string::npos);
    EXPECT_TRUE(contents.str().find("learned_workers: 17") != std::string::npos);
    EXPECT_TRUE(contents.str().find("max_workers: auto") != std::string::npos);
}

void test_periodic_status_reporter_reuses_status_registry() {
    std::atomic<std::uint64_t> processed {0};
    std::atomic<std::uint64_t> reports {0};

    hypersync::StatusRegistry status_registry;
    status_registry.register_job("periodic_job", [&]() {
        hypersync::MonitorJobSnapshot job;
        job.name = "periodic_job";
        job.running = true;
        job.worker_count = 1;
        job.processed_count = processed.fetch_add(1, std::memory_order_relaxed);
        job.count_unit = "records";
        return job;
    });

    hypersync::PeriodicStatusReporter reporter(
        status_registry,
        std::chrono::milliseconds(5),
        [&](std::string text) {
            if (text.find("periodic_job") != std::string::npos &&
                text.find("current=") != std::string::npos) {
                reports.fetch_add(1, std::memory_order_relaxed);
            }
        });
    reporter.start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (reports.load(std::memory_order_acquire) == 0U &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    reporter.stop();

    EXPECT_FALSE(reporter.running());
    EXPECT_TRUE(reports.load(std::memory_order_acquire) > 0U);
}

void test_raw_buffer_pool_allocates_byte_slots_and_registry_discards_any_handle() {
    RawBufferPool raw_pool(77U, 2U, 128U);
    EXPECT_EQ(raw_pool.pool_id(), 77U);
    EXPECT_EQ(raw_pool.capacity(), 2U);
    EXPECT_EQ(raw_pool.buffer_size_bytes(), 128U);
    EXPECT_TRUE(raw_pool.stride_size_bytes() >= raw_pool.buffer_size_bytes());
    EXPECT_EQ(raw_pool.total_size_bytes(), raw_pool.capacity() * raw_pool.stride_size_bytes());

    const BufferHandle first = raw_pool.acquire_spin();
    EXPECT_EQ(first.pool_id, 77U);
    std::byte* bytes = raw_pool.data(first);
    bytes[0] = std::byte{0x2a};
    bytes[127] = std::byte{0x7f};
    EXPECT_EQ(raw_pool.data(first)[0], std::byte{0x2a});
    EXPECT_EQ(raw_pool.data(first)[127], std::byte{0x7f});

    RawBufferPool metadata_pool = hypersync::make_metadata_buffer_pool(1);
    RawBufferPool data_pool = hypersync::make_data_buffer_pool(1);
    BufferPoolRegistry registry;
    registry.register_pool(raw_pool);
    registry.register_pool(metadata_pool);
    registry.register_pool(data_pool);

    const BufferHandle metadata = metadata_pool.acquire_spin();
    const BufferHandle data = data_pool.acquire_spin();
    BufQueue queue(3);
    EXPECT_TRUE(queue.try_push(first));
    EXPECT_TRUE(queue.try_push(metadata));
    EXPECT_TRUE(queue.try_push(data));

    BufferHandle popped;
    while (queue.try_pop(popped)) {
        registry.release(popped);
    }

    EXPECT_EQ(raw_pool.available(), 2U);
    EXPECT_EQ(metadata_pool.available(), 1U);
    EXPECT_EQ(data_pool.available(), 1U);
}

void test_raw_metadata_buffer_view_detects_stale_and_wrong_handles() {
    RawBufferPool pool = hypersync::make_metadata_buffer_pool(1);
    EXPECT_EQ(pool.capacity(), 1U);
    EXPECT_EQ(pool.buffer_size_bytes(), sizeof(hypersync::MetadataBuffer));
    EXPECT_EQ(pool.total_size_bytes(), sizeof(hypersync::MetadataBuffer));
    EXPECT_TRUE(pool.stride_size_bytes() >= pool.buffer_size_bytes());
    EXPECT_EQ(pool.available(), 1U);

    const BufferHandle first = pool.acquire_spin();
    EXPECT_EQ(first.pool_id, kMetadataBufferPoolId);
    EXPECT_EQ(first.index, 0U);
    EXPECT_EQ(first.generation, 1U);
    EXPECT_EQ(pool.in_use(), 1U);
    EXPECT_EQ(pool.available(), 0U);

    auto& buffer = hypersync::metadata_buffer(pool, first);
    buffer.record_kind = MetadataBufferRecordKind::file;
    buffer.bytes_used = 123U;
    buffer.file_id = 42U;

    EXPECT_THROW(pool.release(BufferHandle {kDataBufferPoolId, first.index, first.generation}));
    pool.release(first);
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_EQ(pool.available(), 1U);
    EXPECT_THROW(pool.release(first));
    EXPECT_THROW(static_cast<void>(hypersync::metadata_buffer(pool, first)));

    const BufferHandle second = pool.acquire_spin();
    EXPECT_EQ(second.index, first.index);
    EXPECT_TRUE(second.generation != first.generation);
    EXPECT_EQ(hypersync::metadata_buffer(pool, second).file_id, 42U);
    pool.release(second);
}

void test_raw_buffer_pool_and_buf_queue_are_mpmc_safe() {
    constexpr std::size_t kPoolSlots = 256;
    constexpr std::size_t kQueueSlots = 128;
    constexpr std::size_t kProducers = 4;
    constexpr std::size_t kConsumers = 4;
    constexpr std::size_t kItemsPerProducer = 4000;
    constexpr std::size_t kTotalItems = kProducers * kItemsPerProducer;

    RawBufferPool pool = hypersync::make_metadata_buffer_pool(kPoolSlots);
    BufQueue queue(kQueueSlots);
    std::atomic<bool> start {false};
    std::atomic<std::size_t> produced {0};
    std::atomic<std::size_t> consumed {0};
    std::atomic<std::uint64_t> checksum {0};

    std::vector<std::thread> consumers;
    consumers.reserve(kConsumers);
    for (std::size_t consumer = 0; consumer < kConsumers; ++consumer) {
        (void)consumer;
        consumers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (consumed.load(std::memory_order_acquire) < kTotalItems) {
                BufferHandle handle;
                if (!queue.try_pop(handle)) {
                    std::this_thread::yield();
                    continue;
                }
                auto& buffer = hypersync::metadata_buffer(pool, handle);
                checksum.fetch_add(buffer.file_id, std::memory_order_relaxed);
                buffer.record_kind = MetadataBufferRecordKind::empty;
                buffer.bytes_used = 0U;
                pool.release(handle);
                consumed.fetch_add(1U, std::memory_order_acq_rel);
            }
        });
    }

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (std::size_t producer = 0; producer < kProducers; ++producer) {
        producers.emplace_back([&, producer] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (std::size_t item = 0; item < kItemsPerProducer; ++item) {
                const std::uint64_t value =
                    static_cast<std::uint64_t>(producer * kItemsPerProducer + item + 1U);
                BufferHandle handle = pool.acquire_spin();
                auto& buffer = hypersync::metadata_buffer(pool, handle);
                buffer.record_kind = MetadataBufferRecordKind::file;
                buffer.bytes_used = sizeof(value);
                buffer.file_id = value;
                while (!queue.try_push(handle)) {
                    std::this_thread::yield();
                }
                produced.fetch_add(1U, std::memory_order_acq_rel);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& producer : producers) {
        producer.join();
    }
    while (consumed.load(std::memory_order_acquire) < kTotalItems) {
        std::this_thread::yield();
    }
    for (auto& consumer : consumers) {
        consumer.join();
    }

    const std::uint64_t expected_checksum =
        static_cast<std::uint64_t>(kTotalItems) * static_cast<std::uint64_t>(kTotalItems + 1U) / 2U;
    EXPECT_EQ(produced.load(), kTotalItems);
    EXPECT_EQ(consumed.load(), kTotalItems);
    EXPECT_EQ(checksum.load(), expected_checksum);
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_EQ(pool.available(), kPoolSlots);
    EXPECT_TRUE(queue.empty());
    EXPECT_TRUE(pool.peak_in_use() <= kPoolSlots);
    EXPECT_TRUE(queue.high_watermark() <= queue.capacity());
}

void test_sharded_buf_queue_steals_from_busy_shards() {
    ShardedBufQueue queue(4U, 8U);
    EXPECT_EQ(queue.shard_count(), 4U);
    EXPECT_EQ(queue.capacity(), 32U);

    const BufferHandle first {kMetadataBufferPoolId, 1U, 1U};
    const BufferHandle second {kMetadataBufferPoolId, 2U, 1U};
    const BufferHandle third {kMetadataBufferPoolId, 3U, 1U};
    EXPECT_TRUE(queue.try_push(2U, first));
    EXPECT_TRUE(queue.try_push(2U, second));
    EXPECT_TRUE(queue.try_push(2U, third));
    EXPECT_EQ(queue.size(), 3U);

    BufferHandle popped;
    EXPECT_TRUE(queue.try_pop(0U, popped));
    EXPECT_EQ(popped, first);
    EXPECT_TRUE(queue.try_pop(1U, popped));
    EXPECT_EQ(popped, second);
    EXPECT_TRUE(queue.try_pop(3U, popped));
    EXPECT_EQ(popped, third);
    EXPECT_FALSE(queue.try_pop(0U, popped));
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.push_count(), 3ULL);
    EXPECT_EQ(queue.pop_count(), 3ULL);
    EXPECT_TRUE(queue.high_watermark() >= 3U);

    queue.close();
    EXPECT_TRUE(queue.closed());
    EXPECT_FALSE(queue.try_push(0U, first));
}

void test_buffer_generator_writes_configured_patterns_to_raw_buffers() {
    RawBufferPool pool(90U, 4U, 64U);
    BufQueue queue(4U);

    BufferGeneratorJob zero_generator(
        BufferGeneratorConfig(2U, 3U, BufferGeneratorPattern::zero, 123U), pool, queue);
    zero_generator.start();
    zero_generator.wait();

    std::size_t popped_count = 0;
    BufferHandle handle;
    while (queue.try_pop(handle)) {
        const std::byte* bytes = pool.data(handle);
        for (std::size_t index = 0; index < pool.buffer_size_bytes(); ++index) {
            EXPECT_EQ(bytes[index], std::byte{0});
        }
        pool.release(handle);
        ++popped_count;
    }
    EXPECT_EQ(popped_count, 3U);
    EXPECT_EQ(pool.available(), 4U);
    EXPECT_EQ(zero_generator.stats().buffers_generated, 3ULL);

    RawBufferPool text_pool(91U, 2U, 128U);
    BufQueue text_queue(2U);
    BufferGeneratorJob text_generator(
        BufferGeneratorConfig(1U, 1U, BufferGeneratorPattern::fast_text, 456U), text_pool, text_queue);
    text_generator.start();
    text_generator.wait();
    EXPECT_TRUE(text_queue.try_pop(handle));
    const std::byte* text = text_pool.data(handle);
    bool saw_nonzero = false;
    for (std::size_t index = 0; index < text_pool.buffer_size_bytes(); ++index) {
        const unsigned char ch = static_cast<unsigned char>(text[index]);
        EXPECT_TRUE(ch == '-' || ch == '_' || (ch >= '0' && ch <= '9') ||
                    (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z'));
        saw_nonzero = saw_nonzero || ch != 0U;
    }
    EXPECT_TRUE(saw_nonzero);
    text_pool.release(handle);

    EXPECT_EQ(hypersync::parse_buffer_generator_pattern("random_text"), BufferGeneratorPattern::fast_text);
    EXPECT_EQ(hypersync::parse_buffer_generator_pattern("pseudo_random"), BufferGeneratorPattern::xoshiro256);
    EXPECT_EQ(hypersync::parse_buffer_generator_pattern("xoshiro256"), BufferGeneratorPattern::xoshiro256);
    EXPECT_EQ(hypersync::parse_buffer_generator_pattern("fast_text"), BufferGeneratorPattern::fast_text);

    RawBufferPool fast_text_pool(93U, 2U, 128U);
    BufQueue fast_text_queue(2U);
    BufferGeneratorJob fast_text_generator(
        BufferGeneratorConfig(1U, 1U, BufferGeneratorPattern::fast_text, 654U), fast_text_pool, fast_text_queue);
    fast_text_generator.start();
    fast_text_generator.wait();
    EXPECT_TRUE(fast_text_queue.try_pop(handle));
    const std::byte* fast_text = fast_text_pool.data(handle);
    for (std::size_t index = 0; index < fast_text_pool.buffer_size_bytes(); ++index) {
        const unsigned char ch = static_cast<unsigned char>(fast_text[index]);
        EXPECT_TRUE(ch == '-' || ch == '_' || (ch >= '0' && ch <= '9') ||
                    (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z'));
    }
    fast_text_pool.release(handle);

    RawBufferPool random_pool(94U, 2U, 128U);
    BufQueue random_queue(2U);
    BufferGeneratorJob random_generator(
        BufferGeneratorConfig(1U, 1U, BufferGeneratorPattern::xoshiro256, 987U), random_pool, random_queue);
    random_generator.start();
    random_generator.wait();
    EXPECT_TRUE(random_queue.try_pop(handle));
    const std::byte* random_bytes = random_pool.data(handle);
    bool random_saw_nonzero = false;
    for (std::size_t index = 0; index < random_pool.buffer_size_bytes(); ++index) {
        random_saw_nonzero = random_saw_nonzero || random_bytes[index] != std::byte{0};
    }
    EXPECT_TRUE(random_saw_nonzero);
    random_pool.release(handle);

    RawBufferPool compressed_pool(95U, 2U, 100U);
    BufQueue compressed_queue(2U);
    BufferGeneratorJob compressed_generator(
        BufferGeneratorConfig(1U, 1U, BufferGeneratorPattern::xoshiro256, 987U, 2.0), compressed_pool, compressed_queue);
    compressed_generator.start();
    compressed_generator.wait();
    EXPECT_TRUE(compressed_queue.try_pop(handle));
    const std::byte* compressed_bytes = compressed_pool.data(handle);
    bool compressed_prefix_saw_nonzero = false;
    for (std::size_t index = 0; index < 50U; ++index) {
        compressed_prefix_saw_nonzero = compressed_prefix_saw_nonzero || compressed_bytes[index] != std::byte{0};
    }
    EXPECT_TRUE(compressed_prefix_saw_nonzero);
    for (std::size_t index = 50U; index < compressed_pool.buffer_size_bytes(); ++index) {
        EXPECT_EQ(compressed_bytes[index], std::byte{0});
    }
    compressed_pool.release(handle);
}

void test_buffer_generator_to_discarder_pipeline_releases_all_buffers() {
    constexpr std::uint64_t kBufferCount = 5000;
    RawBufferPool pool(92U, 128U, 1024U);
    BufQueue queue(64U);
    BufferPoolRegistry registry;
    registry.register_pool(pool);

    BufferDiscarderJob discarder(BufferDiscarderConfig(3U), queue, registry);
    BufferGeneratorJob generator(
        BufferGeneratorConfig(4U, kBufferCount, BufferGeneratorPattern::xoshiro256, 789U), pool, queue);

    discarder.start();
    generator.start();
    generator.wait();
    discarder.wait();

    const auto generator_stats = generator.stats();
    const auto discarder_stats = discarder.stats();
    EXPECT_EQ(generator_stats.buffers_generated, kBufferCount);
    EXPECT_EQ(generator_stats.bytes_generated, kBufferCount * pool.buffer_size_bytes());
    EXPECT_EQ(discarder_stats.buffers_discarded, kBufferCount);
    EXPECT_EQ(discarder_stats.bytes_discarded, kBufferCount * pool.buffer_size_bytes());
    EXPECT_EQ(pool.available(), pool.capacity());
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_TRUE(queue.empty());
}

void test_buffer_generator_to_sharded_discarder_pipeline_steals_and_releases_all_buffers() {
    constexpr std::uint64_t kBufferCount = 7000;
    RawBufferPool pool(93U, 128U, 1024U);
    ShardedBufQueue queue(8U, 16U);
    BufferPoolRegistry registry;
    registry.register_pool(pool);

    BufferDiscarderJob discarder(BufferDiscarderConfig(16U), queue, registry);
    BufferGeneratorJob generator(
        BufferGeneratorConfig(4U, kBufferCount, BufferGeneratorPattern::xoshiro256, 2468U), pool, queue);

    discarder.start();
    generator.start();
    generator.wait();
    discarder.wait();

    const auto generator_stats = generator.stats();
    const auto discarder_stats = discarder.stats();
    EXPECT_EQ(generator_stats.buffers_generated, kBufferCount);
    EXPECT_EQ(discarder_stats.buffers_discarded, kBufferCount);
    EXPECT_EQ(pool.available(), pool.capacity());
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.push_count(), kBufferCount);
    EXPECT_EQ(queue.pop_count(), kBufferCount);
}

void test_buffer_transport_moves_raw_buffers_over_tcp_and_unix() {
    const auto run_case = [](const BufferTransportEndpoint& endpoint) {
        RawBufferPool sender_pool(hypersync::kMetadataBufferPoolId, 8U, sizeof(hypersync::MetadataBuffer), alignof(hypersync::MetadataBuffer));
        RawBufferPool receiver_pool(hypersync::kMetadataBufferPoolId, 8U, sizeof(hypersync::MetadataBuffer), alignof(hypersync::MetadataBuffer));
        BufferPoolRegistry sender_registry;
        sender_registry.register_pool(sender_pool);

        BufQueue sender_queue(8U);
        BufQueue receiver_queue(8U);
        BufferReceiverJob receiver(1U, receiver_pool, receiver_queue, endpoint);
        BufferSenderJob sender(1U, sender_queue, sender_registry, endpoint);

        receiver.start();
        sender.start();

        for (std::size_t index = 0; index < 3U; ++index) {
            const BufferHandle handle = *sender_pool.try_acquire();
            auto& buffer = hypersync::metadata_buffer(sender_pool, handle);
            FileSpec file;
            file.rel_path = "transport/file_" + std::to_string(index) + ".dat";
            file.declared_size = 100U + index;
            file.mtime = 200U + index;
            file.mode = 0644;
            file.uid = 1000;
            file.gid = 1000;
            EXPECT_TRUE(hypersync::encode_metadata_file_record(buffer, file));
            EXPECT_TRUE(sender_queue.push_wait(handle));
        }
        sender_queue.close();
        sender.wait();
        receiver.wait();

        EXPECT_EQ(sender_pool.available(), sender_pool.capacity());
        EXPECT_EQ(sender.stats().buffers, 3ULL);
        EXPECT_EQ(receiver.stats().buffers, 3ULL);

        std::set<std::string> paths;
        BufferHandle received;
        while (receiver_queue.try_pop(received)) {
            const auto file = hypersync::decode_metadata_file_record(hypersync::metadata_buffer(receiver_pool, received));
            paths.insert(file.rel_path);
            receiver_pool.release(received);
        }
        EXPECT_EQ(paths.size(), 3U);
        EXPECT_TRUE(paths.count("transport/file_0.dat") == 1U);
        EXPECT_TRUE(paths.count("transport/file_1.dat") == 1U);
        EXPECT_TRUE(paths.count("transport/file_2.dat") == 1U);
        EXPECT_EQ(receiver_pool.available(), receiver_pool.capacity());
    };

    run_case(BufferTransportEndpoint::tcp("127.0.0.1", pick_unused_port()));

    const fs::path unix_socket_path =
        fs::path("/tmp") / ("wsync_bt_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock");
    run_case(BufferTransportEndpoint::unix_socket(unix_socket_path));
    std::error_code ignored;
    fs::remove(unix_socket_path, ignored);
}

void test_compact_folder_metadata_batch_round_trip() {
    MetadataFolderRecord folder;
    folder.spec.rel_path = "writer/nested";
    folder.spec.mtime = 9000;
    folder.spec.mode = 0755;
    folder.spec.uid = 1000;
    folder.spec.gid = 1001;
    folder.flat_file_count = 16;
    folder.flat_logical_size_bytes = 16 * 4096;

    std::vector<FileSpec> input_files;
    for (std::size_t index = 0; index < 16U; ++index) {
        FileSpec file;
        file.rel_path = "writer/nested/file_" + std::to_string(index) + ".dat";
        file.declared_size = 4096 + index;
        file.mtime = 10'000 + index;
        file.mode = 0644;
        file.uid = 2000 + static_cast<std::uint32_t>(index);
        file.gid = 3000 + static_cast<std::uint32_t>(index);
        input_files.push_back(std::move(file));
    }

    MetadataFolderRecord child_folder;
    child_folder.spec.rel_path = "writer/nested/child";
    child_folder.spec.mtime = 12'000;
    child_folder.spec.mode = 0750;
    child_folder.spec.uid = 4000;
    child_folder.spec.gid = 5000;
    child_folder.flat_file_count = 3;
    child_folder.flat_logical_size_bytes = 12'345;

    hypersync::MetadataBatchBuffer full_batch;
    hypersync::reset_metadata_batch(full_batch);
    EXPECT_TRUE(hypersync::append_metadata_batch_folder(full_batch, folder));
    for (const auto& file : input_files) {
        EXPECT_TRUE(hypersync::append_metadata_batch_file(full_batch, file));
    }
    EXPECT_TRUE(hypersync::append_metadata_batch_folder(full_batch, child_folder));

    hypersync::MetadataBatchBuffer compact_batch;
    EXPECT_TRUE(hypersync::reset_folder_metadata_batch(compact_batch, folder, true));
    for (const auto& file : input_files) {
        EXPECT_TRUE(hypersync::append_folder_metadata_batch_file(compact_batch, file));
    }
    EXPECT_TRUE(hypersync::append_folder_metadata_batch_folder(compact_batch, child_folder));
    EXPECT_TRUE(compact_batch.bytes_used < full_batch.bytes_used);

    std::vector<FileSpec> files;
    std::vector<MetadataFolderRecord> folders;
    hypersync::decode_metadata_batch(compact_batch, files, folders);
    EXPECT_EQ(files.size(), input_files.size());
    EXPECT_EQ(folders.size(), 2U);
    EXPECT_EQ(folders.front().spec.rel_path, folder.spec.rel_path);
    EXPECT_EQ(folders.front().flat_file_count, folder.flat_file_count);
    EXPECT_EQ(folders.front().flat_logical_size_bytes, folder.flat_logical_size_bytes);
    EXPECT_EQ(folders.back().spec.rel_path, child_folder.spec.rel_path);
    EXPECT_EQ(folders.back().flat_file_count, child_folder.flat_file_count);
    for (std::size_t index = 0; index < input_files.size(); ++index) {
        EXPECT_EQ(files[index].rel_path, input_files[index].rel_path);
        EXPECT_EQ(files[index].declared_size, input_files[index].declared_size);
        EXPECT_EQ(files[index].mtime, input_files[index].mtime);
        EXPECT_EQ(files[index].uid, input_files[index].uid);
        EXPECT_EQ(files[index].gid, input_files[index].gid);
    }

    hypersync::MetadataBatchBuffer continuation_batch;
    EXPECT_TRUE(hypersync::reset_folder_metadata_batch(continuation_batch, folder, false));
    EXPECT_TRUE(hypersync::append_folder_metadata_batch_file(continuation_batch, input_files.front()));
    files.clear();
    folders.clear();
    hypersync::decode_metadata_batch(continuation_batch, files, folders);
    EXPECT_EQ(files.size(), 1U);
    EXPECT_EQ(folders.size(), 0U);
    EXPECT_EQ(files.front().rel_path, input_files.front().rel_path);
}

void test_flat_folder_and_diff_result_buffer_codecs_round_trip() {
    FileSpec folder;
    folder.rel_path = "root/folder";
    folder.mtime = 123;
    folder.mode = 0755;
    folder.uid = 1000;
    folder.gid = 1001;

    FileSpec file;
    file.rel_path = "root/folder/file.dat";
    file.declared_size = 4096;
    file.mtime = 456;
    file.mode = 0644;
    file.uid = 2000;
    file.gid = 2001;

    FileSpec child_folder;
    child_folder.rel_path = "root/folder/child";
    child_folder.mtime = 789;
    child_folder.mode = 0750;

    hypersync::MetadataBatchBuffer folder_buffer;
    hypersync::reset_flat_folder_buffer(folder_buffer,
                                        folder,
                                        7,
                                        false,
                                        false,
                                        {},
                                        1,
                                        1,
                                        4096,
                                        0x1234,
                                        11,
                                        22);
    EXPECT_TRUE(hypersync::append_flat_folder_file(folder_buffer, file, "time"));
    EXPECT_TRUE(hypersync::append_flat_folder_folder(folder_buffer, child_folder, "time"));
    hypersync::set_flat_folder_buffer_final(folder_buffer, true);

    const auto info = hypersync::flat_folder_buffer_info(folder_buffer);
    EXPECT_TRUE(info.final_batch);
    EXPECT_EQ(info.sequence, 7U);
    EXPECT_EQ(info.folder_path, "root/folder");
    EXPECT_EQ(info.total_file_count, 1ULL);
    EXPECT_EQ(info.total_folder_count, 1ULL);
    EXPECT_EQ(info.total_logical_size_bytes, 4096ULL);
    EXPECT_EQ(info.metadata_hash, 0x1234ULL);

    std::size_t files = 0;
    std::size_t folders = 0;
    hypersync::visit_flat_folder_children(folder_buffer, [&](hypersync::FlatFolderChildView child) {
        if (child.is_file) {
            ++files;
            EXPECT_EQ(child.name, "file.dat");
            EXPECT_EQ(child.logical_size, 4096ULL);
        } else {
            ++folders;
            EXPECT_EQ(child.name, "child");
        }
    });
    EXPECT_EQ(files, 1U);
    EXPECT_EQ(folders, 1U);

    hypersync::MetadataBatchBuffer result_buffer;
    hypersync::reset_diff_result_buffer(result_buffer);
    hypersync::FolderDiffSummary summary;
    summary.rel_path = info.folder_path;
    summary.source_file_count = 2;
    summary.target_file_count = 2;
    summary.files_same = 1;
    summary.files_changed = 1;
    summary.bytes_planned = 4096;
    EXPECT_TRUE(hypersync::append_diff_result(result_buffer, summary));

    std::size_t result_count = 0;
    hypersync::visit_diff_results(result_buffer, [&](hypersync::FolderDiffSummary decoded) {
        ++result_count;
        EXPECT_EQ(decoded.rel_path, "root/folder");
        EXPECT_EQ(decoded.files_same, 1ULL);
        EXPECT_EQ(decoded.files_changed, 1ULL);
        EXPECT_EQ(decoded.bytes_planned, 4096ULL);
    });
    EXPECT_EQ(result_count, 1U);
}

void test_buffer_stream_transport_moves_raw_buffers_over_existing_fd() {
    int sockets[2] = {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
        throw std::runtime_error("socketpair failed");
    }
    hypersync::ScopedFd left(sockets[0]);
    hypersync::ScopedFd right(sockets[1]);

    RawBufferPool sender_pool(hypersync::kMetadataBufferPoolId,
                              8U,
                              sizeof(hypersync::MetadataBuffer),
                              alignof(hypersync::MetadataBuffer));
    RawBufferPool receiver_pool(77U,
                                8U,
                                sizeof(hypersync::MetadataBuffer),
                                alignof(hypersync::MetadataBuffer));
    BufferPoolRegistry sender_registry;
    sender_registry.register_pool(sender_pool);
    BufQueue sender_queue(8U);
    BufQueue receiver_queue(8U);

    hypersync::BufferStreamReceiverJob receiver(1U, receiver_pool, receiver_queue, right.get());
    hypersync::BufferStreamSenderJob sender(1U, sender_queue, sender_registry, left.get());
    receiver.start();
    sender.start();

    for (std::size_t index = 0; index < 2U; ++index) {
        const BufferHandle handle = *sender_pool.try_acquire();
        FileSpec file;
        file.rel_path = "stream/file_" + std::to_string(index);
        file.declared_size = 10 + index;
        EXPECT_TRUE(hypersync::encode_metadata_file_record(hypersync::metadata_buffer(sender_pool, handle), file));
        EXPECT_TRUE(sender_queue.push_wait(handle));
    }
    sender_queue.close();
    sender.wait();
    left.reset();
    receiver.wait();

    EXPECT_EQ(sender_pool.available(), sender_pool.capacity());
    EXPECT_EQ(sender.stats().buffers, 2ULL);
    EXPECT_EQ(receiver.stats().buffers, 2ULL);
    EXPECT_EQ(receiver_queue.size(), 2U);

    BufferHandle received;
    std::size_t decoded = 0;
    while (receiver_queue.try_pop(received)) {
        const FileSpec file = hypersync::decode_metadata_file_record(hypersync::metadata_buffer(receiver_pool, received));
        EXPECT_TRUE(file.rel_path == "stream/file_0" || file.rel_path == "stream/file_1");
        receiver_pool.release(received);
        ++decoded;
    }
    EXPECT_EQ(decoded, 2U);
    EXPECT_EQ(receiver_pool.available(), receiver_pool.capacity());
}

void test_buffer_transport_feeds_metadata_writer_job() {
    TempDir output("hypersync_transport_metadata_writer");
    const fs::path socket_path =
        fs::path("/tmp") / ("wsync_writer_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock");
    const fs::path csv_path = output.path / "records.csv";

    RawBufferPool sender_pool(hypersync::kMetadataBufferPoolId, 8U, sizeof(hypersync::MetadataBuffer), alignof(hypersync::MetadataBuffer));
    RawBufferPool receiver_pool(hypersync::kMetadataBufferPoolId, 8U, sizeof(hypersync::MetadataBuffer), alignof(hypersync::MetadataBuffer));
    BufferPoolRegistry sender_registry;
    sender_registry.register_pool(sender_pool);
    BufferPoolRegistry writer_registry;
    writer_registry.register_pool(receiver_pool);

    BufQueue sender_queue(8U);
    BufQueue writer_queue(8U);

    MetadataRecordWriterConfig writer_config(true,
                                             csv_path,
                                             MetadataRecordFormat::csv,
                                             true,
                                             true,
                                             false);
    BufferReceiverJob receiver(1U,
                               receiver_pool,
                               writer_queue,
                               BufferTransportEndpoint::unix_socket(socket_path));
    MetadataRecordWriterJob writer(1U, writer_queue, writer_registry, writer_config);
    BufferSenderJob sender(1U,
                           sender_queue,
                           sender_registry,
                           BufferTransportEndpoint::unix_socket(socket_path));

    receiver.start();
    writer.start();
    sender.start();

    const BufferHandle file_handle = *sender_pool.try_acquire();
    auto& file_buffer = hypersync::metadata_buffer(sender_pool, file_handle);
    FileSpec file;
    file.rel_path = "writer/file.dat";
    file.declared_size = 1234;
    file.mtime = 9876;
    file.mode = 0644;
    file.uid = 1000;
    file.gid = 1000;
    EXPECT_TRUE(hypersync::encode_metadata_file_record(file_buffer, file));
    EXPECT_TRUE(sender_queue.push_wait(file_handle));

    const BufferHandle folder_handle = *sender_pool.try_acquire();
    auto& folder_buffer = hypersync::metadata_buffer(sender_pool, folder_handle);
    MetadataFolderRecord folder;
    folder.spec.rel_path = "writer";
    folder.spec.mtime = 9870;
    folder.spec.mode = 0755;
    folder.spec.uid = 1000;
    folder.spec.gid = 1000;
    folder.flat_file_count = 1;
    folder.flat_logical_size_bytes = 1234;
    EXPECT_TRUE(hypersync::encode_metadata_folder_record(folder_buffer, folder));
    EXPECT_TRUE(sender_queue.push_wait(folder_handle));

    sender_queue.close();
    sender.wait();
    receiver.wait();
    writer.wait();

    EXPECT_EQ(sender_pool.available(), sender_pool.capacity());
    EXPECT_EQ(receiver_pool.available(), receiver_pool.capacity());
    EXPECT_EQ(sender.stats().buffers, 2ULL);
    EXPECT_EQ(receiver.stats().buffers, 2ULL);
    EXPECT_EQ(writer.writer_stats().files_written, 1ULL);
    EXPECT_EQ(writer.writer_stats().folders_written, 1ULL);

    const std::string csv = hypersync::read_file_contents(csv_path);
    EXPECT_TRUE(csv.find("file,writer/file.dat,1234,") != std::string::npos);
    EXPECT_TRUE(csv.find("folder,writer,,9870,") != std::string::npos);
    EXPECT_TRUE(csv.find(",1,1234,") != std::string::npos);
    std::error_code ignored;
    fs::remove(socket_path, ignored);

    const fs::path batch_socket_path =
        fs::path("/tmp") / ("wsync_writer_batch_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock");
    const fs::path batch_csv_path = output.path / "batch_records.csv";
    RawBufferPool batch_sender_pool(hypersync::kMetadataBatchBufferPoolId,
                                    4U,
                                    sizeof(hypersync::MetadataBatchBuffer),
                                    alignof(hypersync::MetadataBatchBuffer));
    RawBufferPool batch_receiver_pool(hypersync::kMetadataBatchBufferPoolId,
                                      4U,
                                      sizeof(hypersync::MetadataBatchBuffer),
                                      alignof(hypersync::MetadataBatchBuffer));
    BufferPoolRegistry batch_sender_registry;
    batch_sender_registry.register_pool(batch_sender_pool);
    BufferPoolRegistry batch_writer_registry;
    batch_writer_registry.register_pool(batch_receiver_pool);
    BufQueue batch_sender_queue(4U);
    BufQueue batch_writer_queue(4U);
    MetadataRecordWriterConfig batch_writer_config(true,
                                                   batch_csv_path,
                                                   MetadataRecordFormat::csv,
                                                   true,
                                                   true,
                                                   false);
    BufferReceiverJob batch_receiver(1U,
                                     batch_receiver_pool,
                                     batch_writer_queue,
                                     BufferTransportEndpoint::unix_socket(batch_socket_path));
    MetadataRecordWriterJob batch_writer(1U, batch_writer_queue, batch_writer_registry, batch_writer_config);
    BufferSenderJob batch_sender(1U,
                                 batch_sender_queue,
                                 batch_sender_registry,
                                 BufferTransportEndpoint::unix_socket(batch_socket_path));
    batch_receiver.start();
    batch_writer.start();
    batch_sender.start();

    const BufferHandle batch_handle = *batch_sender_pool.try_acquire();
    auto& metadata_batch = hypersync::metadata_batch_buffer(batch_sender_pool, batch_handle);
    hypersync::reset_metadata_batch(metadata_batch);
    EXPECT_TRUE(hypersync::append_metadata_batch_file(metadata_batch, file));
    EXPECT_TRUE(hypersync::append_metadata_batch_folder(metadata_batch, folder));
    EXPECT_TRUE(batch_sender_queue.push_wait(batch_handle));
    batch_sender_queue.close();
    batch_sender.wait();
    batch_receiver.wait();
    batch_writer.wait();

    EXPECT_EQ(batch_sender_pool.available(), batch_sender_pool.capacity());
    EXPECT_EQ(batch_receiver_pool.available(), batch_receiver_pool.capacity());
    EXPECT_EQ(batch_sender.stats().buffers, 1ULL);
    EXPECT_EQ(batch_receiver.stats().buffers, 1ULL);
    EXPECT_EQ(batch_writer.writer_stats().files_written, 1ULL);
    EXPECT_EQ(batch_writer.writer_stats().folders_written, 1ULL);
    const std::string batch_csv = hypersync::read_file_contents(batch_csv_path);
    EXPECT_TRUE(batch_csv.find("file,writer/file.dat,1234,") != std::string::npos);
    EXPECT_TRUE(batch_csv.find("folder,writer,,9870,") != std::string::npos);
    fs::remove(batch_socket_path, ignored);

#if HYPERSYNC_HAS_DUCKDB
    const fs::path parquet_socket_path =
        fs::path("/tmp") / ("wsync_writer_pq_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock");
    const fs::path parquet_path = output.path / "records.parquet";
    BufQueue parquet_sender_queue(8U);
    BufQueue parquet_writer_queue(8U);
    MetadataRecordWriterConfig parquet_writer_config(true,
                                                     parquet_path,
                                                     MetadataRecordFormat::parquet,
                                                     true,
                                                     true,
                                                     false);
    parquet_writer_config.duckdb_memory_limit = "512MB";
    parquet_writer_config.duckdb_threads = 1;
    parquet_writer_config.parquet_compression = "zstd";
    BufferReceiverJob parquet_receiver(1U,
                                       receiver_pool,
                                       parquet_writer_queue,
                                       BufferTransportEndpoint::unix_socket(parquet_socket_path));
    MetadataRecordWriterJob parquet_writer(1U, parquet_writer_queue, writer_registry, parquet_writer_config);
    BufferSenderJob parquet_sender(1U,
                                   parquet_sender_queue,
                                   sender_registry,
                                   BufferTransportEndpoint::unix_socket(parquet_socket_path));

    parquet_receiver.start();
    parquet_writer.start();
    parquet_sender.start();

    const BufferHandle parquet_file_handle = *sender_pool.try_acquire();
    EXPECT_TRUE(hypersync::encode_metadata_file_record(hypersync::metadata_buffer(sender_pool, parquet_file_handle), file));
    EXPECT_TRUE(parquet_sender_queue.push_wait(parquet_file_handle));
    parquet_sender_queue.close();
    parquet_sender.wait();
    parquet_receiver.wait();
    parquet_writer.wait();

    EXPECT_TRUE(fs::exists(parquet_path));
    EXPECT_FALSE(fs::exists(fs::path(parquet_path.string() + ".tmp")));
    EXPECT_FALSE(fs::exists(fs::path(parquet_path.string() + ".duckdb.tmp")));
    EXPECT_EQ(parquet_writer.writer_stats().files_written, 1ULL);
    EXPECT_EQ(sender_pool.available(), sender_pool.capacity());
    EXPECT_EQ(receiver_pool.available(), receiver_pool.capacity());

    MetadataRecordWriter append_writer(parquet_writer_config);
    append_writer.write_batch({}, {folder});
    append_writer.close();
    EXPECT_TRUE(fs::exists(parquet_path));
    EXPECT_FALSE(fs::exists(fs::path(parquet_path.string() + ".tmp")));
    EXPECT_FALSE(fs::exists(fs::path(parquet_path.string() + ".duckdb.tmp")));
    EXPECT_EQ(append_writer.folders_written(), 1ULL);
    fs::remove(parquet_socket_path, ignored);
#endif
}

void test_data_hasher_hashes_and_forwards_raw_buffers() {
    constexpr std::uint64_t kBufferCount = 1024;
    RawBufferPool pool(96U, 64U, 4096U);
    BufQueue generator_to_hasher(32U);
    BufQueue hasher_to_discarder(32U);
    BufferPoolRegistry registry;
    registry.register_pool(pool);

    BufferGeneratorJob generator(
        BufferGeneratorConfig(1U, kBufferCount, BufferGeneratorPattern::xoshiro256, 1234U), pool, generator_to_hasher);
    DataHasherJob hasher(
        DataHasherConfig(1U, hypersync::ContentHashAlgorithm::xxh3_64), generator_to_hasher, hasher_to_discarder, registry);
    BufferDiscarderJob discarder(BufferDiscarderConfig(1U), hasher_to_discarder, registry);

    discarder.start();
    hasher.start();
    generator.start();
    generator.wait();
    hasher.wait();
    discarder.wait();

    const auto hasher_stats = hasher.stats();
    EXPECT_EQ(hasher_stats.buffers_hashed, kBufferCount);
    EXPECT_EQ(hasher_stats.bytes_hashed, kBufferCount * pool.buffer_size_bytes());
    EXPECT_EQ(hasher_stats.work_factor, 1U);
    EXPECT_TRUE(hasher_stats.digest_marker != 0U);
    EXPECT_EQ(discarder.stats().buffers_discarded, kBufferCount);
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_EQ(pool.available(), pool.capacity());
}

void test_packed_small_file_data_buffers_hash_without_repacking() {
    RawBufferPool pool = hypersync::make_data_buffer_pool(2U);
    BufQueue reader_to_hasher(2U);
    BufQueue hasher_to_discarder(2U);
    BufferPoolRegistry registry;
    registry.register_pool(pool);

    std::optional<BufferHandle> acquired = pool.try_acquire();
    EXPECT_TRUE(acquired.has_value());
    hypersync::DataBuffer& buffer = hypersync::data_buffer(pool, *acquired);
    hypersync::reset_packed_small_file_buffer(buffer);

    hypersync::PackedSmallFileMeta alpha;
    alpha.file_id = 11U;
    alpha.folder_hash = 22U;
    alpha.file_size = 3U;
    alpha.mtime = 100U;
    alpha.mode = 0644U;
    alpha.uid = 1U;
    alpha.gid = 2U;
    alpha.rel_path = "folder/alpha.txt";
    EXPECT_TRUE(hypersync::append_packed_small_file(buffer, alpha, "abc"));

    hypersync::PackedSmallFileMeta beta;
    beta.file_id = 33U;
    beta.folder_hash = 22U;
    beta.file_size = 6U;
    beta.mtime = 101U;
    beta.mode = 0600U;
    beta.uid = 3U;
    beta.gid = 4U;
    beta.rel_path = "folder/beta.bin";
    EXPECT_TRUE(hypersync::append_packed_small_file(buffer, beta, "012345"));

    EXPECT_TRUE(hypersync::is_packed_small_file_buffer(buffer));
    EXPECT_EQ(hypersync::packed_small_file_count(buffer), 2U);
    EXPECT_EQ(hypersync::packed_small_file_payload_bytes(buffer), 9U);

    std::vector<std::string> paths;
    std::vector<std::string> payloads;
    const bool visited = hypersync::visit_packed_small_files(buffer, [&](const hypersync::PackedSmallFileView& file) {
        paths.emplace_back(file.rel_path);
        payloads.emplace_back(file.data);
    });
    EXPECT_TRUE(visited);
    EXPECT_EQ(paths.size(), 2U);
    EXPECT_EQ(paths[0], std::string("folder/alpha.txt"));
    EXPECT_EQ(paths[1], std::string("folder/beta.bin"));
    EXPECT_EQ(payloads[0], std::string("abc"));
    EXPECT_EQ(payloads[1], std::string("012345"));

    DataHasherJob hasher(
        DataHasherConfig(1U, hypersync::ContentHashAlgorithm::xxh3_64), reader_to_hasher, hasher_to_discarder, registry);
    BufferDiscarderJob discarder(BufferDiscarderConfig(1U), hasher_to_discarder, registry);

    discarder.start();
    hasher.start();
    EXPECT_TRUE(reader_to_hasher.push_wait(*acquired));
    reader_to_hasher.close();
    hasher.wait();
    discarder.wait();

    EXPECT_EQ(hasher.stats().buffers_hashed, 1U);
    EXPECT_EQ(hasher.stats().bytes_hashed, 9U);
    EXPECT_TRUE((buffer.trailer.flags & hypersync::kFlagHashValid) != 0U);
    EXPECT_TRUE(hasher.stats().digest_marker != 0U);
    EXPECT_EQ(discarder.stats().buffers_discarded, 1U);
    EXPECT_EQ(pool.in_use(), 0U);
}

void test_nfs_data_buffer_reader_feeds_hasher_pipeline() {
    TempDir source("hypersync_nfs_data_buffer_reader");
    write_file(source.path / "alpha.bin", "abcdef");
    write_file(source.path / "beta.bin", "0123456789");

    std::vector<FileSpec> files;
    FileSpec alpha;
    alpha.rel_path = "alpha.bin";
    alpha.declared_size = 6;
    files.push_back(alpha);
    FileSpec beta;
    beta.rel_path = "beta.bin";
    beta.declared_size = 10;
    files.push_back(beta);

    std::atomic<std::size_t> next_file {0};
    RawBufferPool pool = hypersync::make_data_buffer_pool(8U);
    BufQueue reader_to_hasher(4U);
    BufQueue hasher_to_discarder(4U);
    BufferPoolRegistry registry;
    registry.register_pool(pool);

    hypersync::NfsDataReaderConfig reader_config(2U, 2U, 0U, 1U, 4U, hypersync::kLargeChunkBytes, 85.0, 70.0, source.path.string());
    NfsDataBufferReaderJob reader(reader_config,
                                  pool,
                                  reader_to_hasher,
                                  [&files, &next_file]() -> std::optional<FileSpec> {
                                      const std::size_t index = next_file.fetch_add(1U);
                                      if (index >= files.size()) {
                                          return std::nullopt;
                                      }
                                      return files[index];
                                  });
    DataHasherJob hasher(
        DataHasherConfig(1U, hypersync::ContentHashAlgorithm::xxh3_64), reader_to_hasher, hasher_to_discarder, registry);
    BufferDiscarderJob discarder(BufferDiscarderConfig(1U), hasher_to_discarder, registry);

    discarder.start();
    hasher.start();
    reader.start();
    reader.wait();
    hasher.wait();
    discarder.wait();

    EXPECT_EQ(reader.stats().files_read, 2U);
    EXPECT_EQ(reader.stats().files_failed, 0U);
    EXPECT_EQ(reader.stats().bytes_read, 16U);
    EXPECT_EQ(hasher.stats().bytes_hashed, 16U);
    EXPECT_EQ(discarder.stats().buffers_discarded, 2U);
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_EQ(pool.available(), pool.capacity());
}

void test_nfs_data_buffer_reader_packs_small_files_into_owned_buffer() {
    TempDir source("hypersync_nfs_data_buffer_reader_pack");
    write_file(source.path / "one.txt", "111");
    write_file(source.path / "two.txt", "2222");
    write_file(source.path / "three.txt", "33333");

    std::vector<FileSpec> files;
    files.emplace_back("one.txt", "", 1, 0644, 0, 0, true, 3);
    files.emplace_back("two.txt", "", 2, 0644, 0, 0, true, 4);
    files.emplace_back("three.txt", "", 3, 0644, 0, 0, true, 5);

    std::atomic<std::size_t> next_file {0};
    RawBufferPool pool = hypersync::make_data_buffer_pool(4U);
    BufQueue reader_to_discarder(4U);
    BufferPoolRegistry registry;
    registry.register_pool(pool);

    hypersync::NfsDataReaderConfig reader_config(1U, 1U, 0U, 1U, hypersync::kSmallFileThreshold,
                                                 hypersync::kLargeChunkBytes, 85.0, 70.0, source.path.string());
    reader_config.pack_small_files = true;
    NfsDataBufferReaderJob reader(reader_config,
                                  pool,
                                  reader_to_discarder,
                                  [&files, &next_file]() -> std::optional<FileSpec> {
                                      const std::size_t index = next_file.fetch_add(1U);
                                      if (index >= files.size()) {
                                          return std::nullopt;
                                      }
                                      return files[index];
                                  });

    reader.start();
    reader.wait();

    BufferHandle handle;
    EXPECT_TRUE(reader_to_discarder.try_pop(handle));
    EXPECT_FALSE(reader_to_discarder.try_pop(handle));
    hypersync::DataBuffer& buffer = hypersync::data_buffer(pool, handle);
    EXPECT_TRUE(hypersync::is_packed_small_file_buffer(buffer));
    EXPECT_EQ(hypersync::packed_small_file_count(buffer), 3U);
    EXPECT_EQ(hypersync::packed_small_file_payload_bytes(buffer), 12U);
    EXPECT_EQ(reader.stats().files_read, 3U);
    EXPECT_EQ(reader.stats().bytes_read, 12U);

    std::vector<std::string> paths;
    const bool visited = hypersync::visit_packed_small_files(buffer, [&](const hypersync::PackedSmallFileView& file) {
        paths.emplace_back(file.rel_path);
    });
    EXPECT_TRUE(visited);
    EXPECT_EQ(paths.size(), 3U);
    EXPECT_EQ(paths[0], std::string("one.txt"));
    EXPECT_EQ(paths[1], std::string("two.txt"));
    EXPECT_EQ(paths[2], std::string("three.txt"));

    pool.release(handle);
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_EQ(pool.available(), pool.capacity());
}

void test_nfs_data_buffer_reader_slides_small_files_without_packing() {
    TempDir source("hypersync_nfs_data_buffer_reader_raw_window");
    write_file(source.path / "one.txt", "111");
    write_file(source.path / "two.txt", "2222");
    write_file(source.path / "three.txt", "33333");

    std::vector<FileSpec> files;
    files.emplace_back("one.txt", "", 1, 0644, 0, 0, true, 3);
    files.emplace_back("two.txt", "", 2, 0644, 0, 0, true, 4);
    files.emplace_back("three.txt", "", 3, 0644, 0, 0, true, 5);

    std::atomic<std::size_t> next_file {0};
    RawBufferPool pool = hypersync::make_data_buffer_pool(8U);
    BufQueue reader_to_discarder(8U);

    hypersync::NfsDataReaderConfig reader_config(1U, 1U, 2U, 1U, hypersync::kSmallFileThreshold,
                                                 hypersync::kLargeChunkBytes, 85.0, 70.0, source.path.string());
    reader_config.pack_small_files = false;
    NfsDataBufferReaderJob reader(reader_config,
                                  pool,
                                  reader_to_discarder,
                                  [&files, &next_file]() -> std::optional<FileSpec> {
                                      const std::size_t index = next_file.fetch_add(1U);
                                      if (index >= files.size()) {
                                          return std::nullopt;
                                      }
                                      return files[index];
                                  });

    reader.start();
    reader.wait();

    std::uint64_t total_bytes = 0;
    std::size_t buffers = 0;
    BufferHandle handle;
    while (reader_to_discarder.try_pop(handle)) {
        hypersync::DataBuffer& buffer = hypersync::data_buffer(pool, handle);
        EXPECT_FALSE(hypersync::is_packed_small_file_buffer(buffer));
        EXPECT_TRUE((buffer.trailer.flags & hypersync::kFlagSmallFile) != 0U);
        EXPECT_TRUE((buffer.trailer.flags & hypersync::kFlagLastChunk) != 0U);
        total_bytes += buffer.trailer.data_len;
        ++buffers;
        pool.release(handle);
    }

    EXPECT_EQ(buffers, 3U);
    EXPECT_EQ(total_bytes, 12U);
    EXPECT_EQ(reader.stats().files_read, 3U);
    EXPECT_EQ(reader.stats().files_failed, 0U);
    EXPECT_EQ(reader.stats().bytes_read, 12U);
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_EQ(pool.available(), pool.capacity());
}

void test_target_data_writer_writes_regular_and_packed_buffers() {
    TempDir target("hypersync_target_data_writer");
    RawBufferPool pool = hypersync::make_data_buffer_pool(8U);
    ShardedBufQueue queue(2U, 4U);

    BufferHandle large_a = pool.acquire_wait();
    hypersync::DataBuffer& large_a_buffer = hypersync::data_buffer(pool, large_a);
    large_a_buffer.trailer = {};
    large_a_buffer.trailer.file_id = 42U;
    large_a_buffer.trailer.file_size = 11U;
    large_a_buffer.trailer.data_offset = 0U;
    large_a_buffer.trailer.data_len = 6U;
    large_a_buffer.trailer.mode = 0644U;
    large_a_buffer.trailer.uid = static_cast<std::uint32_t>(::getuid());
    large_a_buffer.trailer.gid = static_cast<std::uint32_t>(::getgid());
    large_a_buffer.trailer.rel_path = "large.bin";
    std::memcpy(large_a_buffer.bytes.data(), "hello ", 6U);

    BufferHandle large_b = pool.acquire_wait();
    hypersync::DataBuffer& large_b_buffer = hypersync::data_buffer(pool, large_b);
    large_b_buffer.trailer = large_a_buffer.trailer;
    large_b_buffer.trailer.data_offset = 6U;
    large_b_buffer.trailer.data_len = 5U;
    large_b_buffer.trailer.flags = hypersync::kFlagLastChunk;
    std::memcpy(large_b_buffer.bytes.data(), "world", 5U);

    BufferHandle packed = pool.acquire_wait();
    hypersync::DataBuffer& packed_buffer = hypersync::data_buffer(pool, packed);
    hypersync::reset_packed_small_file_buffer(packed_buffer);
    hypersync::PackedSmallFileMeta small;
    small.file_id = 7U;
    small.folder_hash = 1U;
    small.file_size = 3U;
    small.mode = 0600U;
    small.uid = static_cast<std::uint32_t>(::getuid());
    small.gid = static_cast<std::uint32_t>(::getgid());
    small.rel_path = "tiny/a.txt";
    EXPECT_TRUE(hypersync::append_packed_small_file(packed_buffer, small, "abc"));

    EXPECT_TRUE(queue.shard(0U).push_wait(large_a));
    EXPECT_TRUE(queue.shard(0U).push_wait(large_b));
    EXPECT_TRUE(queue.shard(1U).push_wait(packed));
    queue.close();

    TargetDataWriterJob writer(TargetDataWriterConfig(2U, target.path.string(), false), pool, queue);
    writer.start();
    writer.wait();

    EXPECT_EQ(writer.stats().files_written, 2U);
    EXPECT_EQ(writer.stats().files_failed, 0U);
    EXPECT_EQ(writer.stats().buffers_processed, 3U);
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_EQ(pool.available(), pool.capacity());

    std::ifstream large_in(target.path / "large.bin", std::ios::binary);
    std::string large_payload((std::istreambuf_iterator<char>(large_in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(large_payload, std::string("hello world"));
    std::ifstream small_in(target.path / "tiny/a.txt", std::ios::binary);
    std::string small_payload((std::istreambuf_iterator<char>(small_in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(small_payload, std::string("abc"));
}

void test_target_meta_writer_creates_flat_folder_directories() {
    TempDir target("hypersync_target_meta_writer");
    RawBufferPool pool = hypersync::make_metadata_batch_buffer_pool(4U);
    BufQueue queue(4U);

    BufferHandle handle = pool.acquire_wait();
    hypersync::MetadataBatchBuffer& buffer = hypersync::metadata_batch_buffer(pool, handle);
    FileSpec folder;
    folder.rel_path = "root";
    folder.mode = 0755U;
    folder.uid = static_cast<std::uint32_t>(::getuid());
    folder.gid = static_cast<std::uint32_t>(::getgid());
    hypersync::reset_flat_folder_buffer(buffer,
                                        folder,
                                        0U,
                                        true,
                                        false,
                                        "",
                                        0U,
                                        1U,
                                        0U,
                                        0U,
                                        0U,
                                        0U);
    FileSpec child;
    child.rel_path = "root/child";
    child.mode = 0750U;
    child.uid = static_cast<std::uint32_t>(::getuid());
    child.gid = static_cast<std::uint32_t>(::getgid());
    EXPECT_TRUE(hypersync::append_flat_folder_folder(buffer, child, "metadata"));
    EXPECT_TRUE(queue.push_wait(handle));
    queue.close();

    TargetMetaWriterJob writer(TargetMetaWriterConfig(1U, target.path.string()), pool, queue);
    writer.start();
    writer.wait();

    EXPECT_TRUE(fs::is_directory(target.path / "root"));
    EXPECT_TRUE(fs::is_directory(target.path / "root/child"));
    EXPECT_EQ(writer.stats().buffers_processed, 1U);
    EXPECT_EQ(writer.stats().folders_written, 2U);
    EXPECT_EQ(pool.in_use(), 0U);
    EXPECT_EQ(pool.available(), pool.capacity());
}

void test_scan_index_round_trip_and_folder_hashes() {
    FileSpec alpha{"//root//alpha.txt/", "hello", 10, 0644, 1, 2};
    FileSpec beta{"root/nested/beta.txt", "world", 11, 0600, 3, 4};

    ScanIndex index;
    index.add(hypersync::make_snapshot(alpha, hypersync::hash64(alpha.content), 'S'));
    index.add(hypersync::make_snapshot(beta, hypersync::hash64(beta.content), 'S'));

    EXPECT_FALSE(index.empty());
    const auto alpha_row = index.find("root/alpha.txt");
    EXPECT_TRUE(alpha_row.has_value());
    EXPECT_TRUE(index.file_matches(*alpha_row));
    FileSnapshot missing = *alpha_row;
    missing.rel_path = "root/missing.txt";
    EXPECT_FALSE(index.file_matches(missing));
    EXPECT_TRUE(index.metadata_matches("root/alpha.txt", 5, 10));
    EXPECT_FALSE(index.metadata_matches("root/alpha.txt", 6, 10));
    EXPECT_EQ(index.folder_data_hash("root"), hypersync::compute_folder_data_hash({*alpha_row}));
    EXPECT_EQ(index.folder_data_hash("missing"), 0ULL);

    const std::string csv = index.to_csv();
    const ScanIndex reloaded = ScanIndex::from_csv(csv);
    EXPECT_TRUE(reloaded.file_matches(*alpha_row));
    const auto beta_row = reloaded.find("root/nested/beta.txt");
    EXPECT_TRUE(beta_row.has_value());
    EXPECT_EQ(beta_row->scan_side, 'S');
    EXPECT_EQ(reloaded.rows().size(), 2U);

    const std::string rich_csv =
        "record_type,rel_path,size,mtime,mode,uid,gid,flat_file_count,flat_logical_size_bytes,hash_algorithm,"
        "content_hash,hash_block_size,hash_block_count,block_hash_algorithm,block_hashes,scan_run_id,"
        "run_started_at_utc,run_started_unix_ns,source_root,run_settings\n"
        "folder,root,0,1,493,1,2,1,5,\"\",\"\",0,0,\"\",\"\",99,2026-05-13T00:00:00Z,123,/tmp/src,\"{}\"\n"
        "file,root/alpha.txt,5,10,420,1,2,,,\"\",\"\",0,0,\"\",\"\",99,2026-05-13T00:00:00Z,123,/tmp/src,\"{}\"\n";
    const ScanIndex rich_reloaded = ScanIndex::from_csv(rich_csv);
    const auto rich_alpha = rich_reloaded.find("root/alpha.txt");
    EXPECT_TRUE(rich_alpha.has_value());
    EXPECT_EQ(rich_alpha->size, 5ULL);
    EXPECT_EQ(rich_alpha->mtime, 10ULL);
    EXPECT_EQ(rich_reloaded.rows().size(), 1U);
}

void test_scan_index_rejects_bad_csv() {
    EXPECT_THROW(ScanIndex::from_csv(""));
    EXPECT_THROW(ScanIndex::from_csv("bad-header\n"));
    EXPECT_THROW(ScanIndex::from_csv(
        "folder_hash,file_hash,rel_path,size,mtime,mode,uid,gid,data_hash,hash_ts,scan_side\n1,2,a,3,4,5,6,7,8,9\n"));
    EXPECT_THROW(ScanIndex::from_csv(
        "folder_hash,file_hash,rel_path,size,mtime,mode,uid,gid,data_hash,hash_ts,scan_side\n1,2,a,3,4,5,6,7,8,9,XX\n"));
    EXPECT_THROW(ScanIndex::from_csv(
        "folder_hash,file_hash,rel_path,size,mtime,mode,uid,gid,data_hash,hash_ts,scan_side\n1,x,a,3,4,5,6,7,8,9,S\n"));
}

void test_state_machines_accept_valid_paths_and_reject_invalid_ones() {
    EXPECT_EQ(hypersync::to_string(Mode::transfer), "transfer");
    EXPECT_EQ(hypersync::to_string(Mode::scan), "scan");
    EXPECT_EQ(hypersync::to_string(Mode::dry_run), "dry-run");
    EXPECT_EQ(hypersync::to_string(static_cast<Mode>(99)), "unknown");
    EXPECT_EQ(hypersync::to_string(EndpointRole::sender), "sender");
    EXPECT_EQ(hypersync::to_string(EndpointRole::receiver), "receiver");
    EXPECT_EQ(hypersync::to_string(static_cast<EndpointRole>(99)), "unknown");
    EXPECT_EQ(hypersync::to_string(FileState::pending), "pending");
    EXPECT_EQ(hypersync::to_string(FileState::checking), "checking");
    EXPECT_EQ(hypersync::to_string(FileState::skipped), "skipped");
    EXPECT_EQ(hypersync::to_string(FileState::reading), "reading");
    EXPECT_EQ(hypersync::to_string(FileState::transferring), "transferring");
    EXPECT_EQ(hypersync::to_string(FileState::receiving), "receiving");
    EXPECT_EQ(hypersync::to_string(FileState::writing), "writing");
    EXPECT_EQ(hypersync::to_string(FileState::failed), "failed");
    EXPECT_EQ(hypersync::to_string(FileState::done), "done");
    EXPECT_EQ(hypersync::to_string(static_cast<FileState>(99)), "unknown");
    EXPECT_EQ(hypersync::to_string(FolderState::pending), "pending");
    EXPECT_EQ(hypersync::to_string(FolderState::reading), "reading");
    EXPECT_EQ(hypersync::to_string(FolderState::transferring), "transferring");
    EXPECT_EQ(hypersync::to_string(FolderState::awaiting_ack), "awaiting_ack");
    EXPECT_EQ(hypersync::to_string(FolderState::receiving), "receiving");
    EXPECT_EQ(hypersync::to_string(FolderState::writing), "writing");
    EXPECT_EQ(hypersync::to_string(FolderState::done), "done");
    EXPECT_EQ(hypersync::to_string(static_cast<FolderState>(99)), "unknown");
    EXPECT_EQ(hypersync::to_string(DiffKind::skip), "skip");
    EXPECT_EQ(hypersync::to_string(DiffKind::new_file), "new");
    EXPECT_EQ(hypersync::to_string(DiffKind::changed), "changed");
    EXPECT_EQ(hypersync::to_string(DiffKind::target_only), "target_only");
    EXPECT_EQ(hypersync::to_string(DiffKind::failed), "failed");
    EXPECT_EQ(hypersync::to_string(static_cast<DiffKind>(99)), "unknown");

    EXPECT_EQ(hypersync::normalize_path("/root//nested/file.txt/"), "root/nested/file.txt");
    EXPECT_EQ(hypersync::parent_path("root/nested/file.txt"), "root/nested");
    EXPECT_EQ(hypersync::base_name("root/nested/file.txt"), "file.txt");
    EXPECT_TRUE(hypersync::hash64("a") != hypersync::hash64("b"));
    EXPECT_TRUE(hypersync::chunk_hash32("chunk") != 0U);
    EXPECT_TRUE(hypersync::path_hash("file.txt", 7) != 0ULL);
    EXPECT_TRUE(hypersync::folder_hash_for_path("root/nested") != 0ULL);

    EXPECT_TRUE(hypersync::can_transition_file(EndpointRole::sender, FileState::pending, FileState::reading));
    EXPECT_TRUE(hypersync::can_transition_file(EndpointRole::sender, FileState::checking, FileState::skipped));
    EXPECT_TRUE(hypersync::can_transition_file(EndpointRole::receiver, FileState::pending, FileState::receiving));
    EXPECT_TRUE(hypersync::can_transition_file(EndpointRole::receiver, FileState::checking, FileState::done));
    EXPECT_TRUE(hypersync::can_transition_file(EndpointRole::receiver, FileState::receiving, FileState::writing));
    EXPECT_TRUE(hypersync::can_transition_file(EndpointRole::receiver, FileState::writing, FileState::failed));
    EXPECT_TRUE(hypersync::can_transition_file(EndpointRole::receiver, FileState::failed, FileState::receiving));
    EXPECT_TRUE(hypersync::can_transition_file(EndpointRole::receiver, FileState::done, FileState::done));
    EXPECT_FALSE(hypersync::can_transition_file(EndpointRole::receiver, static_cast<FileState>(99), FileState::done));
    EXPECT_FALSE(hypersync::can_transition_file(EndpointRole::receiver, FileState::done, FileState::receiving));
    EXPECT_TRUE(hypersync::can_transition_folder(EndpointRole::sender, FolderState::reading, FolderState::awaiting_ack));
    EXPECT_TRUE(hypersync::can_transition_folder(EndpointRole::receiver, FolderState::receiving, FolderState::done));
    EXPECT_TRUE(hypersync::can_transition_folder(EndpointRole::receiver, FolderState::receiving, FolderState::writing));
    EXPECT_TRUE(hypersync::can_transition_folder(EndpointRole::sender, FolderState::done, FolderState::done));
    EXPECT_FALSE(hypersync::can_transition_folder(EndpointRole::sender, FolderState::done, FolderState::writing));
    EXPECT_FALSE(hypersync::can_transition_folder(EndpointRole::receiver, static_cast<FolderState>(99), FolderState::done));
    EXPECT_FALSE(hypersync::can_transition_folder(EndpointRole::receiver, FolderState::done, FolderState::writing));

    RecBuf record;
    hypersync::transition_file(record, EndpointRole::sender, FileState::checking);
    hypersync::transition_file(record, EndpointRole::sender, FileState::reading);
    hypersync::transition_file(record, EndpointRole::sender, FileState::transferring);
    hypersync::transition_file(record, EndpointRole::sender, FileState::done);
    EXPECT_EQ(record.state, FileState::done);
    EXPECT_THROW(hypersync::transition_file(record, EndpointRole::sender, FileState::reading));

    FolderRecord folder;
    hypersync::transition_folder(folder, EndpointRole::sender, FolderState::reading);
    hypersync::transition_folder(folder, EndpointRole::sender, FolderState::done);
    EXPECT_EQ(folder.state, FolderState::done);
    EXPECT_THROW(hypersync::transition_folder(folder, EndpointRole::receiver, FolderState::writing));
}

void test_hash64_matches_xxhash64_vectors_and_streaming_updates() {
    EXPECT_EQ(hypersync::hash64(""), 0xef46db3751d8e999ULL);
    EXPECT_EQ(hypersync::hash64("hello"), 0x26c7827d889f6da3ULL);
    EXPECT_EQ(hypersync::hash64("abcdefghijklmnopqrstuvwxyz"), 0xcfe1f278fa89835cULL);

    const std::string large_payload(1'000'000, 'x');
    EXPECT_EQ(hypersync::hash64(large_payload), 0x16c7c43f6b9adc14ULL);

    hypersync::Hash64State streaming;
    streaming.update("abcdefgh");
    streaming.update("ijklmnopqrst");
    streaming.update("uvwxyz");
    EXPECT_EQ(streaming.value(), hypersync::hash64("abcdefghijklmnopqrstuvwxyz"));

    hypersync::Hash64State large_streaming;
    for (std::size_t offset = 0; offset < large_payload.size(); offset += 33333U) {
        large_streaming.update(std::string_view(large_payload).substr(offset, 33333U));
    }
    EXPECT_EQ(large_streaming.value(), hypersync::hash64(large_payload));
}

void test_content_hash_matches_standard_vectors_and_streaming_updates() {
    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::md5, ""),
              "d41d8cd98f00b204e9800998ecf8427e");
    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::md5, "abc"),
              "900150983cd24fb0d6963f7d28e17f72");

    hypersync::Md5State md5_streaming;
    md5_streaming.update("a");
    md5_streaming.update("b");
    md5_streaming.update("c");
    EXPECT_EQ(md5_streaming.hex_digest(), "900150983cd24fb0d6963f7d28e17f72");

    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::sha256, ""),
              "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::sha256, "abc"),
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");

    hypersync::Sha256State streaming;
    streaming.update("a");
    streaming.update("b");
    streaming.update("c");
    EXPECT_EQ(streaming.hex_digest(),
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::xxh64, "hello"),
              "26c7827d889f6da3");
    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::xxh3_64, ""),
              "2d06800538d394c2");
    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::xxh3_64, "abc"),
              "78af5f94892f3950");
    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::xxh3_128, ""),
              "99aa06d3014798d86001c324468d497f");
    EXPECT_EQ(hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::xxh3_128, "abc"),
              "06b05ab6733a618578af5f94892f3950");

    hypersync::Xxh3_64State xxh3_64_streaming;
    xxh3_64_streaming.update("a");
    xxh3_64_streaming.update("bc");
    EXPECT_EQ(xxh3_64_streaming.hex_digest(), "78af5f94892f3950");

    hypersync::Xxh3_128State xxh3_128_streaming;
    xxh3_128_streaming.update("a");
    xxh3_128_streaming.update("bc");
    EXPECT_EQ(xxh3_128_streaming.hex_digest(), "06b05ab6733a618578af5f94892f3950");

    EXPECT_EQ(hypersync::to_string(hypersync::parse_content_hash_algorithm("md5")), "md5");
    EXPECT_EQ(hypersync::to_string(hypersync::parse_content_hash_algorithm("sha256")), "sha256");
    EXPECT_EQ(hypersync::to_string(hypersync::parse_content_hash_algorithm("sha-256")), "sha256");
    EXPECT_EQ(hypersync::to_string(hypersync::parse_content_hash_algorithm("xxh64")), "xxh64");
    EXPECT_EQ(hypersync::to_string(hypersync::parse_content_hash_algorithm("xxh3")), "xxh3_64");
    EXPECT_EQ(hypersync::to_string(hypersync::parse_content_hash_algorithm("xxh3_128")), "xxh3_128");
    EXPECT_THROW(hypersync::parse_content_hash_algorithm("crc32"));
}

void test_buffer_metadata_footer_round_trip_and_checksums() {
    RawBufferPool pool(91, 1, 4096 + 4096);
    const BufferHandle handle = pool.acquire_wait();
    std::byte* bytes = pool.data(handle);
    const std::string payload = "abcdefghijklmnop";
    std::memcpy(bytes, payload.data(), payload.size());

    hypersync::BufferMetadataInfo info;
    info.data_size = static_cast<std::uint32_t>(payload.size());
    info.checksum_algorithm = hypersync::BufferChecksumAlgorithm::fnv1a64;
    info.sub_buffers.push_back(hypersync::BufferSubBufferInfo{0, 8, 0, 0, 0, 7});
    info.sub_buffers.push_back(hypersync::BufferSubBufferInfo{8, 8, 0, 0, 0, 11});

    hypersync::write_buffer_metadata(bytes,
                                     pool.buffer_size_bytes(),
                                     info,
                                     hypersync::BufferMetadataWriteOptions{true, true, true});

    EXPECT_TRUE(hypersync::has_buffer_metadata(bytes, pool.buffer_size_bytes()));
    EXPECT_TRUE(info.data_checksum != 0U);
    EXPECT_TRUE(info.metadata_checksum != 0U);

    const auto parsed = hypersync::read_buffer_metadata(bytes, pool.buffer_size_bytes());
    EXPECT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->data_size, info.data_size);
    EXPECT_EQ(parsed->version, hypersync::kBufferMetadataVersion);
    EXPECT_EQ(parsed->checksum_algorithm, hypersync::BufferChecksumAlgorithm::fnv1a64);
    EXPECT_EQ(parsed->data_checksum, info.data_checksum);
    EXPECT_EQ(parsed->metadata_checksum, info.metadata_checksum);
    EXPECT_EQ(parsed->sub_buffers.size(), std::size_t{2});
    EXPECT_EQ(parsed->sub_buffers[0].offset, std::uint32_t{0});
    EXPECT_EQ(parsed->sub_buffers[1].offset, std::uint32_t{8});
    EXPECT_TRUE(hypersync::validate_buffer_metadata(bytes, pool.buffer_size_bytes(), *parsed));

    const std::size_t metadata_size = hypersync::buffer_metadata_size_for_sub_buffers(parsed->sub_buffers.size());
    EXPECT_EQ(std::memcmp(bytes + payload.size(), bytes + pool.buffer_size_bytes() - metadata_size, metadata_size), 0);

    bytes[0] = static_cast<std::byte>('z');
    EXPECT_FALSE(hypersync::validate_buffer_metadata(bytes, pool.buffer_size_bytes(), *parsed));
    bytes[0] = static_cast<std::byte>('a');

    hypersync::BufferMetadataInfo no_checksum;
    no_checksum.data_size = 4;
    no_checksum.checksum_algorithm = hypersync::BufferChecksumAlgorithm::none;
    hypersync::write_buffer_metadata(bytes,
                                     pool.buffer_size_bytes(),
                                     no_checksum,
                                     hypersync::BufferMetadataWriteOptions{true, true, true});
    const auto parsed_no_checksum = hypersync::read_buffer_metadata(bytes, pool.buffer_size_bytes());
    EXPECT_TRUE(parsed_no_checksum.has_value());
    EXPECT_EQ(parsed_no_checksum->data_checksum, std::uint64_t{0});
    EXPECT_EQ(parsed_no_checksum->metadata_checksum, std::uint64_t{0});

    pool.release(handle);
}

void test_watermark_thresholds() {
    const auto low = hypersync::evaluate_watermarks(10.0, 10.0);
    EXPECT_FALSE(low.soft_throttle);
    EXPECT_TRUE(low.prefetch_to_ram);

    const auto mid = hypersync::evaluate_watermarks(75.0, 50.0);
    EXPECT_TRUE(mid.soft_throttle);
    EXPECT_FALSE(mid.spill_to_nvme);
    EXPECT_FALSE(mid.hard_stop);

    const auto high = hypersync::evaluate_watermarks(90.0, 85.0);
    EXPECT_TRUE(high.soft_throttle);
    EXPECT_TRUE(high.spill_to_nvme);
    EXPECT_TRUE(high.pause_reader);

    const auto critical = hypersync::evaluate_watermarks(96.0, 96.0);
    EXPECT_TRUE(critical.hard_stop);
    EXPECT_TRUE(critical.spill_to_nvme);
    EXPECT_TRUE(critical.pause_reader);
    EXPECT_THROW(hypersync::evaluate_watermarks(-1.0, 0.0));
    EXPECT_THROW(hypersync::evaluate_watermarks(0.0, 101.0));
}

void test_job_classes_exist_and_process_messages() {
    EXPECT_EQ(std::string(hypersync::message_kinds::folder_record), "folder_record");
    EXPECT_EQ(std::string(hypersync::message_kinds::file_record), "file_record");
    EXPECT_EQ(std::string(hypersync::message_kinds::data_chunk), "data_chunk");
    EXPECT_EQ(std::string(hypersync::message_kinds::file_snapshot), "file_snapshot");

    FolderRecord folder;
    folder.rel_path = "root";
    InputProvider provider({1, 100, 128, "input.csv", "overflow.csv"});
    provider.start();
    provider.submit_folder(folder);
    provider.submit_folder(FolderRecord{});

    JobMessage message;
    EXPECT_TRUE(provider.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::folder_record));
    EXPECT_EQ(hypersync::message_as<FolderRecord>(message).rel_path, "root");
    EXPECT_EQ(provider.overflow_entries(), 1U);
    const JobStats provider_stats = provider.stats();
    EXPECT_TRUE(provider_stats.running);
    EXPECT_EQ(provider_stats.accepted, 1U);
    EXPECT_EQ(provider_stats.deferred, 1U);
    provider.stop();

    NfsMetaReader meta_reader;
    meta_reader.start();
    meta_reader.begin_folder(folder);
    RecBuf rec = hypersync::make_recbuf({"root/file.txt", "payload", 9});
    meta_reader.publish_record(rec);
    FolderRecord child_folder;
    child_folder.rel_path = "root/child";
    meta_reader.discover_child_folder(child_folder);
    meta_reader.finish_folder(1);
    EXPECT_EQ(meta_reader.files_seen(), 1U);
    EXPECT_EQ(meta_reader.folders_completed(), 1U);
    EXPECT_EQ(meta_reader.take_discovered_folders().size(), 1U);
    EXPECT_TRUE(meta_reader.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::file_record));
    meta_reader.stop();

    FileSpec skip_file{"root/file.txt", "payload", 9};
    ScanIndex source_scan;
    ScanIndex target_scan;
    source_scan.add(hypersync::make_snapshot(skip_file, hypersync::hash64(skip_file.content), 'S'));
    target_scan.add(hypersync::make_snapshot(skip_file, hypersync::hash64(skip_file.content), 'T'));
    Checker checker;
    checker.bind_scans(&source_scan, &target_scan);
    EXPECT_TRUE(checker.should_skip(rec));
    checker.queue_checked_record(rec);
    EXPECT_TRUE(checker.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::file_record));

    Checker discard_checker(CheckerConfig{true});
    discard_checker.queue_checked_record(rec);
    EXPECT_FALSE(discard_checker.pull(message));
    EXPECT_EQ(discard_checker.stats().deferred, 1U);

    NfsDataReader data_reader({2, 256, 0, 8, 4, 8, 85.0, 70.0, "."});
    const FileSpec large_file{"root/large.bin", "abcdefghijklmnop", 10};
    const auto chunks = data_reader.chunk_file(large_file);
    EXPECT_EQ(chunks.size(), 2U);
    EXPECT_TRUE(data_reader.should_pause(90.0));
    EXPECT_TRUE(data_reader.should_resume(70.0));
    data_reader.publish_file(large_file);
    EXPECT_TRUE(data_reader.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::data_chunk));

    DataChunk chunk = hypersync::message_as<DataChunk>(message);
    TempDir cache_dir("hypersync_cache_layer");
    DataCacher cacher({EndpointRole::sender, 85.0, 70.0, cache_dir.path.string(), 1, 1});
    EXPECT_TRUE(cacher.should_spill(85.0));
    EXPECT_TRUE(cacher.should_drain(70.0));
    cacher.cache_chunk(chunk);
    EXPECT_TRUE(cacher.pull(message));
    EXPECT_TRUE(hypersync::message_as<DataChunk>(message).cached);
    const auto cached_ids = cacher.cached_entries_for(chunk.trailer.file_id);
    EXPECT_EQ(cached_ids.size(), 1U);
    EXPECT_TRUE(fs::exists(cacher.cache_path_for(cached_ids.front())));
    EXPECT_EQ(cacher.manifest_for(chunk.trailer.file_id).completed_chunks, 1U);
    EXPECT_TRUE(cacher.cached_bytes() != 0ULL);
    EXPECT_TRUE(cacher.cache_usage_percent(cacher.cached_bytes()) >= 100.0);
    const DataChunk cached_chunk = cacher.take_chunk(cached_ids.front());
    EXPECT_EQ(cached_chunk.data, chunk.data);
    EXPECT_EQ(cached_chunk.trailer.rel_path, chunk.trailer.rel_path);
    EXPECT_EQ(cacher.cached_bytes(), 0ULL);

    hypersync::DataSlotPool slot_pool(1, 1);
    const auto slot_handle = slot_pool.acquire_or_throw(hypersync::DataSlotClass::large, chunk.data.size());
    std::copy(chunk.data.begin(), chunk.data.end(), slot_pool.data(slot_handle));
    slot_pool.trailer(slot_handle) = chunk.trailer;
    cacher.cache_slot(slot_pool, slot_handle);
    slot_pool.release(slot_handle);
    const auto cached_slot_ids = cacher.cached_entries_for(chunk.trailer.file_id);
    EXPECT_EQ(cached_slot_ids.size(), 1U);
    const auto restored_slot = cacher.take_slot(cached_slot_ids.front(), slot_pool);
    EXPECT_EQ(slot_pool.data_view(restored_slot), std::string_view(chunk.data));
    EXPECT_EQ(slot_pool.trailer(restored_slot).rel_path, chunk.trailer.rel_path);
    slot_pool.release(restored_slot);
    EXPECT_EQ(cacher.cached_bytes(), 0ULL);

    DataSender sender({4, 1024, false});
    sender.queue_chunk(chunk);
    EXPECT_EQ(sender.dispatch_connection(chunk), chunk.trailer.file_id % 4U);
    EXPECT_TRUE(sender.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::data_chunk));

    DataReceiver receiver({8, 50.0});
    EXPECT_TRUE(receiver.should_refill(49.0));
    receiver.receive_chunk(chunk);
    EXPECT_TRUE(receiver.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::data_chunk));

    DataWriter writer;
    writer.predeclare_directory("root");
    EXPECT_TRUE(writer.known_directory("root"));
    writer.queue_chunk(chunks.back());
    EXPECT_EQ(writer.completed_files(), 1U);
    EXPECT_TRUE(writer.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::data_chunk));

    ScanWriter scan_writer;
    const auto snapshot = scan_writer.snapshot_from_chunk(chunks.back());
    EXPECT_TRUE(snapshot.has_value());
    scan_writer.record_chunk(chunks.back());
    EXPECT_EQ(scan_writer.rows_written(), 1U);
    EXPECT_TRUE(scan_writer.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::file_snapshot));

    EXPECT_THROW(provider.push_back(JobMessage{hypersync::message_kinds::file_record, RecBuf{}}));
}

void test_metadata_stats_discarder_drops_records_and_reports_totals() {
    MetadataStatsDiscarder discarder({true, 5, "stderr"});
    discarder.start();

    RecBuf first = hypersync::make_recbuf({"folder/a.txt", "abc", 10});
    RecBuf second = hypersync::make_recbuf({"folder/nested/b.bin", "012345", 11});
    discarder.push_back(JobMessage{hypersync::message_kinds::file_record, first});
    discarder.discard_record(second);
    FolderRecord empty_folder;
    empty_folder.rel_path = "empty";
    discarder.push_back(JobMessage{hypersync::message_kinds::folder_record, empty_folder});

    JobMessage output;
    EXPECT_FALSE(discarder.pull(output));

    const auto snapshot = discarder.snapshot();
    EXPECT_EQ(snapshot.records_discarded, 2U);
    EXPECT_EQ(snapshot.files_found, 2U);
    EXPECT_EQ(snapshot.folders_found, 3U);
    EXPECT_EQ(snapshot.logical_size_bytes, 9ULL);
    EXPECT_TRUE(snapshot.records_per_second >= 0.0);

    const JobStats stats = discarder.stats();
    EXPECT_TRUE(stats.running);
    EXPECT_EQ(stats.accepted, 3U);
    EXPECT_EQ(stats.deferred, 2U);
    discarder.stop();

    EXPECT_THROW(MetadataStatsDiscarder({true, 0, "stderr"}));
    EXPECT_THROW(MetadataStatsDiscarder({true, 5, "file"}));
    EXPECT_THROW(discarder.push_back(JobMessage{hypersync::message_kinds::data_chunk, DataChunk{}}));
}

void test_threaded_job_rethrows_worker_failures_after_joining() {
    class ThrowingJob final : public ThreadedJob {
    public:
        ThrowingJob()
            : ThreadedJob(2) {}

        std::atomic<std::size_t> workers_entered {0};
        std::atomic<bool> stop_hook_called {false};

    private:
        void run_worker(std::size_t worker_index) override {
            workers_entered.fetch_add(1U, std::memory_order_relaxed);
            if (worker_index == 0U) {
                throw std::runtime_error("worker failure");
            }
            while (!stop_requested()) {
                std::this_thread::yield();
            }
        }

        void on_stop_requested() override {
            stop_hook_called.store(true, std::memory_order_release);
        }
    };

    ThrowingJob job;
    job.start();
    EXPECT_THROW(job.wait());
    EXPECT_TRUE(job.workers_entered.load(std::memory_order_acquire) >= 1U);
    EXPECT_TRUE(job.stop_hook_called.load(std::memory_order_acquire));
    EXPECT_FALSE(job.running());
}

void test_metadata_record_writer_writes_csv_and_text_inventory() {
    TempDir output("hypersync_metadata_record_writer");

    FileSpec file;
    file.rel_path = "folder/a.txt";
    file.declared_size = 7;
    file.mtime = 123;
    file.mode = 0644;
    file.uid = 10;
    file.gid = 20;
    file.hash_algorithm = "sha256";
    file.content_hash = "abc123";
    file.hash_block_size = 4;
    file.hash_block_count = 2;
    file.block_hash_algorithm = "md5";
    file.block_hashes = "block1;block2";

    MetadataFolderRecord folder;
    folder.spec.rel_path = "folder";
    folder.spec.mtime = 456;
    folder.spec.mode = 0755;
    folder.spec.uid = 11;
    folder.spec.gid = 21;
    folder.flat_file_count = 1;
    folder.flat_logical_size_bytes = 7;

    const fs::path csv_path = output.path / "metadata.csv";
    MetadataRecordWriter csv_writer({true, csv_path, MetadataRecordFormat::csv, true, true, true});
    csv_writer.write_batch(std::vector<FileSpec>{file}, std::vector<MetadataFolderRecord>{folder});
    EXPECT_EQ(csv_writer.files_written(), 1U);
    EXPECT_EQ(csv_writer.folders_written(), 1U);
    const std::string csv = hypersync::read_file_contents(csv_path);
    EXPECT_TRUE(csv.find("record_type,rel_path,size,mtime,mode,uid,gid,flat_file_count,flat_logical_size_bytes,"
                         "hash_algorithm,content_hash,hash_block_size,hash_block_count,block_hash_algorithm,block_hashes") !=
                std::string::npos);
    EXPECT_TRUE(csv.find("scan_run_id,run_started_at_utc,run_started_unix_ns,source_root,run_settings") !=
                std::string::npos);
    EXPECT_TRUE(csv.find("file,folder/a.txt,7,123,420,10,20,,,sha256,abc123,4,2,md5,block1;block2") !=
                std::string::npos);
    EXPECT_TRUE(csv.find("folder,folder,,456,493,11,21,1,7") != std::string::npos);

    const fs::path text_path = output.path / "metadata.txt";
    MetadataRecordWriter text_writer({true, text_path, MetadataRecordFormat::text, true, true, true});
    text_writer.write_batch(std::vector<FileSpec>{file}, std::vector<MetadataFolderRecord>{folder});
    const std::string text = hypersync::read_file_contents(text_path);
    EXPECT_TRUE(text.find("file rel_path=\"folder/a.txt\"") != std::string::npos);
    EXPECT_TRUE(text.find("hash_algorithm=\"sha256\"") != std::string::npos);
    EXPECT_TRUE(text.find("content_hash=\"abc123\"") != std::string::npos);
    EXPECT_TRUE(text.find("hash_block_size=4") != std::string::npos);
    EXPECT_TRUE(text.find("block_hash_algorithm=\"md5\"") != std::string::npos);
    EXPECT_TRUE(text.find("folder rel_path=\"folder\"") != std::string::npos);
    EXPECT_TRUE(text.find("flat_file_count=1") != std::string::npos);

#if !HYPERSYNC_HAS_DUCKDB
    EXPECT_THROW(MetadataRecordWriter({true,
                                       output.path / "metadata.parquet",
                                       MetadataRecordFormat::parquet,
                                       true,
                                       true,
                                       false}));
#endif
}

void test_file_metadata_generator_feeds_metadata_writer() {
    TempDir output("hypersync_file_metadata_generator");
    hypersync::FileMetadataGenerator generator(
        hypersync::FileMetadataGeneratorConfig(10U, 3U, 4U, 1024U, 123U, "synthetic"));

    std::uint64_t files = 0;
    std::uint64_t folders = 0;
    std::uint64_t logical_size = 0;
    hypersync::FileMetadataGeneratorBatch batch;

    const fs::path csv_path = output.path / "generated.csv";
    MetadataRecordWriter writer({true, csv_path, MetadataRecordFormat::csv, true, true, false});
    while (generator.next_batch(batch)) {
        for (const auto& file : batch.files) {
            logical_size += file.declared_size;
        }
        files += batch.files.size();
        folders += batch.folders.size();
        writer.write_batch(batch.files, batch.folders);
    }
    writer.close();

    EXPECT_EQ(files, 10ULL);
    EXPECT_EQ(folders, 3ULL);
    EXPECT_EQ(generator.files_generated(), 10ULL);
    EXPECT_EQ(generator.folders_generated(), 3ULL);
    EXPECT_EQ(writer.files_written(), 10U);
    EXPECT_EQ(writer.folders_written(), 3U);
    EXPECT_TRUE(logical_size > 0U);

    const std::string csv = hypersync::read_file_contents(csv_path);
    EXPECT_TRUE(csv.find("file,synthetic/dir_00000000/file_000000000000.dat") != std::string::npos);
    EXPECT_TRUE(csv.find("folder,synthetic/dir_00000000") != std::string::npos);
}

void test_queue_job_accepts_custom_message_kinds_without_shared_header_changes() {
    struct CustomPayload {
        int number = 0;
    };

    class CustomJob : public hypersync::TypedQueueJob<CustomPayload> {
    public:
        CustomJob() : TypedQueueJob("custom_job", "custom.payload") {}

        void publish_custom(CustomPayload payload) {
            publish_item(std::move(payload));
        }
    };

    CustomJob job;
    job.start();
    job.publish_custom({42});

    JobMessage message;
    EXPECT_TRUE(job.pull(message));
    EXPECT_EQ(message.kind, "custom.payload");
    EXPECT_EQ(hypersync::message_as<CustomPayload>(message).number, 42);
}

void test_spsc_ring_preserves_order_and_handles_cross_thread_transfer() {
    EXPECT_THROW(SpscRing<int>(3));

    SpscRing<int> ring(4);
    EXPECT_TRUE(ring.empty());
    EXPECT_EQ(ring.capacity(), 4U);
    EXPECT_TRUE(ring.try_push(1));
    EXPECT_TRUE(ring.try_push(2));
    EXPECT_TRUE(ring.try_push(3));
    EXPECT_TRUE(ring.try_push(4));
    EXPECT_TRUE(ring.full());
    EXPECT_FALSE(ring.try_push(5));

    int value = 0;
    EXPECT_TRUE(ring.try_pop(value));
    EXPECT_EQ(value, 1);
    EXPECT_TRUE(ring.try_pop(value));
    EXPECT_EQ(value, 2);
    EXPECT_TRUE(ring.try_push(5));
    EXPECT_TRUE(ring.try_pop(value));
    EXPECT_EQ(value, 3);
    EXPECT_TRUE(ring.try_pop(value));
    EXPECT_EQ(value, 4);
    EXPECT_TRUE(ring.try_pop(value));
    EXPECT_EQ(value, 5);
    EXPECT_FALSE(ring.try_pop(value));

    SpscRing<std::uint64_t> threaded_ring(1024);
    constexpr std::uint64_t total = 100'000;
    std::atomic<bool> producer_done = false;
    std::thread producer([&] {
        for (std::uint64_t next = 0; next < total; ++next) {
            while (!threaded_ring.try_push(next)) {
                std::this_thread::yield();
            }
        }
        producer_done = true;
    });

    for (std::uint64_t expected = 0; expected < total;) {
        std::uint64_t observed = 0;
        if (!threaded_ring.try_pop(observed)) {
            std::this_thread::yield();
            continue;
        }
        EXPECT_EQ(observed, expected);
        ++expected;
    }
    producer.join();
    EXPECT_TRUE(producer_done);
    EXPECT_TRUE(threaded_ring.empty());
}

void test_config_store_reads_sections_merges_defaults_and_reloads() {
    TempDir root("hypersync_config_store");
    const fs::path config_path = root.path / "override.yaml";
    {
        std::ofstream output(config_path);
        output << "jobs:\n";
        output << "  defaults:\n";
        output << "    connection_count: 12\n";
        output << "  data_sender:\n";
        output << "    zero_copy: true\n";
        output << "    socket_buffer_bytes: 4096\n";
        output << "  nfs_meta_reader:\n";
        output << "    worker_count: 11\n";
        output << "    async_directory_depth: 256\n";
        output << "  nfs_data_reader:\n";
        output << "    data_reader_worker_count: 3\n";
        output << "  checker:\n";
        output << "    worker_count: 9\n";
        output << "    target_request_queue_depth: 1234\n";
        output << "    batch_queue_depth: 5678\n";
        output << "  input_provider:\n";
        output << "    max_queue_entries: 44\n";
        output << "    refill_threshold: 5\n";
        output << "    refill_batch_size: 9\n";
        output << "    input_csv_path: custom-input.csv\n";
        output << "    overflow_csv_path: custom-overflow.csv\n";
    }

    hypersync::ConfigStore config(
        std::vector<fs::path>{hypersync::default_config_path(), config_path});

    const auto raw_section = config.section("jobs.input_provider");
    EXPECT_EQ(raw_section.at("input_csv_path"), "custom-input.csv");
    EXPECT_EQ(raw_section.at("overflow_csv_path"), "custom-overflow.csv");
    EXPECT_EQ(config.section("jobs.defaults").at("connection_count"), "12");

    const auto merged = config.merged_sections(hypersync::default_job_config_sections("data_sender"));
    EXPECT_EQ(merged.at("connection_count"), "8");
    EXPECT_EQ(merged.at("zero_copy"), "true");

    const auto input_config = hypersync::load_input_provider_config(config);
    EXPECT_EQ(input_config.max_queue_entries, 44U);
    EXPECT_EQ(input_config.refill_threshold, 5U);
    EXPECT_EQ(input_config.refill_batch_size, 9U);

    const auto sender_config = hypersync::load_data_sender_config(config);
    EXPECT_EQ(sender_config.connection_count, 8U);
    EXPECT_EQ(sender_config.socket_buffer_bytes, 4096ULL);
    EXPECT_TRUE(sender_config.zero_copy);

    const auto meta_config = hypersync::load_nfs_meta_reader_config(config);
    EXPECT_EQ(meta_config.worker_count, 11U);
    EXPECT_EQ(meta_config.thread_count, 11U);
    EXPECT_EQ(meta_config.async_directory_depth, 256U);

    const auto data_reader_config = hypersync::load_nfs_data_reader_config(config);
    EXPECT_EQ(data_reader_config.data_reader_worker_count, 3U);

    const auto checker_config = hypersync::load_checker_config(config);
    EXPECT_EQ(checker_config.worker_count, 9U);
    EXPECT_EQ(checker_config.target_request_queue_depth, 1234U);
    EXPECT_EQ(checker_config.batch_queue_depth, 5678U);

    {
        std::ofstream output(config_path);
        output << "jobs:\n";
        output << "  defaults:\n";
        output << "    connection_count: 3\n";
        output << "  data_sender:\n";
        output << "    zero_copy: false\n";
        output << "    socket_buffer_bytes: 2048\n";
    }
    config.reload();
    EXPECT_EQ(config.section("jobs.defaults").at("connection_count"), "3");
    const auto reloaded_sender = hypersync::load_data_sender_config(config);
    EXPECT_EQ(reloaded_sender.connection_count, 8U);
    EXPECT_EQ(reloaded_sender.socket_buffer_bytes, 2048ULL);
    EXPECT_FALSE(reloaded_sender.zero_copy);
}

void test_nfs_backend_local_fallback_and_optional_libnfs_gate() {
    TempDir source("hypersync_nfs_backend");
    write_file(source.path / "nested" / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "01234567");
    write_file(source.path / "nested" / "child" / "gamma.dat", "gamma");

    EXPECT_FALSE(hypersync::is_nfs_url(source.path.string()));
    EXPECT_TRUE(hypersync::is_nfs_url("nfs://127.0.0.1/export"));

    auto backend = hypersync::make_nfs_backend(source.path.string());
    EXPECT_FALSE(backend->uses_async_api());
    const auto listed = backend->list_files(true);
    EXPECT_EQ(listed.size(), 3U);
    const auto loaded = backend->load_file("nested/alpha.txt");
    EXPECT_EQ(loaded.content, "alpha");
    EXPECT_EQ(loaded.rel_path, "nested/alpha.txt");

    std::set<std::string> flat_files;
    std::set<std::string> flat_directories;
    backend->visit_folder(
        "nested",
        [&flat_files](FileSpec spec) {
            flat_files.insert(std::move(spec.rel_path));
        },
        [&flat_directories](FileSpec spec) {
            flat_directories.insert(std::move(spec.rel_path));
        });
    const std::set<std::string> expected_files{"nested/alpha.txt", "nested/beta.bin"};
    const std::set<std::string> expected_directories{"nested/child"};
    EXPECT_EQ(flat_files, expected_files);
    EXPECT_EQ(flat_directories, expected_directories);

    if (!hypersync::libnfs_support_enabled()) {
        EXPECT_THROW(hypersync::make_nfs_backend("nfs://127.0.0.1/export/root"));
    }
}

void test_nfs_url_server_range_expansion() {
    const auto expanded = hypersync::expand_nfs_url_server_candidates(
        "nfs://172.27.255.2-172.27.255.5/volumes/example/data");
    const std::vector<std::string> expected{
        "nfs://172.27.255.2/volumes/example/data",
        "nfs://172.27.255.3/volumes/example/data",
        "nfs://172.27.255.4/volumes/example/data",
        "nfs://172.27.255.5/volumes/example/data",
    };
    EXPECT_EQ(expanded, expected);

    const auto shorthand = hypersync::expand_nfs_url_server_candidates("nfs://10.0.0.8-10/export");
    const std::vector<std::string> shorthand_expected{
        "nfs://10.0.0.8/export",
        "nfs://10.0.0.9/export",
        "nfs://10.0.0.10/export",
    };
    EXPECT_EQ(shorthand, shorthand_expected);

    const auto comma_separated = hypersync::expand_nfs_url_server_candidates(
        "nfs://172.27.255.2,172.27.255.9-172.27.255.10/export");
    const std::vector<std::string> comma_expected{
        "nfs://172.27.255.2/export",
        "nfs://172.27.255.9/export",
        "nfs://172.27.255.10/export",
    };
    EXPECT_EQ(comma_separated, comma_expected);

    const auto hostname_with_dash = hypersync::expand_nfs_url_server_candidates("nfs://nfs-prod-a/export");
    EXPECT_EQ(hostname_with_dash.size(), 1U);
    EXPECT_EQ(hostname_with_dash.front(), "nfs://nfs-prod-a/export");
}

void test_privilege_utils_require_root_for_owner_change() {
    EXPECT_FALSE(hypersync::running_as_root());
    EXPECT_THROW(hypersync::require_root_for_owner_change("nested/file.txt", "file", 0, 0, 1234, 5678));
}

void test_filesystem_utils_collect_and_apply_metadata_helpers() {
    TempDir root("hypersync_filesystem_utils");
    write_file(root.path / "nested" / "alpha.txt", "alpha");
    fs::create_directories(root.path / "empty");

    const auto recursive_files = hypersync::collect_file_specs(root.path, true);
    EXPECT_EQ(recursive_files.size(), 1U);
    EXPECT_EQ(recursive_files.front().rel_path, "nested/alpha.txt");

    const auto top_level_files = hypersync::collect_file_specs(root.path, false);
    EXPECT_EQ(top_level_files.size(), 0U);

    const auto recursive_directories = hypersync::collect_directory_specs(root.path, true);
    EXPECT_EQ(recursive_directories.size(), 2U);
    const auto top_level_directories = hypersync::collect_directory_specs(root.path, false);
    EXPECT_EQ(top_level_directories.size(), 2U);

    EXPECT_THROW(hypersync::collect_file_specs(root.path / "missing", true));
    EXPECT_THROW(hypersync::collect_directory_specs(root.path / "nested" / "alpha.txt", true));
    EXPECT_THROW(hypersync::read_file_contents(root.path / "missing.txt"));
    EXPECT_THROW(hypersync::file_hash64(root.path / "missing.txt"));

    const fs::path generated_file = root.path / "out" / "deep" / "file.txt";
    hypersync::ensure_parent_directories(generated_file);
    EXPECT_TRUE(fs::is_directory(root.path / "out" / "deep"));
    write_file(generated_file, "x");

    const std::uint32_t uid = static_cast<std::uint32_t>(::geteuid());
    const std::uint32_t gid = static_cast<std::uint32_t>(::getegid());
    const std::uint64_t file_mtime = 1'700'004'000ULL * 1'000'000'000ULL + 111'222'333ULL;
    const std::uint64_t dir_mtime = 1'700'004'100ULL * 1'000'000'000ULL + 444'555'666ULL;

    FileSpec file_spec;
    file_spec.rel_path = "out/deep/file.txt";
    file_spec.mtime = file_mtime;
    file_spec.mode = 0600;
    file_spec.uid = uid;
    file_spec.gid = gid;
    hypersync::apply_file_metadata(generated_file, file_spec);

    FileSpec dir_spec;
    dir_spec.rel_path = "out/deep";
    dir_spec.mtime = dir_mtime;
    dir_spec.mode = 0750;
    dir_spec.uid = uid;
    dir_spec.gid = gid;
    hypersync::apply_directory_metadata(root.path / "out" / "deep", dir_spec);

    const auto file_stat = read_stat_snapshot(generated_file);
    EXPECT_EQ(file_stat.mode, 0600U);
    EXPECT_EQ(file_stat.uid, uid);
    EXPECT_EQ(file_stat.gid, gid);
    EXPECT_EQ(file_stat.mtime, file_mtime);

    const auto dir_stat = read_stat_snapshot(root.path / "out" / "deep");
    EXPECT_EQ(dir_stat.mode, 0750U);
    EXPECT_EQ(dir_stat.uid, uid);
    EXPECT_EQ(dir_stat.gid, gid);
    EXPECT_EQ(dir_stat.mtime, dir_mtime);

    FileSpec invalid_owner = file_spec;
    invalid_owner.uid = uid + 1U;
    EXPECT_THROW(hypersync::apply_file_metadata(generated_file, invalid_owner));
}

void test_local_target_backend_restores_directory_and_file_metadata() {
    TempDir target("hypersync_local_backend_target");

    auto writer = hypersync::make_target_writer_backend(target.path.string());
    auto backend = hypersync::make_nfs_backend(target.path.string());

    const std::uint32_t uid = static_cast<std::uint32_t>(::geteuid());
    const std::uint32_t gid = static_cast<std::uint32_t>(::getegid());
    const std::uint64_t dir_mtime = 1'700'002'000ULL * 1'000'000'000ULL + 111'222'333ULL;
    const std::uint64_t file_mtime = 1'700'002'100ULL * 1'000'000'000ULL + 444'555'666ULL;

    FileSpec directory;
    directory.rel_path = "nested";
    directory.mtime = dir_mtime;
    directory.mode = 0750;
    directory.uid = uid;
    directory.gid = gid;
    writer->ensure_directory(directory);

    FileSpec file;
    file.rel_path = "nested/file.txt";
    file.declared_size = 3;
    file.mtime = file_mtime;
    file.mode = 0600;
    file.uid = uid;
    file.gid = gid;
    writer->write_chunk(file, "abc", 0);
    writer->finish_file(file);
    writer->apply_directory_metadata(directory);
    writer->abort_file("nested/missing.txt");

    EXPECT_EQ(hypersync::read_file_contents(target.path / "nested" / "file.txt"), "abc");
    EXPECT_EQ(backend->file_hash("nested/file.txt"), hypersync::hash64("abc"));
    EXPECT_TRUE(backend->metadata_matches("nested/file.txt", 3, file_mtime));
    EXPECT_EQ(backend->list_directories(true).size(), 1U);

    const auto file_stat = backend->stat_path("nested/file.txt");
    EXPECT_TRUE(file_stat.has_value());
    EXPECT_EQ(file_stat->mode, 0600U);
    EXPECT_EQ(file_stat->uid, uid);
    EXPECT_EQ(file_stat->gid, gid);

    const auto dir_stat = read_stat_snapshot(target.path / "nested");
    EXPECT_EQ(dir_stat.mode, 0750U);
    EXPECT_EQ(dir_stat.uid, uid);
    EXPECT_EQ(dir_stat.gid, gid);
    EXPECT_EQ(dir_stat.mtime, dir_mtime);
}

void test_null_target_backend_discards_regular_and_batch_writes() {
    EXPECT_TRUE(hypersync::is_null_url("null"));
    EXPECT_TRUE(hypersync::is_null_url("null:"));
    EXPECT_TRUE(hypersync::is_null_url("null://"));
    EXPECT_FALSE(hypersync::is_null_url("nfs://example/export"));

    auto writer = hypersync::make_target_writer_backend("null://");

    FileSpec directory;
    directory.rel_path = "nested";
    writer->ensure_directory(directory);
    writer->apply_directory_metadata(directory);

    FileSpec file;
    file.rel_path = "nested/file.txt";
    file.declared_size = 3;
    file.mode = 0644;
    writer->write_chunk(file, "abc", 0);
    writer->finish_file(file);

    std::vector<hypersync::TargetWriterBackend::WriteChunk> files;
    hypersync::TargetWriterBackend::WriteChunk chunk;
    chunk.spec = file;
    chunk.data = "abc";
    chunk.last_chunk = true;
    files.push_back(chunk);
    writer->write_chunks(files);
    writer->write_files(files);
    writer->abort_file("nested/file.txt");

    EXPECT_FALSE(writer->uses_async_api());
    EXPECT_EQ(writer->file_hash("nested/file.txt"), hypersync::hash64("nested/file.txt"));
}

void test_real_libnfs_backend_directory_and_target_writer_paths() {
    if (!hypersync::libnfs_support_enabled()) {
        return;
    }

    TempDir export_root("hypersync_real_nfs_backend_export");
    ScopedNfsExport exported(export_root.path);
    const std::string export_url = exported.url();

    auto writer = hypersync::make_target_writer_backend(export_url);
    auto backend = hypersync::make_nfs_backend(export_url);

    const std::uint32_t uid = static_cast<std::uint32_t>(::geteuid());
    const std::uint32_t gid = static_cast<std::uint32_t>(::getegid());
    const std::uint64_t dir_mtime = 1'700'003'000ULL * 1'000'000'000ULL + 111'111'000ULL;
    const std::uint64_t child_dir_mtime = 1'700'003'100ULL * 1'000'000'000ULL + 222'222'000ULL;
    const std::uint64_t file_mtime = 1'700'003'200ULL * 1'000'000'000ULL + 333'333'000ULL;

    FileSpec directory;
    directory.rel_path = "nested";
    directory.mtime = dir_mtime;
    directory.mode = 0750;
    directory.uid = uid;
    directory.gid = gid;
    writer->ensure_directory(directory);

    FileSpec child_directory;
    child_directory.rel_path = "empty/child";
    child_directory.mtime = child_dir_mtime;
    child_directory.mode = 0700;
    child_directory.uid = uid;
    child_directory.gid = gid;
    writer->ensure_directory(child_directory);
    writer->apply_directory_metadata(child_directory);

    FileSpec file;
    file.rel_path = "nested/file.txt";
    file.declared_size = 3;
    file.mtime = file_mtime;
    file.mode = 0640;
    file.uid = uid;
    file.gid = gid;
    writer->write_chunk(file, "abc", 0);
    writer->finish_file(file);
    writer->apply_directory_metadata(directory);
    writer->abort_file("nested/missing.txt");

    EXPECT_EQ(backend->load_file("nested/file.txt").content, "abc");
    EXPECT_EQ(backend->file_hash("nested/file.txt"), hypersync::hash64("abc"));
    EXPECT_TRUE(backend->metadata_matches("nested/file.txt", 3, file_mtime));
    const auto directories = backend->list_directories(true);
    EXPECT_EQ(directories.size(), 3U);

    const auto file_stat = backend->stat_path("nested/file.txt");
    EXPECT_TRUE(file_stat.has_value());
    EXPECT_EQ(file_stat->mode, 0640U);
    EXPECT_EQ(file_stat->uid, uid);
    EXPECT_EQ(file_stat->gid, gid);

    const auto dir_stat = read_stat_snapshot(export_root.path / "nested");
    EXPECT_EQ(dir_stat.mode, 0750U);
    EXPECT_EQ(dir_stat.uid, uid);
    EXPECT_EQ(dir_stat.gid, gid);
    EXPECT_EQ(dir_stat.mtime, dir_mtime);

    const auto child_dir_stat = read_stat_snapshot(export_root.path / "empty" / "child");
    EXPECT_EQ(child_dir_stat.mode, 0700U);
    EXPECT_EQ(child_dir_stat.uid, uid);
    EXPECT_EQ(child_dir_stat.gid, gid);
    EXPECT_EQ(child_dir_stat.mtime, child_dir_mtime);
}

void test_nfs_jobs_use_backend_for_local_sources() {
    TempDir source("hypersync_nfs_jobs");
    write_file(source.path / "a.txt", "abc");
    write_file(source.path / "sub" / "b.bin", "0123456789");
    write_file(source.path / "sub" / "deeper" / "c.bin", "xyz");

    NfsMetaReader meta_reader({1, 1'000'000, 1'000'000, true, source.path.string(), true});
    const auto scanned = meta_reader.scan_tree();
    EXPECT_EQ(scanned.size(), 3U);
    EXPECT_FALSE(meta_reader.using_async_backend());
    meta_reader.publish_tree();

    JobMessage message;
    EXPECT_TRUE(meta_reader.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::file_record));
    EXPECT_TRUE(meta_reader.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::file_record));
    EXPECT_TRUE(meta_reader.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::file_record));

    NfsMetaReader flat_reader({1, 1'000'000, 1'000'000, true, source.path.string(), true});
    FolderRecord active_folder;
    active_folder.rel_path = "sub";
    flat_reader.begin_folder(active_folder);
    flat_reader.publish_tree();
    EXPECT_EQ(flat_reader.files_seen(), 1U);
    EXPECT_EQ(flat_reader.folders_completed(), 1U);
    const auto completed_folder = flat_reader.active_folder();
    EXPECT_TRUE(completed_folder.has_value());
    EXPECT_EQ(completed_folder->files_total, 1ULL);
    EXPECT_EQ(completed_folder->reading_offset, 1ULL);
    const auto child_folders = flat_reader.take_discovered_folders();
    EXPECT_EQ(child_folders.size(), 1U);
    EXPECT_EQ(child_folders.front().rel_path, "sub/deeper");
    EXPECT_TRUE(flat_reader.pull(message));
    EXPECT_EQ(hypersync::message_as<RecBuf>(message).rel_path.view(), std::string_view("sub/b.bin"));
    EXPECT_FALSE(flat_reader.pull(message));

    NfsDataReader data_reader({2, 256, 0, 8, 4, 8, 85.0, 70.0, source.path.string()});
    EXPECT_FALSE(data_reader.using_async_backend());
    const auto loaded = data_reader.load_file("sub/b.bin");
    EXPECT_EQ(loaded.content, "0123456789");
    EXPECT_EQ(data_reader.read_file_bytes(loaded), 10ULL);
    const auto chunks = data_reader.read_file("sub/b.bin");
    EXPECT_EQ(chunks.size(), 2U);
    data_reader.publish_path("a.txt");
    EXPECT_TRUE(data_reader.pull(message));
    EXPECT_EQ(message.kind, std::string(hypersync::message_kinds::data_chunk));
}

void test_phase1_runtime_transfers_directory_over_tcp() {
    TempDir source("hypersync_source");
    TempDir target("hypersync_target");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "0123456789abcdef");

    std::uint16_t priority_port = pick_unused_port();
    std::uint16_t data_port = pick_unused_port();
    while (data_port == priority_port) {
        data_port = pick_unused_port();
    }

    EngineConfig config;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 8;

    TransferEngine receiver_engine(config);
    hypersync::ReceiverRuntimeConfig receiver_runtime;
    receiver_runtime.target_root = target.path;
    receiver_runtime.bind_host = "127.0.0.1";
    receiver_runtime.priority_port = priority_port;
    receiver_runtime.data_port = data_port;

    std::exception_ptr receiver_error;
    std::thread receiver([&] {
        try {
            receiver_engine.run_receiver(receiver_runtime);
        } catch (...) {
            receiver_error = std::current_exception();
        }
    });

    TransferEngine sender_engine(config);
    hypersync::SenderRuntimeConfig sender_runtime;
    sender_runtime.source_root = source.path;
    sender_runtime.remote_host = "127.0.0.1";
    sender_runtime.priority_port = priority_port;
    sender_runtime.data_port = data_port;
    sender_runtime.recursive = true;

    const auto report = sender_engine.transfer_directory(sender_runtime);
    receiver.join();
    if (receiver_error != nullptr) {
        std::rethrow_exception(receiver_error);
    }

    EXPECT_EQ(report.files_total, 2U);
    EXPECT_EQ(report.files_transferred, 2U);
    EXPECT_EQ(report.files_failed, 0U);
    EXPECT_EQ(report.files_skipped, 0U);
    EXPECT_EQ(report.chunks_sent, 3U);
    EXPECT_EQ(hypersync::read_file_contents(target.path / "alpha.txt"), "alpha");
    EXPECT_EQ(hypersync::read_file_contents(target.path / "nested" / "beta.bin"), "0123456789abcdef");
}

void test_runtime_packs_small_files_over_tcp() {
    TempDir source("hypersync_small_pack_source");
    TempDir target("hypersync_small_pack_target");

    for (std::size_t index = 0; index < 64; ++index) {
        write_file(source.path / "small" / ("file_" + std::to_string(index) + ".txt"), "abc");
    }

    std::uint16_t priority_port = pick_unused_port();
    std::uint16_t data_port = pick_unused_port();
    while (data_port == priority_port) {
        data_port = pick_unused_port();
    }

    EngineConfig config;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 4096;
    config.small_pool_slots = 8;
    config.large_pool_slots = 8;

    TransferEngine receiver_engine(config);
    hypersync::ReceiverRuntimeConfig receiver_runtime;
    receiver_runtime.target_root = target.path;
    receiver_runtime.bind_host = "127.0.0.1";
    receiver_runtime.priority_port = priority_port;
    receiver_runtime.data_port = data_port;

    std::exception_ptr receiver_error;
    std::thread receiver([&] {
        try {
            receiver_engine.run_receiver(receiver_runtime);
        } catch (...) {
            receiver_error = std::current_exception();
        }
    });

    TransferEngine sender_engine(config);
    hypersync::SenderRuntimeConfig sender_runtime;
    sender_runtime.source_root = source.path;
    sender_runtime.remote_host = "127.0.0.1";
    sender_runtime.priority_port = priority_port;
    sender_runtime.data_port = data_port;
    sender_runtime.recursive = true;

    const auto report = sender_engine.transfer_directory(sender_runtime);
    receiver.join();
    if (receiver_error != nullptr) {
        std::rethrow_exception(receiver_error);
    }

    EXPECT_EQ(report.files_total, 64U);
    EXPECT_EQ(report.files_transferred, 64U);
    EXPECT_EQ(report.files_failed, 0U);
    EXPECT_TRUE(report.chunks_sent < report.files_transferred);
    EXPECT_EQ(hypersync::read_file_contents(target.path / "small" / "file_0.txt"), "abc");
    EXPECT_EQ(hypersync::read_file_contents(target.path / "small" / "file_63.txt"), "abc");
}

void test_phase2_runtime_spills_to_cache_and_honors_backpressure() {
    TempDir source("hypersync_phase2_source");
    TempDir target("hypersync_phase2_target");
    TempDir cache("hypersync_phase2_cache");

    const std::string payload = "abcdefghijklmnopqrstuvwxyz012345";
    write_file(source.path / "large.bin", payload);

    std::uint16_t priority_port = pick_unused_port();
    std::uint16_t data_port = pick_unused_port();
    while (data_port == priority_port) {
        data_port = pick_unused_port();
    }

    EngineConfig config;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 8;

    TransferEngine receiver_engine(config);
    hypersync::ReceiverRuntimeConfig receiver_runtime;
    receiver_runtime.target_root = target.path;
    receiver_runtime.bind_host = "127.0.0.1";
    receiver_runtime.priority_port = priority_port;
    receiver_runtime.data_port = data_port;
    receiver_runtime.backpressure_window_bytes = 8;
    receiver_runtime.backpressure_pause_ms = 2;

    std::exception_ptr receiver_error;
    std::thread receiver([&] {
        try {
            receiver_engine.run_receiver(receiver_runtime);
        } catch (...) {
            receiver_error = std::current_exception();
        }
    });

    TransferEngine sender_engine(config);
    hypersync::SenderRuntimeConfig sender_runtime;
    sender_runtime.source_root = source.path;
    sender_runtime.remote_host = "127.0.0.1";
    sender_runtime.priority_port = priority_port;
    sender_runtime.data_port = data_port;
    sender_runtime.recursive = true;
    sender_runtime.cache_root = cache.path;
    sender_runtime.cache_file_threshold_bytes = 8;

    const auto report = sender_engine.transfer_directory(sender_runtime);
    receiver.join();
    if (receiver_error != nullptr) {
        std::rethrow_exception(receiver_error);
    }

    EXPECT_EQ(report.files_total, 1U);
    EXPECT_EQ(report.files_transferred, 1U);
    EXPECT_EQ(report.files_failed, 0U);
    EXPECT_EQ(report.chunks_sent, 4U);
    EXPECT_EQ(hypersync::read_file_contents(target.path / "large.bin"), payload);
    EXPECT_EQ(count_regular_files(cache.path), 0U);
}

void test_real_libnfs_loopback_export_can_scan_and_transfer() {
    if (!hypersync::libnfs_support_enabled()) {
        return;
    }

    TempDir source("hypersync_real_nfs_source");
    TempDir target("hypersync_real_nfs_target");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "0123456789abcdef");

    ScopedNfsExport exported_source(source.path);
    const std::string source_url = exported_source.url();

    auto backend = hypersync::make_nfs_backend(source_url);
    EXPECT_TRUE(backend->uses_async_api());
    EXPECT_EQ(backend->list_files(true).size(), 2U);
    EXPECT_EQ(backend->load_file("alpha.txt").content, "alpha");

    NfsMetaReader meta_reader({1, 1'000'000, 1'000'000, true, source_url, true});
    NfsDataReader data_reader({2, 256, 0, 8, 4, 8, 85.0, 70.0, source_url});
    EXPECT_TRUE(meta_reader.using_async_backend());
    EXPECT_TRUE(data_reader.using_async_backend());
    EXPECT_EQ(meta_reader.scan_tree().size(), 2U);
    EXPECT_EQ(data_reader.load_file("nested/beta.bin").content, "0123456789abcdef");

    std::uint16_t priority_port = pick_unused_port();
    std::uint16_t data_port = pick_unused_port();
    while (data_port == priority_port) {
        data_port = pick_unused_port();
    }

    EngineConfig config;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 8;

    TransferEngine receiver_engine(config);
    hypersync::ReceiverRuntimeConfig receiver_runtime;
    receiver_runtime.target_root = target.path;
    receiver_runtime.bind_host = "127.0.0.1";
    receiver_runtime.priority_port = priority_port;
    receiver_runtime.data_port = data_port;

    std::exception_ptr receiver_error;
    std::thread receiver([&] {
        try {
            receiver_engine.run_receiver(receiver_runtime);
        } catch (...) {
            receiver_error = std::current_exception();
        }
    });

    TransferEngine sender_engine(config);
    hypersync::SenderRuntimeConfig sender_runtime;
    sender_runtime.source_root = source_url;
    sender_runtime.remote_host = "127.0.0.1";
    sender_runtime.priority_port = priority_port;
    sender_runtime.data_port = data_port;
    sender_runtime.recursive = true;

    const auto report = sender_engine.transfer_directory(sender_runtime);
    receiver.join();
    if (receiver_error != nullptr) {
        std::rethrow_exception(receiver_error);
    }

    EXPECT_EQ(report.files_total, 2U);
    EXPECT_EQ(report.files_transferred, 2U);
    EXPECT_EQ(report.files_failed, 0U);
    EXPECT_EQ(hypersync::read_file_contents(target.path / "alpha.txt"), "alpha");
    EXPECT_EQ(hypersync::read_file_contents(target.path / "nested" / "beta.bin"), "0123456789abcdef");
}

void test_root_receiver_restores_local_standard_attributes() {
    if (!sudo_available()) {
        return;
    }

    TempDir source("hypersync_root_local_source");
    TempDir target("hypersync_root_local_target");
    TempDir logs("hypersync_root_local_logs");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "0123456789abcdef");
    fs::create_directories(source.path / "empty" / "child");

    const std::uint64_t alpha_mtime = 1'700'000'000ULL * 1'000'000'000ULL + 123'456'789ULL;
    const std::uint64_t beta_mtime = 1'700'000'100ULL * 1'000'000'000ULL + 234'567'890ULL;
    const std::uint64_t nested_mtime = 1'700'000'200ULL * 1'000'000'000ULL + 345'678'901ULL;
    const std::uint64_t empty_mtime = 1'700'000'300ULL * 1'000'000'000ULL + 456'789'012ULL;
    const std::uint64_t empty_child_mtime = 1'700'000'400ULL * 1'000'000'000ULL + 567'890'123ULL;

    set_path_metadata(source.path / "alpha.txt", 0640, alpha_mtime);
    set_path_metadata(source.path / "nested" / "beta.bin", 0600, beta_mtime);
    set_path_metadata(source.path / "nested", 0750, nested_mtime);
    set_path_metadata(source.path / "empty", 0711, empty_mtime);
    set_path_metadata(source.path / "empty" / "child", 0700, empty_child_mtime);

    EngineConfig config;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 8;

    auto run_once = [&](std::size_t expected_transferred, std::size_t expected_skipped) {
        std::uint16_t priority_port = pick_unused_port();
        std::uint16_t data_port = pick_unused_port();
        while (data_port == priority_port) {
            data_port = pick_unused_port();
        }

        auto receiver = launch_receiver_process(target.path.string(), priority_port, data_port, logs.path, true);
        TransferEngine sender_engine(config);
        hypersync::SenderRuntimeConfig sender_runtime;
        sender_runtime.source_root = source.path;
        sender_runtime.remote_host = "127.0.0.1";
        sender_runtime.priority_port = priority_port;
        sender_runtime.data_port = data_port;
        sender_runtime.recursive = true;

        const auto report = sender_engine.transfer_directory(sender_runtime);
        expect_child_success(receiver, "root local receiver");

        EXPECT_EQ(report.files_total, 2U);
        EXPECT_EQ(report.files_failed, 0U);
        EXPECT_EQ(report.files_transferred, expected_transferred);
        EXPECT_EQ(report.files_skipped, expected_skipped);
    };

    run_once(2U, 0U);

    const std::uint32_t expected_uid = static_cast<std::uint32_t>(::geteuid());
    const std::uint32_t expected_gid = static_cast<std::uint32_t>(::getegid());
    EXPECT_EQ(hypersync::read_file_contents(target.path / "alpha.txt"), "alpha");
    EXPECT_EQ(hypersync::read_file_contents(target.path / "nested" / "beta.bin"), "0123456789abcdef");
    EXPECT_TRUE(fs::is_directory(target.path / "empty" / "child"));

    const auto alpha_stat = read_stat_snapshot(target.path / "alpha.txt");
    EXPECT_EQ(alpha_stat.uid, expected_uid);
    EXPECT_EQ(alpha_stat.gid, expected_gid);
    EXPECT_EQ(alpha_stat.mode, 0640U);
    EXPECT_EQ(alpha_stat.mtime, alpha_mtime);

    const auto beta_stat = read_stat_snapshot(target.path / "nested" / "beta.bin");
    EXPECT_EQ(beta_stat.uid, expected_uid);
    EXPECT_EQ(beta_stat.gid, expected_gid);
    EXPECT_EQ(beta_stat.mode, 0600U);
    EXPECT_EQ(beta_stat.mtime, beta_mtime);

    const auto nested_stat = read_stat_snapshot(target.path / "nested");
    EXPECT_EQ(nested_stat.uid, expected_uid);
    EXPECT_EQ(nested_stat.gid, expected_gid);
    EXPECT_EQ(nested_stat.mode, 0750U);
    EXPECT_EQ(nested_stat.mtime, nested_mtime);

    const auto empty_stat = read_stat_snapshot(target.path / "empty");
    EXPECT_EQ(empty_stat.uid, expected_uid);
    EXPECT_EQ(empty_stat.gid, expected_gid);
    EXPECT_EQ(empty_stat.mode, 0711U);
    EXPECT_EQ(empty_stat.mtime, empty_mtime);

    const auto empty_child_stat = read_stat_snapshot(target.path / "empty" / "child");
    EXPECT_EQ(empty_child_stat.uid, expected_uid);
    EXPECT_EQ(empty_child_stat.gid, expected_gid);
    EXPECT_EQ(empty_child_stat.mode, 0700U);
    EXPECT_EQ(empty_child_stat.mtime, empty_child_mtime);

    run_once(0U, 2U);
}

void test_root_receiver_restores_nfs_standard_attributes_and_skips_on_retry() {
    if (!hypersync::libnfs_support_enabled() || !sudo_available()) {
        return;
    }

    TempDir source("hypersync_root_nfs_source");
    TempDir target("hypersync_root_nfs_target_export");
    TempDir logs("hypersync_root_nfs_logs");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "0123456789abcdef");
    fs::create_directories(source.path / "empty" / "child");

    const std::uint64_t alpha_mtime = 1'700'001'000ULL * 1'000'000'000ULL + 111'111'000ULL;
    const std::uint64_t beta_mtime = 1'700'001'100ULL * 1'000'000'000ULL + 222'222'000ULL;
    const std::uint64_t nested_mtime = 1'700'001'200ULL * 1'000'000'000ULL + 333'333'000ULL;
    const std::uint64_t empty_mtime = 1'700'001'300ULL * 1'000'000'000ULL + 444'444'000ULL;
    const std::uint64_t empty_child_mtime = 1'700'001'400ULL * 1'000'000'000ULL + 555'555'000ULL;

    set_path_metadata(source.path / "alpha.txt", 0644, alpha_mtime);
    set_path_metadata(source.path / "nested" / "beta.bin", 0600, beta_mtime);
    set_path_metadata(source.path / "nested", 0750, nested_mtime);
    set_path_metadata(source.path / "empty", 0711, empty_mtime);
    set_path_metadata(source.path / "empty" / "child", 0700, empty_child_mtime);

    ScopedNfsExport exported_target(target.path);
    const std::string target_url = exported_target.url();

    EngineConfig config;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 8;

    auto run_once = [&](std::size_t expected_transferred, std::size_t expected_skipped) {
        std::uint16_t priority_port = pick_unused_port();
        std::uint16_t data_port = pick_unused_port();
        while (data_port == priority_port) {
            data_port = pick_unused_port();
        }

        auto receiver = launch_receiver_process(target_url, priority_port, data_port, logs.path, true);
        TransferEngine sender_engine(config);
        hypersync::SenderRuntimeConfig sender_runtime;
        sender_runtime.source_root = source.path;
        sender_runtime.remote_host = "127.0.0.1";
        sender_runtime.priority_port = priority_port;
        sender_runtime.data_port = data_port;
        sender_runtime.recursive = true;

        const auto report = sender_engine.transfer_directory(sender_runtime);
        expect_child_success(receiver, "root nfs receiver");

        EXPECT_EQ(report.files_total, 2U);
        EXPECT_EQ(report.files_failed, 0U);
        EXPECT_EQ(report.files_transferred, expected_transferred);
        EXPECT_EQ(report.files_skipped, expected_skipped);
    };

    run_once(2U, 0U);

    const std::uint32_t expected_uid = static_cast<std::uint32_t>(::geteuid());
    const std::uint32_t expected_gid = static_cast<std::uint32_t>(::getegid());
    EXPECT_EQ(hypersync::read_file_contents(target.path / "alpha.txt"), "alpha");
    EXPECT_EQ(hypersync::read_file_contents(target.path / "nested" / "beta.bin"), "0123456789abcdef");
    EXPECT_TRUE(fs::is_directory(target.path / "empty" / "child"));

    const auto alpha_stat = read_stat_snapshot(target.path / "alpha.txt");
    EXPECT_EQ(alpha_stat.uid, expected_uid);
    EXPECT_EQ(alpha_stat.gid, expected_gid);
    EXPECT_EQ(alpha_stat.mode, 0644U);
    EXPECT_EQ(alpha_stat.mtime, alpha_mtime);

    const auto beta_stat = read_stat_snapshot(target.path / "nested" / "beta.bin");
    EXPECT_EQ(beta_stat.uid, expected_uid);
    EXPECT_EQ(beta_stat.gid, expected_gid);
    EXPECT_EQ(beta_stat.mode, 0600U);
    EXPECT_EQ(beta_stat.mtime, beta_mtime);

    const auto nested_stat = read_stat_snapshot(target.path / "nested");
    EXPECT_EQ(nested_stat.uid, expected_uid);
    EXPECT_EQ(nested_stat.gid, expected_gid);
    EXPECT_EQ(nested_stat.mode, 0750U);
    EXPECT_EQ(nested_stat.mtime, nested_mtime);

    const auto empty_stat = read_stat_snapshot(target.path / "empty");
    EXPECT_EQ(empty_stat.uid, expected_uid);
    EXPECT_EQ(empty_stat.gid, expected_gid);
    EXPECT_EQ(empty_stat.mode, 0711U);
    EXPECT_EQ(empty_stat.mtime, empty_mtime);

    const auto empty_child_stat = read_stat_snapshot(target.path / "empty" / "child");
    EXPECT_EQ(empty_child_stat.uid, expected_uid);
    EXPECT_EQ(empty_child_stat.gid, expected_gid);
    EXPECT_EQ(empty_child_stat.mode, 0700U);
    EXPECT_EQ(empty_child_stat.mtime, empty_child_mtime);

    run_once(0U, 2U);
}

void test_non_root_receiver_can_restore_nfs_ownership_via_remote_credentials() {
    if (!hypersync::libnfs_support_enabled() || !sudo_available()) {
        return;
    }

    TempDir source("hypersync_non_root_nfs_source");
    TempDir target("hypersync_non_root_nfs_target_export");
    TempDir logs("hypersync_non_root_nfs_logs");

    write_file(source.path / "alpha.txt", "alpha");
    fs::create_directories(source.path / "empty" / "child");

    const std::uint32_t expected_uid = static_cast<std::uint32_t>(::geteuid());
    const std::uint32_t expected_gid = static_cast<std::uint32_t>(::getegid());
    const std::uint64_t alpha_mtime = 1'700'005'000ULL * 1'000'000'000ULL + 111'111'000ULL;
    const std::uint64_t empty_child_mtime = 1'700'005'100ULL * 1'000'000'000ULL + 222'222'000ULL;

    set_path_metadata(source.path / "alpha.txt", 0640, alpha_mtime);
    set_path_metadata(source.path / "empty" / "child", 0700, empty_child_mtime);

    ScopedNfsExport exported_target(target.path);
    const std::string target_url = exported_target.url() + "?uid=0&gid=0";

    std::uint16_t priority_port = pick_unused_port();
    std::uint16_t data_port = pick_unused_port();
    while (data_port == priority_port) {
        data_port = pick_unused_port();
    }

    EngineConfig config;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 8;

    auto receiver = launch_receiver_process(target_url, priority_port, data_port, logs.path, false);
    TransferEngine sender_engine(config);
    hypersync::SenderRuntimeConfig sender_runtime;
    sender_runtime.source_root = source.path;
    sender_runtime.remote_host = "127.0.0.1";
    sender_runtime.priority_port = priority_port;
    sender_runtime.data_port = data_port;
    sender_runtime.recursive = true;

    const auto report = sender_engine.transfer_directory(sender_runtime);
    expect_child_success(receiver, "non-root nfs receiver");

    EXPECT_EQ(report.files_total, 1U);
    EXPECT_EQ(report.files_failed, 0U);
    EXPECT_EQ(report.files_transferred, 1U);
    EXPECT_EQ(report.files_skipped, 0U);

    EXPECT_EQ(hypersync::read_file_contents(target.path / "alpha.txt"), "alpha");
    EXPECT_TRUE(fs::is_directory(target.path / "empty" / "child"));

    const auto alpha_stat = read_stat_snapshot(target.path / "alpha.txt");
    EXPECT_EQ(alpha_stat.uid, expected_uid);
    EXPECT_EQ(alpha_stat.gid, expected_gid);
    EXPECT_EQ(alpha_stat.mode, 0640U);
    EXPECT_EQ(alpha_stat.mtime, alpha_mtime);

    const auto empty_child_stat = read_stat_snapshot(target.path / "empty" / "child");
    EXPECT_EQ(empty_child_stat.uid, expected_uid);
    EXPECT_EQ(empty_child_stat.gid, expected_gid);
    EXPECT_EQ(empty_child_stat.mode, 0700U);
    EXPECT_EQ(empty_child_stat.mtime, empty_child_mtime);
}

void test_main_cli_scan_and_dry_run_smoke() {
    TempDir source("hypersync_cli_source");
    TempDir output("hypersync_cli_output");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "012345");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path scan_csv = output.path / "scan.csv";
    const fs::path diff_csv = output.path / "diff.csv";

    EXPECT_TRUE(command_succeeds(app + " scan --source " + source.path.string() + " --output " + scan_csv.string() +
                                 " >/dev/null 2>&1"));
    EXPECT_TRUE(fs::exists(scan_csv));

    const fs::path metadata_scan_csv = output.path / "metadata_scan.csv";
    const fs::path metadata_scan_stdout = output.path / "metadata_scan_stdout.txt";
    EXPECT_TRUE(command_succeeds(app + " scan --source " + source.path.string() +
                                 " --output " + metadata_scan_csv.string() +
                                 " --output-format csv --records all --meta-reader-threads 2" +
                                 " --metadata-async-depth 2 --record-buffer-slots 100" +
                                 " --max-duration-seconds 10 --stats-interval-seconds 1 > " +
                                 metadata_scan_stdout.string() + " 2>&1"));
    const std::string metadata_scan = hypersync::read_file_contents(metadata_scan_csv);
    const std::string metadata_scan_output = hypersync::read_file_contents(metadata_scan_stdout);
    EXPECT_TRUE(metadata_scan.find("scan_run_id,run_started_at_utc") != std::string::npos);
    EXPECT_TRUE(metadata_scan.find("file,alpha.txt,5,") != std::string::npos);
    EXPECT_TRUE(metadata_scan_output.find("scan_metadata_records files_found=2") != std::string::npos);
    EXPECT_TRUE(metadata_scan_output.find("record_buffer_slots=100") != std::string::npos);

    EXPECT_TRUE(command_succeeds(app + " dry-run --source " + source.path.string() + " --source-scan " + scan_csv.string() +
                                 " --target-scan " + scan_csv.string() + " --output " + diff_csv.string() +
                                 " >/dev/null 2>&1"));
    EXPECT_TRUE(fs::exists(diff_csv));
    EXPECT_TRUE(hypersync::read_file_contents(diff_csv).find("skip") != std::string::npos);

    ScanIndex source_diff_scan;
    source_diff_scan.add(hypersync::make_snapshot(FileSpec{"same.txt", "same", 10}, hypersync::hash64("same"), 'S'));
    source_diff_scan.add(hypersync::make_snapshot(FileSpec{"changed.txt", "fresh", 20}, hypersync::hash64("fresh"), 'S'));
    source_diff_scan.add(hypersync::make_snapshot(FileSpec{"new.txt", "new", 30}, hypersync::hash64("new"), 'S'));
    ScanIndex target_diff_scan;
    target_diff_scan.add(hypersync::make_snapshot(FileSpec{"same.txt", "same", 10}, hypersync::hash64("same"), 'T'));
    target_diff_scan.add(hypersync::make_snapshot(FileSpec{"changed.txt", "stale", 20}, hypersync::hash64("stale"), 'T'));
    target_diff_scan.add(hypersync::make_snapshot(FileSpec{"extra.txt", "extra", 40}, hypersync::hash64("extra"), 'T'));
    const fs::path source_diff_scan_path = output.path / "source_diff_scan.csv";
    const fs::path target_diff_scan_path = output.path / "target_diff_scan.csv";
    const fs::path first_class_diff_csv = output.path / "first_class_diff.csv";
    hypersync::TransferEngine::write_scan_csv(source_diff_scan, source_diff_scan_path);
    hypersync::TransferEngine::write_scan_csv(target_diff_scan, target_diff_scan_path);
    EXPECT_TRUE(command_succeeds(app + " diff --source-scan " + source_diff_scan_path.string() +
                                 " --target-scan " + target_diff_scan_path.string() +
                                 " --compare content --output " + first_class_diff_csv.string() +
                                 " >/dev/null 2>&1"));
    const std::string first_class_diff = hypersync::read_file_contents(first_class_diff_csv);
    EXPECT_TRUE(first_class_diff.find("same.txt,skip") != std::string::npos);
    EXPECT_TRUE(first_class_diff.find("changed.txt,changed") != std::string::npos);
    EXPECT_TRUE(first_class_diff.find("new.txt,new") != std::string::npos);
    EXPECT_TRUE(first_class_diff.find("extra.txt,target_only") != std::string::npos);

    const fs::path live_diff_csv = output.path / "live_diff.csv";
    EXPECT_TRUE(command_succeeds(app + " diff --source " + source.path.string() +
                                 " --target " + source.path.string() +
                                 " --compare size --meta-reader-threads 2 --metadata-async-depth 2" +
                                 " --max-duration-seconds 10 --output " + live_diff_csv.string() +
                                 " >/dev/null 2>&1"));
    const std::string live_diff = hypersync::read_file_contents(live_diff_csv);
    EXPECT_TRUE(live_diff.find("alpha.txt,skip") != std::string::npos);
    EXPECT_TRUE(live_diff.find("nested/beta.bin,skip") != std::string::npos);

    EXPECT_FALSE(command_succeeds(app + " receive >/dev/null 2>&1"));
}

void test_main_cli_benchmark_meta_smoke() {
    TempDir source("hypersync_cli_benchmark_meta_source");
    TempDir output("hypersync_cli_benchmark_meta_output");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "012345");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "benchmark.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-meta --source " + source.path.string() +
                                 " --discard-after-checker > " + stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("files_seen=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("checker_emitted=0") != std::string::npos);
    EXPECT_TRUE(output_text.find("checker_discarded=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("records_per_second=") != std::string::npos);

    const fs::path stats_stdout_path = output.path / "benchmark_stats.txt";
    EXPECT_TRUE(command_succeeds(app + " benchmark-meta --source " + source.path.string() +
                                 " --metadata-stats-discarder --metadata-async-depth 4 > " +
                                 stats_stdout_path.string() + " 2>&1"));
    const std::string stats_output_text = hypersync::read_file_contents(stats_stdout_path);
    EXPECT_TRUE(stats_output_text.find("checker_discarded=2") != std::string::npos);
    EXPECT_TRUE(stats_output_text.find("folders_found=2") != std::string::npos);
    EXPECT_TRUE(stats_output_text.find("logical_size_bytes=11") != std::string::npos);
    EXPECT_TRUE(stats_output_text.find("metadata_async_depth=4") != std::string::npos);

    const fs::path metadata_csv_path = output.path / "metadata.csv";
    EXPECT_TRUE(command_succeeds(app + " benchmark-meta --source " + source.path.string() +
                                 " --metadata-output " + metadata_csv_path.string() +
                                 " --metadata-output-format csv --metadata-records all --max-duration-seconds 10 > " +
                                 (output.path / "metadata_stdout.txt").string() + " 2>&1"));
    const std::string metadata_csv = hypersync::read_file_contents(metadata_csv_path);
    EXPECT_TRUE(metadata_csv.find("file,alpha.txt,5,") != std::string::npos);
    EXPECT_TRUE(metadata_csv.find("file,nested/beta.bin,6,") != std::string::npos);
    EXPECT_TRUE(metadata_csv.find("folder,nested,,") != std::string::npos);
    EXPECT_TRUE(metadata_csv.find(",1,6") != std::string::npos);

    const fs::path partitioned_metadata_dir = output.path / "partitioned_metadata";
    EXPECT_TRUE(command_succeeds(app + " benchmark-meta --source " + source.path.string() +
                                 " --metadata-output " + partitioned_metadata_dir.string() +
                                 " --metadata-output-format csv --metadata-records all" +
                                 " --metadata-output-partitions 2 --metadata-output-partition-mode processes" +
                                 " --max-duration-seconds 10 > " +
                                 (output.path / "metadata_partitioned_stdout.txt").string() + " 2>&1"));
    EXPECT_TRUE(fs::exists(partitioned_metadata_dir / "part-00000.csv"));
    EXPECT_TRUE(fs::exists(partitioned_metadata_dir / "part-00001.csv"));
    const std::string part0 = hypersync::read_file_contents(partitioned_metadata_dir / "part-00000.csv");
    const std::string part1 = hypersync::read_file_contents(partitioned_metadata_dir / "part-00001.csv");
    const std::string combined_partitions = part0 + part1;
    EXPECT_TRUE(combined_partitions.find("file,alpha.txt,5,") != std::string::npos);
    EXPECT_TRUE(combined_partitions.find("file,nested/beta.bin,6,") != std::string::npos);
    EXPECT_TRUE(combined_partitions.find("folder,nested,,") != std::string::npos);
}

void test_main_cli_benchmark_data_smoke() {
    TempDir source("hypersync_cli_benchmark_data_source");
    TempDir output("hypersync_cli_benchmark_data_output");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "012345");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "benchmark_data.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-data --source " + source.path.string() +
                                 " --meta-reader-threads 1 --metadata-async-depth 4" +
                                 " --data-reader-threads 2 --data-outstanding-requests 4" +
                                 " --max-files-queued 2 --data-buffer-slots 32" +
                                 " --data-queue-depth 8 --data-copy-mode no-copy" +
                                 " --max-duration-seconds 10 > " +
                                 stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("data_benchmark files_found=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_read=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("bytes_read=11") != std::string::npos);
    EXPECT_TRUE(output_text.find("bytes_per_second=") != std::string::npos);
    EXPECT_TRUE(output_text.find("gigabits_per_second=") != std::string::npos);
    EXPECT_TRUE(output_text.find("metadata_async_depth=4") != std::string::npos);
    EXPECT_TRUE(output_text.find("data_reader_threads=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("data_outstanding_requests=4") != std::string::npos);
    EXPECT_TRUE(output_text.find("max_files_queued=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("data_buffer_slots=32") != std::string::npos);
    EXPECT_TRUE(output_text.find("data_queue_depth=8") != std::string::npos);
    EXPECT_TRUE(output_text.find("data_copy_mode=no-copy") != std::string::npos);
}

void test_main_cli_benchmark_data_write_folder_ready_discard_smoke() {
    TempDir source("hypersync_cli_folder_ready_source");
    TempDir target("hypersync_cli_folder_ready_target");
    TempDir output("hypersync_cli_folder_ready_output");

    write_file(source.path / "a" / "one.txt", "one");
    write_file(source.path / "a" / "two.txt", "two2");
    write_file(source.path / "b" / "three.txt", "33333");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "folder_ready.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-data-write --mode folder-ready-discard --source " +
                                 source.path.string() + " --target " + target.path.string() +
                                 " --meta-reader-threads 2 --metadata-async-depth 4" +
                                 " --data-writer-threads 2 --data-reader-threads 2" +
                                 " --max-files-queued 16 --max-duration-seconds 10 > " +
                                 stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("data_write_benchmark mode=folder-ready-discard") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_found=3") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_read=3") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_written=3") != std::string::npos);
    EXPECT_TRUE(output_text.find("folders_written=") != std::string::npos);
    EXPECT_TRUE(fs::is_directory(target.path / "a"));
    EXPECT_TRUE(fs::is_directory(target.path / "b"));
    EXPECT_TRUE(!fs::exists(target.path / "a" / "one.txt"));
}

void test_main_cli_benchmark_data_write_folder_ready_write_smoke() {
    TempDir source("hypersync_cli_folder_ready_write_source");
    TempDir target("hypersync_cli_folder_ready_write_target");
    TempDir output("hypersync_cli_folder_ready_write_output");

    write_file(source.path / "a" / "one.txt", "one");
    write_file(source.path / "b" / "two.txt", "two2");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "folder_ready_write.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-data-write --mode folder-ready-write --source " +
                                 source.path.string() + " --target " + target.path.string() +
                                 " --meta-reader-threads 2 --metadata-async-depth 4" +
                                 " --data-writer-threads 2 --data-reader-threads 2" +
                                 " --data-outstanding-requests 4 --data-queue-depth 8" +
                                 " --max-files-queued 16 --max-duration-seconds 10 > " +
                                 stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("data_write_benchmark mode=folder-ready-write") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_found=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_read=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_written=2") != std::string::npos);
    EXPECT_TRUE(fs::is_directory(target.path / "a"));
    EXPECT_TRUE(fs::is_directory(target.path / "b"));
    EXPECT_EQ(hypersync::read_file_contents(target.path / "a" / "one.txt"), std::string("one"));
    EXPECT_EQ(hypersync::read_file_contents(target.path / "b" / "two.txt"), std::string("two2"));
}

void test_main_cli_benchmark_data_hash_smoke() {
    TempDir source("hypersync_cli_benchmark_data_hash_source");
    TempDir output("hypersync_cli_benchmark_data_hash_output");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "012345");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "benchmark_data_hash.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-data-hash --source " + source.path.string() +
                                 " --hash xxh3_64 --meta-reader-threads 1 --metadata-async-depth 2" +
                                 " --data-reader-threads 2 --data-outstanding-requests 2" +
                                 " --hash-threads 2 --hash-work-factor 2 --max-files-queued 2" +
                                 " --data-buffer-slots 8 --data-queue-depth 4" +
                                 " --max-duration-seconds 10 > " +
                                 stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("data_hash_benchmark files_found=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_read=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("bytes_read=11") != std::string::npos);
    EXPECT_TRUE(output_text.find("bytes_hashed=11") != std::string::npos);
    EXPECT_TRUE(output_text.find("hash_algorithm=xxh3_64") != std::string::npos);
    EXPECT_TRUE(output_text.find("hash_worker_threads=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("hash_work_factor=2") != std::string::npos);
}

void test_main_cli_benchmark_synthetic_profile_smoke() {
    TempDir output("hypersync_cli_benchmark_synthetic_profile_output");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "synthetic_profile_stdout.txt";
    const fs::path profile_path = output.path / "synthetic_profile.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-synthetic-profile --file-count 2000000" +
                                 " --block-file-count 100000 --output " +
                                 profile_path.string() + " > " + stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("synthetic_profile_benchmark files_observed=2000000") !=
                std::string::npos);
    EXPECT_TRUE(output_text.find("phases=3") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_per_second=") != std::string::npos);
    EXPECT_TRUE(output_text.find("phase index=0") != std::string::npos);
    EXPECT_TRUE(output_text.find("phase index=1") != std::string::npos);
    EXPECT_TRUE(output_text.find("phase index=2") != std::string::npos);

    const std::string profile_text = hypersync::read_file_contents(profile_path);
    EXPECT_TRUE(profile_text.find("logical_size_bytes=") != std::string::npos);
    EXPECT_TRUE(profile_text.find("small_ratio=0.960") != std::string::npos);
    EXPECT_TRUE(profile_text.find("small_ratio=0.650") != std::string::npos);
    EXPECT_TRUE(profile_text.find("small_ratio=0.199") != std::string::npos ||
                profile_text.find("small_ratio=0.200") != std::string::npos);
}

void test_main_cli_benchmark_synthetic_replay_smoke() {
    TempDir output("hypersync_cli_benchmark_synthetic_replay_output");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path profile_path = output.path / "synthetic_profile.txt";
    const fs::path replay_stdout_path = output.path / "synthetic_replay_stdout.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-synthetic-profile --file-count 10000" +
                                 " --block-file-count 1000 --output " +
                                 profile_path.string() + " > /dev/null 2>&1"));
    EXPECT_TRUE(command_succeeds(app + " benchmark-synthetic-replay --profile " +
                                 profile_path.string() +
                                 " --max-files 5000 --with-payload > " +
                                 replay_stdout_path.string() + " 2>&1"));
    const std::string replay_output = hypersync::read_file_contents(replay_stdout_path);
    EXPECT_TRUE(replay_output.find("synthetic_replay_benchmark") != std::string::npos);
    EXPECT_TRUE(replay_output.find("files=5000") != std::string::npos);
    EXPECT_TRUE(replay_output.find("payload_views=5000") != std::string::npos);
    EXPECT_TRUE(replay_output.find("files_per_second=") != std::string::npos);
}

void test_main_cli_benchmark_nfs_profile_smoke() {
    TempDir source("hypersync_cli_benchmark_nfs_profile_source");
    TempDir output("hypersync_cli_benchmark_nfs_profile_output");

    fs::create_directories(source.path / "small");
    fs::create_directories(source.path / "large");
    for (int index = 0; index < 20; ++index) {
        write_file(source.path / "small" / ("s" + std::to_string(index) + ".txt"), "x");
        write_file(source.path / "large" / ("l" + std::to_string(index) + ".bin"),
                   std::string(200U * 1024U, 'L'));
    }

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "nfs_profile_stdout.txt";
    const fs::path profile_path = output.path / "nfs_profile.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-nfs-profile --source " +
                                 source.path.string() +
                                 " --max-records 30 --phase-count 10" +
                                 " --meta-reader-threads 2 --metadata-async-depth 2" +
                                 " --profile-data-reads --data-sample-rate 1" +
                                 " --data-sample-max-files-per-phase 4" +
                                 " --data-sample-large-read-bytes 1048576" +
                                 " --output " + profile_path.string() +
                                 " > " + stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("synthetic_profile_benchmark files_observed=30") !=
                std::string::npos);
    EXPECT_TRUE(output_text.find("phases=10") != std::string::npos);
    EXPECT_TRUE(output_text.find("nfs_profile source=") != std::string::npos);
    EXPECT_TRUE(output_text.find("failed_folders=0") != std::string::npos);
    EXPECT_TRUE(output_text.find("profile_data_reads=true") != std::string::npos);

    const std::string profile_text = hypersync::read_file_contents(profile_path);
    EXPECT_TRUE(profile_text.find("phase index=0") != std::string::npos);
    EXPECT_TRUE(profile_text.find("phase index=9") != std::string::npos);
    EXPECT_TRUE(profile_text.find("small_files=") != std::string::npos);
    EXPECT_TRUE(profile_text.find("large_files=") != std::string::npos);
    EXPECT_TRUE(profile_text.find("<=1048576:") != std::string::npos);
    EXPECT_TRUE(profile_text.find("sampled_small_read_files=") != std::string::npos);
    EXPECT_TRUE(profile_text.find("sampled_large_read_files=") != std::string::npos);
    EXPECT_TRUE(profile_text.find("sampled_small_read_latency_buckets_us=") != std::string::npos);
}

void test_main_cli_benchmark_hash_smoke() {
    TempDir output("hypersync_cli_benchmark_hash_output");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "benchmark_hash.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-hash --hash xxh64 --threads 2" +
                                 " --block-size 4096 --duration-seconds 0.05" +
                                 " --min-gigabits-per-core 0.000001 > " +
                                 stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("hash_speed hash_algorithm=xxh64") != std::string::npos);
    EXPECT_TRUE(output_text.find("worker_threads=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("block_size=4096") != std::string::npos);
    EXPECT_TRUE(output_text.find("gigabits_per_core=") != std::string::npos);
    EXPECT_TRUE(output_text.find("min_gigabits_per_core=1e-06") != std::string::npos ||
                output_text.find("min_gigabits_per_core=0.000001") != std::string::npos);
    EXPECT_TRUE(output_text.find("passed=true") != std::string::npos);

    const fs::path xxh3_stdout_path = output.path / "benchmark_hash_xxh3.txt";
    EXPECT_TRUE(command_succeeds(app + " benchmark-hash --hash xxh3_128 --threads 1" +
                                 " --block-size 4096 --duration-seconds 0.05" +
                                 " --min-gigabits-per-core 0.000001 > " +
                                 xxh3_stdout_path.string() + " 2>&1"));
    const std::string xxh3_output_text = hypersync::read_file_contents(xxh3_stdout_path);
    EXPECT_TRUE(xxh3_output_text.find("hash_speed hash_algorithm=xxh3_128") != std::string::npos);
    EXPECT_TRUE(xxh3_output_text.find("passed=true") != std::string::npos);

    const fs::path failed_stdout_path = output.path / "benchmark_hash_failed.txt";
    EXPECT_FALSE(command_succeeds(app + " benchmark-hash --hash xxh64 --threads 1" +
                                  " --block-size 4096 --duration-seconds 0.01" +
                                  " --min-gigabits-per-core 1000000 > " +
                                  failed_stdout_path.string() + " 2>&1"));
    const std::string failed_output_text = hypersync::read_file_contents(failed_stdout_path);
    EXPECT_TRUE(failed_output_text.find("passed=false") != std::string::npos);
}

void test_main_cli_benchmark_metadata_writer_smoke() {
    TempDir output("hypersync_cli_benchmark_metadata_writer_output");
    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path csv_path = output.path / "generated.csv";
    const fs::path stdout_path = output.path / "writer_benchmark.txt";

    EXPECT_TRUE(command_succeeds(app + " benchmark-metadata-writer --output " + csv_path.string() +
                                 " --output-format csv --file-count 20 --folder-count 4" +
                                 " --batch-size 7 --average-file-size 4096 > " +
                                 stdout_path.string() + " 2>&1"));

    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("metadata_writer_benchmark output_format=csv") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_generated=20") != std::string::npos);
    EXPECT_TRUE(output_text.find("folders_generated=4") != std::string::npos);
    EXPECT_TRUE(output_text.find("records_written=24") != std::string::npos);
    EXPECT_TRUE(output_text.find("records_per_second=") != std::string::npos);

    const std::string csv = hypersync::read_file_contents(csv_path);
    EXPECT_TRUE(csv.find("file,generated/dir_00000000/file_000000000000.dat") != std::string::npos);
    EXPECT_TRUE(csv.find("folder,generated/dir_00000000") != std::string::npos);

    const fs::path partitioned_dir = output.path / "partitioned";
    const fs::path partitioned_stdout_path = output.path / "writer_benchmark_partitioned.txt";
    EXPECT_TRUE(command_succeeds(app + " benchmark-metadata-writer --output " + partitioned_dir.string() +
                                 " --output-format csv --file-count 20 --folder-count 4" +
                                 " --batch-size 7 --average-file-size 4096 --partitions 2 > " +
                                 partitioned_stdout_path.string() + " 2>&1"));

    const std::string partitioned_output = hypersync::read_file_contents(partitioned_stdout_path);
    EXPECT_TRUE(partitioned_output.find("records_written=24") != std::string::npos);
    EXPECT_TRUE(partitioned_output.find("partitions=2") != std::string::npos);
    EXPECT_TRUE(fs::exists(partitioned_dir / "part-00000.csv"));
    EXPECT_TRUE(fs::exists(partitioned_dir / "part-00001.csv"));
}

void test_main_cli_hash_smoke() {
    TempDir source("hypersync_cli_hash_source");
    TempDir output("hypersync_cli_hash_output");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "012345");

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    const fs::path stdout_path = output.path / "hash_stdout.txt";
    const fs::path csv_path = output.path / "hash.csv";

    EXPECT_TRUE(command_succeeds(app + " hash --source " + source.path.string() +
                                 " --output " + csv_path.string() +
                                 " --output-format csv --records all --hash sha256" +
                                 " --meta-reader-threads 1 --metadata-async-depth 4" +
                                 " --data-reader-threads 2 --data-outstanding-requests 4" +
                                 " --hash-threads 3 --max-hash-chunks-queued 12" +
                                 " --max-files-queued 2 --max-duration-seconds 10 > " +
                                 stdout_path.string() + " 2>&1"));
    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("hash_inventory files_found=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_hashed=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("files_failed=0") != std::string::npos);
    EXPECT_TRUE(output_text.find("bytes_read=11") != std::string::npos);
    EXPECT_TRUE(output_text.find("bytes_hashed=11") != std::string::npos);
    EXPECT_TRUE(output_text.find("read_gigabits_per_second=") != std::string::npos);
    EXPECT_TRUE(output_text.find("read_elapsed_s=") != std::string::npos);
    EXPECT_TRUE(output_text.find("hash_algorithm=sha256") != std::string::npos);
    EXPECT_TRUE(output_text.find("metadata_files_written=2") != std::string::npos);
    EXPECT_TRUE(output_text.find("hash_threads=3") != std::string::npos);
    EXPECT_TRUE(output_text.find("max_hash_chunks_queued=12") != std::string::npos);

    const std::string csv = hypersync::read_file_contents(csv_path);
    EXPECT_TRUE(csv.find("file,alpha.txt,5,") != std::string::npos);
    EXPECT_TRUE(csv.find(std::string(",sha256,") +
                         hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::sha256, "alpha")) !=
                std::string::npos);
    EXPECT_TRUE(csv.find(std::string(",sha256,") +
                         hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::sha256, "012345")) !=
                std::string::npos);
    EXPECT_TRUE(csv.find("folder,nested,,") != std::string::npos);
    EXPECT_TRUE(csv.find(",1,6,,") != std::string::npos);

    const fs::path block_stdout_path = output.path / "hash_blocks_stdout.txt";
    const fs::path block_csv_path = output.path / "hash_blocks.csv";
    EXPECT_TRUE(command_succeeds(app + " hash --source " + source.path.string() +
                                 " --output " + block_csv_path.string() +
                                 " --output-format csv --records files --hash md5 --hash-mode blocks" +
                                 " --hash-block-size 4" +
                                 " --meta-reader-threads 1 --metadata-async-depth 4" +
                                 " --data-reader-threads 2 --data-outstanding-requests 4" +
                                 " --hash-threads 4 --max-hash-chunks-queued 16" +
                                 " --max-files-queued 2 --max-duration-seconds 10 > " +
                                 block_stdout_path.string() + " 2>&1"));
    const std::string block_output_text = hypersync::read_file_contents(block_stdout_path);
    EXPECT_TRUE(block_output_text.find("hash_mode=blocks") != std::string::npos);
    EXPECT_TRUE(block_output_text.find("hash_algorithm=md5") != std::string::npos);
    EXPECT_TRUE(block_output_text.find("hash_block_size=4") != std::string::npos);

    const std::string block_csv = hypersync::read_file_contents(block_csv_path);
    const std::string alpha_blocks =
        hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::md5, "alph") + ";" +
        hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::md5, "a");
    const std::string beta_blocks =
        hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::md5, "0123") + ";" +
        hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::md5, "45");
    EXPECT_TRUE(block_csv.find("file,alpha.txt,5,") != std::string::npos);
    EXPECT_TRUE(block_csv.find(",4,2,md5," + alpha_blocks) != std::string::npos);
    EXPECT_TRUE(block_csv.find("file,nested/beta.bin,6,") != std::string::npos);
    EXPECT_TRUE(block_csv.find(",4,2,md5," + beta_blocks) != std::string::npos);

    const fs::path xxh3_stdout_path = output.path / "hash_xxh3_stdout.txt";
    const fs::path xxh3_csv_path = output.path / "hash_xxh3.csv";
    EXPECT_TRUE(command_succeeds(app + " hash --source " + source.path.string() +
                                 " --output " + xxh3_csv_path.string() +
                                 " --output-format csv --records files --hash xxh3_64" +
                                 " --meta-reader-threads 1 --metadata-async-depth 4" +
                                 " --data-reader-threads 1 --data-outstanding-requests 4" +
                                 " --hash-threads 1 --max-hash-chunks-queued 2" +
                                 " --max-files-queued 2 --max-duration-seconds 10 > " +
                                 xxh3_stdout_path.string() + " 2>&1"));
    const std::string xxh3_output_text = hypersync::read_file_contents(xxh3_stdout_path);
    EXPECT_TRUE(xxh3_output_text.find("hash_algorithm=xxh3_64") != std::string::npos);
    const std::string xxh3_csv = hypersync::read_file_contents(xxh3_csv_path);
    EXPECT_TRUE(xxh3_csv.find(std::string(",xxh3_64,") +
                              hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::xxh3_64, "alpha")) !=
                std::string::npos);
    EXPECT_TRUE(xxh3_csv.find(std::string(",xxh3_64,") +
                              hypersync::content_hash_hex(hypersync::ContentHashAlgorithm::xxh3_64, "012345")) !=
                std::string::npos);
}

void test_main_cli_status_command_smoke() {
    TempDir output("hypersync_cli_status_output");
    const fs::path socket_path = fs::path("/tmp") /
                                 ("wsync_status_" +
                                  std::to_string(static_cast<long long>(
                                      std::chrono::steady_clock::now().time_since_epoch().count())) +
                                  ".sock");
    const fs::path stdout_path = output.path / "status.txt";

    hypersync::StatusRegistry registry;
    registry.register_job("cli_status_job", []() {
        hypersync::MonitorJobSnapshot snapshot;
        snapshot.name = "cli_status_job";
        snapshot.running = true;
        snapshot.worker_count = 3;
        snapshot.processed_count = 7;
        snapshot.byte_count = 1024U * 1024U;
        snapshot.count_unit = "records";
        return snapshot;
    });
    registry.register_queue("cli_status_queue", []() {
        hypersync::MonitorQueueSnapshot snapshot;
        snapshot.name = "cli_status_queue";
        snapshot.capacity = 10;
        snapshot.depth = 5;
        snapshot.high_watermark = 8;
        snapshot.pushed = 11;
        snapshot.popped = 6;
        return snapshot;
    });

    hypersync::StatusServer server(socket_path, registry);
    server.start();

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    EXPECT_TRUE(command_succeeds(app + " status --socket " + socket_path.string() +
                                 " > " + stdout_path.string() + " 2>&1"));
    server.stop();
    std::error_code ignored;
    fs::remove(socket_path, ignored);

    const std::string output_text = hypersync::read_file_contents(stdout_path);
    EXPECT_TRUE(output_text.find("cli_status_job") != std::string::npos);
    EXPECT_TRUE(output_text.find("cli_status_queue") != std::string::npos);
    EXPECT_TRUE(output_text.find("full=50.0%") != std::string::npos);
}

void test_main_cli_version_smoke() {
    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    TempDir output("hypersync_cli_version_output");
    const fs::path stdout_path = output.path / "version.txt";

    EXPECT_TRUE(command_succeeds(app + " --version > " + stdout_path.string() + " 2>&1"));
    EXPECT_EQ(hypersync::read_file_contents(stdout_path), "hypersync 0.0.4.10\n");
}

void test_main_cli_send_and_receive_smoke() {
    TempDir source("hypersync_cli_send_source");
    TempDir target("hypersync_cli_send_target");
    TempDir logs("hypersync_cli_send_logs");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "01234567");

    std::uint16_t priority_port = pick_unused_port();
    std::uint16_t data_port = pick_unused_port();
    while (data_port == priority_port) {
        data_port = pick_unused_port();
    }

    auto receiver = launch_receiver_process(target.path.string(), priority_port, data_port, logs.path, false);
    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    EXPECT_TRUE(command_succeeds(app + " send --source " + source.path.string() +
                                 " --host 127.0.0.1 --priority-port " + std::to_string(priority_port) +
                                 " --data-port " + std::to_string(data_port) + " >/dev/null 2>&1"));
    expect_child_success(receiver, "cli receiver");

    EXPECT_EQ(hypersync::read_file_contents(target.path / "alpha.txt"), "alpha");
    EXPECT_EQ(hypersync::read_file_contents(target.path / "nested" / "beta.bin"), "01234567");
}

void test_main_cli_uses_yaml_runtime_defaults() {
    TempDir source("hypersync_cli_cfg_source");
    TempDir target("hypersync_cli_cfg_target");
    TempDir logs("hypersync_cli_cfg_logs");
    TempDir config_root("hypersync_cli_cfg");

    write_file(source.path / "alpha.txt", "alpha");
    write_file(source.path / "nested" / "beta.bin", "01234567");

    std::uint16_t priority_port = pick_unused_port();
    std::uint16_t data_port = pick_unused_port();
    while (data_port == priority_port) {
        data_port = pick_unused_port();
    }

    const fs::path config_path = config_root.path / "runtime.yaml";
    {
        std::ofstream output(config_path);
        output << "runtime:\n";
        output << "  sender:\n";
        output << "    remote_host: 127.0.0.1\n";
        output << "    priority_port: " << priority_port << "\n";
        output << "    data_port: " << data_port << "\n";
        output << "  receiver:\n";
        output << "    bind_host: 127.0.0.1\n";
        output << "    priority_port: " << priority_port << "\n";
        output << "    data_port: " << data_port << "\n";
    }

    const std::string app = (fs::current_path() / "build" / "hypersync").string();
    std::vector<std::string> receiver_command{
        app,
        "--config",
        config_path.string(),
        "receive",
        "--target",
        target.path.string(),
    };
    auto receiver = ScopedChildProcess(std::move(receiver_command), logs.path);

    EXPECT_TRUE(command_succeeds(app + " --config " + config_path.string() + " send --source " + source.path.string() +
                                 " >/dev/null 2>&1"));
    expect_child_success(receiver, "cli receiver with config");

    EXPECT_EQ(hypersync::read_file_contents(target.path / "alpha.txt"), "alpha");
    EXPECT_EQ(hypersync::read_file_contents(target.path / "nested" / "beta.bin"), "01234567");
}

void test_transfer_mode_transfers_small_and_large_files() {
    EngineConfig config;
    config.mode = Mode::transfer;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 8;
    config.small_pool_slots = 1;
    config.large_pool_slots = 1;

    const std::vector<FileSpec> files{
        {"folder/small.txt", "abcd", 10},
        {"folder/large.bin", "abcdefghijklmnopq", 20},
    };

    TransferEngine engine(config);
    const auto report = engine.run(files);

    EXPECT_EQ(report.files_total, 2U);
    EXPECT_EQ(report.files_transferred, 2U);
    EXPECT_EQ(report.files_skipped, 0U);
    EXPECT_EQ(report.files_failed, 0U);
    EXPECT_EQ(report.bytes_planned, 21ULL);
    EXPECT_EQ(report.bytes_transferred, 21ULL);
    EXPECT_EQ(report.chunks_sent, 4U);
    EXPECT_EQ(report.source_scan_rows.size(), 2U);
    EXPECT_EQ(report.target_scan_rows.size(), 2U);
    EXPECT_EQ(report.files.at("folder/small.txt").chunk_count, 1U);
    EXPECT_EQ(report.files.at("folder/large.bin").chunk_count, 3U);
    EXPECT_EQ(report.files.at("folder/large.bin").diff, DiffKind::new_file);
    EXPECT_EQ(report.folders.at("folder").state, FolderState::done);
    EXPECT_EQ(report.folders.at("folder").remote_state, FolderState::done);
}

void test_transfer_mode_skips_via_scan_indexes() {
    const FileSpec file{"folder/existing.txt", "same", 50};
    ScanIndex source_scan;
    ScanIndex target_scan;
    const auto source_row = hypersync::make_snapshot(file, hypersync::hash64(file.content), 'S');
    const auto target_row = hypersync::make_snapshot(file, hypersync::hash64(file.content), 'T');
    source_scan.add(source_row);
    target_scan.add(target_row);

    EngineConfig config;
    config.mode = Mode::transfer;

    const TransferEngine engine(config);
    const auto report = engine.run({file}, &source_scan, &target_scan);

    EXPECT_EQ(report.files_skipped, 1U);
    EXPECT_EQ(report.files_transferred, 0U);
    EXPECT_EQ(report.bytes_planned, 0ULL);
    EXPECT_EQ(report.chunks_sent, 0U);
    EXPECT_EQ(report.files.at("folder/existing.txt").diff, DiffKind::skip);
    EXPECT_EQ(report.files.at("folder/existing.txt").data_hash, source_row.data_hash);
    EXPECT_EQ(report.source_scan_rows.size(), 1U);
    EXPECT_EQ(report.target_scan_rows.size(), 1U);
}

void test_transfer_mode_can_skip_per_file_without_folder_fast_skip() {
    const FileSpec same{"folder/same.txt", "same", 10};
    const FileSpec changed{"folder/changed.txt", "fresh", 11};
    const FileSpec stale_changed{"folder/changed.txt", "stale", 9};

    ScanIndex source_scan;
    ScanIndex target_scan;
    source_scan.add(hypersync::make_snapshot(same, hypersync::hash64(same.content), 'S'));
    source_scan.add(hypersync::make_snapshot(changed, hypersync::hash64(changed.content), 'S'));
    target_scan.add(hypersync::make_snapshot(same, hypersync::hash64(same.content), 'T'));
    target_scan.add(hypersync::make_snapshot(stale_changed, hypersync::hash64(stale_changed.content), 'T'));

    EngineConfig config;
    config.mode = Mode::transfer;

    const TransferEngine engine(config);
    const auto report = engine.run({same, changed}, &source_scan, &target_scan);

    EXPECT_EQ(report.files_skipped, 1U);
    EXPECT_EQ(report.files_transferred, 1U);
    EXPECT_EQ(report.files.at("folder/same.txt").diff, DiffKind::skip);
    EXPECT_EQ(report.files.at("folder/changed.txt").diff, DiffKind::changed);
}

void test_transfer_mode_retries_then_succeeds() {
    EngineConfig config;
    config.mode = Mode::transfer;
    config.small_file_threshold = 4;
    config.large_chunk_bytes = 8;
    config.forced_failures["retry/me.bin"] = 1;
    config.max_retries = 2;

    const TransferEngine engine(config);
    const auto report = engine.run({{"retry/me.bin", "abcdefghij", 77}});

    EXPECT_EQ(report.retries, 1U);
    EXPECT_EQ(report.files_transferred, 1U);
    EXPECT_EQ(report.files_failed, 0U);
    EXPECT_EQ(report.files.at("retry/me.bin").attempts, 2U);
    EXPECT_EQ(report.files.at("retry/me.bin").sender_state, FileState::done);
    EXPECT_EQ(report.files.at("retry/me.bin").receiver_state, FileState::done);
}

void test_transfer_mode_can_exhaust_retries_and_fail() {
    EngineConfig config;
    config.mode = Mode::transfer;
    config.forced_failures["broken.bin"] = 4;
    config.max_retries = 2;

    const TransferEngine engine(config);
    const auto report = engine.run({{"broken.bin", "payload", 88}});

    EXPECT_EQ(report.files_failed, 1U);
    EXPECT_EQ(report.files_transferred, 0U);
    EXPECT_EQ(report.retries, 2U);
    EXPECT_EQ(report.files.at("broken.bin").diff, DiffKind::failed);
    EXPECT_EQ(report.files.at("broken.bin").sender_state, FileState::failed);
    EXPECT_EQ(report.files.at("broken.bin").receiver_state, FileState::failed);
    EXPECT_FALSE(report.files.at("broken.bin").hash_verified);
}

void test_transfer_mode_skip_verify_ignores_forced_failures() {
    EngineConfig config;
    config.mode = Mode::transfer;
    config.skip_verify = true;
    config.forced_failures["trusted.bin"] = 3;

    const TransferEngine engine(config);
    const auto report = engine.run({{"trusted.bin", "payload", 90}});

    EXPECT_EQ(report.files_transferred, 1U);
    EXPECT_EQ(report.retries, 0U);
    EXPECT_TRUE(report.files.at("trusted.bin").hash_verified);
}

void test_dry_run_uses_scans_and_metadata() {
    const FileSpec skipped{"dir/skip.txt", "same", 1};
    const FileSpec changed{"dir/changed.txt", "newer", 2};
    const FileSpec created{"dir/new.txt", "brand-new", 3};
    const FileSpec target_changed{"dir/changed.txt", "older", 1};

    ScanIndex target_scan;
    target_scan.add(hypersync::make_snapshot(skipped, hypersync::hash64(skipped.content), 'T'));
    target_scan.add(hypersync::make_snapshot(target_changed, hypersync::hash64(target_changed.content), 'T'));

    EngineConfig config;
    config.mode = Mode::dry_run;

    const TransferEngine engine(config);
    const auto report = engine.run({skipped, changed, created}, nullptr, &target_scan);

    EXPECT_EQ(report.files_skipped, 1U);
    EXPECT_EQ(report.files_transferred, 0U);
    EXPECT_EQ(report.bytes_planned, changed.content.size() + created.content.size());
    EXPECT_TRUE(report.diff_csv.find("dir/skip.txt,skip") != std::string::npos);
    EXPECT_TRUE(report.diff_csv.find("dir/changed.txt,changed") != std::string::npos);
    EXPECT_TRUE(report.diff_csv.find("dir/new.txt,new") != std::string::npos);
}

void test_diff_scan_indexes_reports_changes_and_target_only() {
    const FileSpec same{"dir/same.txt", "same", 10};
    const FileSpec changed_source{"dir/changed.txt", "fresh", 20};
    const FileSpec changed_target{"dir/changed.txt", "stale", 20};
    const FileSpec created{"dir/new.txt", "new", 30};
    const FileSpec extra{"dir/extra.txt", "extra", 40};

    ScanIndex source_scan;
    source_scan.add(hypersync::make_snapshot(same, hypersync::hash64(same.content), 'S'));
    source_scan.add(hypersync::make_snapshot(changed_source, hypersync::hash64(changed_source.content), 'S'));
    source_scan.add(hypersync::make_snapshot(created, hypersync::hash64(created.content), 'S'));

    ScanIndex target_scan;
    target_scan.add(hypersync::make_snapshot(same, hypersync::hash64(same.content), 'T'));
    target_scan.add(hypersync::make_snapshot(changed_target, hypersync::hash64(changed_target.content), 'T'));
    target_scan.add(hypersync::make_snapshot(extra, hypersync::hash64(extra.content), 'T'));

    EngineConfig config;
    config.mode = Mode::dry_run;
    const TransferEngine engine(config);
    const auto report = engine.diff_scan_indexes(source_scan, target_scan, "content");

    EXPECT_EQ(report.files_total, 4U);
    EXPECT_EQ(report.files_skipped, 1U);
    EXPECT_EQ(report.files.at("dir/same.txt").diff, DiffKind::skip);
    EXPECT_EQ(report.files.at("dir/changed.txt").diff, DiffKind::changed);
    EXPECT_EQ(report.files.at("dir/new.txt").diff, DiffKind::new_file);
    EXPECT_EQ(report.files.at("dir/extra.txt").diff, DiffKind::target_only);
    EXPECT_TRUE(report.diff_csv.find("dir/extra.txt,target_only,0,0,") != std::string::npos);
    EXPECT_TRUE(report.diff_csv.find("dir/changed.txt,changed,") != std::string::npos);
}

void test_live_metadata_diff_compares_flat_folders() {
    TempDir source("hypersync_live_diff_source");
    TempDir target("hypersync_live_diff_target");

    write_file(source.path / "same.txt", "same");
    write_file(target.path / "same.txt", "xxxx");
    write_file(source.path / "changed.txt", "fresh");
    write_file(target.path / "changed.txt", "old");
    write_file(source.path / "new.txt", "new");
    write_file(target.path / "extra.txt", "extra");
    write_file(target.path / "target_only_dir" / "deep.txt", "deep");

    EngineConfig config;
    config.mode = Mode::dry_run;
    const TransferEngine engine(config);
    const auto report = engine.diff_metadata_trees(source.path, target.path, "size", true, 2, 2, 10.0);

    EXPECT_EQ(report.files_total, 5U);
    EXPECT_EQ(report.files_skipped, 1U);
    EXPECT_EQ(report.files_changed, 1U);
    EXPECT_EQ(report.files_new, 1U);
    EXPECT_EQ(report.files_target_only, 2U);
    EXPECT_EQ(report.files.at("same.txt").diff, DiffKind::skip);
    EXPECT_EQ(report.files.at("changed.txt").diff, DiffKind::changed);
    EXPECT_EQ(report.files.at("new.txt").diff, DiffKind::new_file);
    EXPECT_EQ(report.files.at("extra.txt").diff, DiffKind::target_only);
    EXPECT_EQ(report.files.at("target_only_dir/deep.txt").diff, DiffKind::target_only);
    EXPECT_TRUE(report.folders.find("") != report.folders.end());
    EXPECT_TRUE(report.folders.find("target_only_dir") != report.folders.end());
    EXPECT_TRUE(report.diff_csv.find("target_only_dir/deep.txt,target_only") != std::string::npos);
}

void test_live_metadata_diff_summary_only_counts_without_records() {
    TempDir source("hypersync_live_diff_summary_source");
    TempDir target("hypersync_live_diff_summary_target");

    write_file(source.path / "same.txt", "same");
    write_file(target.path / "same.txt", "same");
    write_file(source.path / "changed.txt", "fresh");
    write_file(target.path / "changed.txt", "old");
    write_file(source.path / "new.txt", "new");
    write_file(target.path / "extra.txt", "extra");

    EngineConfig config;
    config.mode = Mode::dry_run;
    const TransferEngine engine(config);
    const auto report = engine.diff_metadata_trees(source.path, target.path, "size", true, 2, 2, 0.0, false);

    EXPECT_EQ(report.files_total, 4U);
    EXPECT_EQ(report.files_skipped, 1U);
    EXPECT_EQ(report.files_changed, 1U);
    EXPECT_EQ(report.files_new, 1U);
    EXPECT_EQ(report.files_target_only, 1U);
    EXPECT_EQ(report.files.size(), 0U);
    EXPECT_TRUE(report.diff_csv.empty());
}

void test_fake_remote_diff_benchmark_keeps_source_pipeline_independent() {
    EngineConfig config;
    config.mode = Mode::dry_run;
    const TransferEngine engine(config);

    const auto report = engine.benchmark_fake_remote_diff_pipeline(1000,
                                                                   20,
                                                                   4096,
                                                                   2,
                                                                   2,
                                                                   100,
                                                                   128,
                                                                   128,
                                                                   0);

    EXPECT_EQ(report.source_files_generated, 1000U);
    EXPECT_EQ(report.source_folders_generated, 20U);
    EXPECT_EQ(report.target_folders_checked, 20U);
    EXPECT_EQ(report.files_compared, 1000U);
    EXPECT_EQ(report.files_same, 1000U);
    EXPECT_TRUE(report.source_records_per_second > 0.0);
    EXPECT_TRUE(report.total_records_per_second > 0.0);
    EXPECT_TRUE(report.source_elapsed_seconds < report.total_elapsed_seconds);
    EXPECT_TRUE(report.fake_remote_delay_seconds > 0.0);
}

void test_scan_mode_builds_source_scan_rows() {
    EngineConfig config;
    config.mode = Mode::scan;
    const TransferEngine engine(config);
    const auto report = engine.run({
        {"scan/a.txt", "1", 10},
        {"scan/b.txt", "22", 11},
    });

    EXPECT_EQ(report.files_total, 2U);
    EXPECT_EQ(report.source_scan_rows.size(), 2U);
    EXPECT_EQ(report.target_scan_rows.size(), 0U);
    EXPECT_EQ(report.files.at("scan/a.txt").sender_state, FileState::done);
    EXPECT_EQ(report.folders.at("scan").folder_data_hash,
              hypersync::compute_folder_data_hash(report.source_scan_rows));
}

void test_transfer_mode_handles_empty_file() {
    EngineConfig config;
    config.mode = Mode::transfer;
    config.small_pool_slots = 1;
    config.large_pool_slots = 1;
    const TransferEngine engine(config);

    const auto report = engine.run({{"empty.txt", "", 5}});
    EXPECT_EQ(report.files_transferred, 1U);
    EXPECT_EQ(report.chunks_sent, 1U);
    EXPECT_EQ(report.files.at("empty.txt").chunk_count, 1U);
    EXPECT_EQ(report.bytes_transferred, 0ULL);
}

void test_engine_validates_config_and_buffer_capacity() {
    EXPECT_THROW(TransferEngine(EngineConfig{Mode::transfer, 0, 1, 1, 1, 1, false, {}}));
    EXPECT_THROW(TransferEngine(EngineConfig{Mode::transfer, 1, 0, 1, 1, 1, false, {}}));

    EngineConfig config;
    config.mode = Mode::transfer;
    config.small_pool_slots = 0;
    config.small_file_threshold = 8;
    config.large_pool_slots = 1;

    const TransferEngine engine(config);
    EXPECT_THROW(engine.run({{"tiny.txt", "1234", 1}}));
}

enum class TestSuite {
    unit,
    integration,
};

struct RegisteredTest {
    std::string name;
    TestSuite suite = TestSuite::unit;
    std::function<void()> run;
};

struct TestSelection {
    std::optional<TestSuite> suite;
    std::vector<std::string> includes;
    std::vector<std::string> excludes;
    bool list_only = false;
};

std::vector<std::string> split_filter_list(const std::string& value) {
    std::vector<std::string> result;
    std::stringstream input(value);
    std::string item;
    while (std::getline(input, item, ',')) {
        item.erase(item.begin(),
                   std::find_if(item.begin(), item.end(), [](unsigned char ch) {
                       return !std::isspace(ch);
                   }));
        item.erase(std::find_if(item.rbegin(), item.rend(), [](unsigned char ch) {
                       return !std::isspace(ch);
                   }).base(),
                   item.end());
        if (!item.empty()) {
            result.push_back(item);
        }
    }
    return result;
}

std::string test_number_id(std::size_t index) {
    std::ostringstream out;
    out << 't' << std::setw(3) << std::setfill('0') << index;
    return out.str();
}

bool filter_matches(const RegisteredTest& test, std::size_t index, const std::string& filter) {
    const std::string number = std::to_string(index);
    const std::string padded = test_number_id(index);
    return filter == number ||
           filter == "#" + number ||
           filter == padded ||
           filter == "#" + padded ||
           test.name.find(filter) != std::string::npos;
}

bool any_filter_matches(const RegisteredTest& test, std::size_t index, const std::vector<std::string>& filters) {
    return std::any_of(filters.begin(), filters.end(), [&](const std::string& filter) {
        return filter_matches(test, index, filter);
    });
}

TestSelection parse_test_selection(int argc, char** argv) {
    TestSelection selection;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--list" || arg == "--list-tests") {
            selection.list_only = true;
        } else if (arg == "--suite") {
            if (i + 1 >= argc) {
                throw TestFailure("missing value for --suite");
            }
            const std::string value = argv[++i];
            if (value == "unit") {
                selection.suite = TestSuite::unit;
            } else if (value == "integration") {
                selection.suite = TestSuite::integration;
            } else if (value == "all") {
                selection.suite = std::nullopt;
            } else {
                throw TestFailure("unknown suite: " + value);
            }
        } else if (arg == "--include" || arg == "--only" || arg == "--test") {
            if (i + 1 >= argc) {
                throw TestFailure("missing value for " + arg);
            }
            const auto values = split_filter_list(argv[++i]);
            selection.includes.insert(selection.includes.end(), values.begin(), values.end());
        } else if (arg == "--exclude" || arg == "--skip") {
            if (i + 1 >= argc) {
                throw TestFailure("missing value for " + arg);
            }
            const auto values = split_filter_list(argv[++i]);
            selection.excludes.insert(selection.excludes.end(), values.begin(), values.end());
        } else {
            throw TestFailure("unknown test option: " + arg);
        }
    }
    return selection;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<RegisteredTest> tests{
        {"raw_buffer_pool_reuses_slots_and_detects_errors",
         TestSuite::unit,
         test_raw_buffer_pool_reuses_slots_and_detects_errors},
        {"raw_buffer_pool_is_thread_safe_under_parallel_acquire_release",
         TestSuite::unit,
         test_raw_buffer_pool_is_thread_safe_under_parallel_acquire_release},
        {"buf_queue_transfers_handles_without_payload_allocation",
         TestSuite::unit,
         test_buf_queue_transfers_handles_without_payload_allocation},
        {"buf_queue_push_wait_blocks_until_space_is_available",
         TestSuite::unit,
         test_buf_queue_push_wait_blocks_until_space_is_available},
        {"raw_buffer_pool_allocates_byte_slots_and_registry_discards_any_handle",
         TestSuite::unit,
         test_raw_buffer_pool_allocates_byte_slots_and_registry_discards_any_handle},
        {"raw_metadata_buffer_view_detects_stale_and_wrong_handles",
         TestSuite::unit,
         test_raw_metadata_buffer_view_detects_stale_and_wrong_handles},
        {"raw_buffer_pool_and_buf_queue_are_mpmc_safe",
         TestSuite::unit,
         test_raw_buffer_pool_and_buf_queue_are_mpmc_safe},
        {"sharded_buf_queue_steals_from_busy_shards",
         TestSuite::unit,
         test_sharded_buf_queue_steals_from_busy_shards},
        {"buffer_generator_writes_configured_patterns_to_raw_buffers",
         TestSuite::unit,
         test_buffer_generator_writes_configured_patterns_to_raw_buffers},
        {"buffer_generator_to_discarder_pipeline_releases_all_buffers",
         TestSuite::unit,
         test_buffer_generator_to_discarder_pipeline_releases_all_buffers},
        {"buffer_generator_to_sharded_discarder_pipeline_steals_and_releases_all_buffers",
         TestSuite::unit,
         test_buffer_generator_to_sharded_discarder_pipeline_steals_and_releases_all_buffers},
        {"buffer_transport_moves_raw_buffers_over_tcp_and_unix",
         TestSuite::unit,
         test_buffer_transport_moves_raw_buffers_over_tcp_and_unix},
        {"compact_folder_metadata_batch_round_trip",
         TestSuite::unit,
         test_compact_folder_metadata_batch_round_trip},
        {"flat_folder_and_diff_result_buffer_codecs_round_trip",
         TestSuite::unit,
         test_flat_folder_and_diff_result_buffer_codecs_round_trip},
        {"buffer_stream_transport_moves_raw_buffers_over_existing_fd",
         TestSuite::unit,
         test_buffer_stream_transport_moves_raw_buffers_over_existing_fd},
        {"buffer_transport_feeds_metadata_writer_job",
         TestSuite::unit,
         test_buffer_transport_feeds_metadata_writer_job},
        {"status_monitor_renders_jobs_queues_and_socket_requests",
         TestSuite::unit,
         test_status_monitor_renders_jobs_queues_and_socket_requests},
        {"threaded_job_runtime_metrics_report_wait_states",
         TestSuite::unit,
         test_threaded_job_runtime_metrics_report_wait_states},
        {"autoscaler_recommends_cooperative_worker_limits",
         TestSuite::unit,
         test_autoscaler_recommends_cooperative_worker_limits},
        {"pipeline_autoscaler_tunes_one_stage_then_advances",
         TestSuite::unit,
         test_pipeline_autoscaler_tunes_one_stage_then_advances},
        {"split_bucket_priority_balances_eta",
         TestSuite::unit,
         test_split_bucket_priority_balances_eta},
        {"split_scanner_capacity_follows_reader_borrowing",
         TestSuite::unit,
         test_split_scanner_capacity_follows_reader_borrowing},
        {"bucket_path_overload_scores_reflect_eta_reservoir_and_wire_pressure",
         TestSuite::unit,
         test_bucket_path_overload_scores_reflect_eta_reservoir_and_wire_pressure},
        {"synthetic_profile_builder_detects_chronological_phases",
         TestSuite::unit,
         test_synthetic_profile_builder_detects_chronological_phases},
        {"synthetic_replay_cursor_generates_gapless_deterministic_views",
         TestSuite::unit,
         test_synthetic_replay_cursor_generates_gapless_deterministic_views},
        {"synthetic_profile_backend_streams_metadata_and_data",
         TestSuite::unit,
         test_synthetic_profile_backend_streams_metadata_and_data},
        {"synthetic_profile_backend_feeds_batch_folders_recursively",
         TestSuite::unit,
         test_synthetic_profile_backend_feeds_batch_folders_recursively},
        {"synthetic_profile_backend_can_emulate_profile_latency",
         TestSuite::unit,
         test_synthetic_profile_backend_can_emulate_profile_latency},
        {"synthetic_profile_backend_can_fill_fast_prng_payload",
         TestSuite::unit,
         test_synthetic_profile_backend_can_fill_fast_prng_payload},
        {"synthetic_payload_pool_returns_preallocated_blocks",
         TestSuite::unit,
         test_synthetic_payload_pool_returns_preallocated_blocks},
        {"autoscale_profile_store_defaults_and_persists_learned_workers",
         TestSuite::unit,
         test_autoscale_profile_store_defaults_and_persists_learned_workers},
        {"periodic_status_reporter_reuses_status_registry",
         TestSuite::unit,
         test_periodic_status_reporter_reuses_status_registry},
        {"data_hasher_hashes_and_forwards_raw_buffers",
         TestSuite::unit,
         test_data_hasher_hashes_and_forwards_raw_buffers},
        {"packed_small_file_data_buffers_hash_without_repacking",
         TestSuite::unit,
         test_packed_small_file_data_buffers_hash_without_repacking},
        {"nfs_data_buffer_reader_feeds_hasher_pipeline",
         TestSuite::unit,
         test_nfs_data_buffer_reader_feeds_hasher_pipeline},
        {"nfs_data_buffer_reader_packs_small_files_into_owned_buffer",
         TestSuite::unit,
         test_nfs_data_buffer_reader_packs_small_files_into_owned_buffer},
        {"nfs_data_buffer_reader_slides_small_files_without_packing",
         TestSuite::unit,
         test_nfs_data_buffer_reader_slides_small_files_without_packing},
        {"target_data_writer_writes_regular_and_packed_buffers",
         TestSuite::unit,
         test_target_data_writer_writes_regular_and_packed_buffers},
        {"target_meta_writer_creates_flat_folder_directories",
         TestSuite::unit,
         test_target_meta_writer_creates_flat_folder_directories},
        {"scan_index_round_trip_and_folder_hashes", TestSuite::unit, test_scan_index_round_trip_and_folder_hashes},
        {"scan_index_rejects_bad_csv", TestSuite::unit, test_scan_index_rejects_bad_csv},
        {"state_machines_accept_valid_paths_and_reject_invalid_ones",
         TestSuite::unit,
         test_state_machines_accept_valid_paths_and_reject_invalid_ones},
        {"hash64_matches_xxhash64_vectors_and_streaming_updates",
         TestSuite::unit,
         test_hash64_matches_xxhash64_vectors_and_streaming_updates},
        {"content_hash_matches_standard_vectors_and_streaming_updates",
         TestSuite::unit,
         test_content_hash_matches_standard_vectors_and_streaming_updates},
        {"buffer_metadata_footer_round_trip_and_checksums",
         TestSuite::unit,
         test_buffer_metadata_footer_round_trip_and_checksums},
        {"watermark_thresholds", TestSuite::unit, test_watermark_thresholds},
        {"job_classes_exist_and_process_messages", TestSuite::unit, test_job_classes_exist_and_process_messages},
        {"metadata_stats_discarder_drops_records_and_reports_totals",
         TestSuite::unit,
         test_metadata_stats_discarder_drops_records_and_reports_totals},
        {"threaded_job_rethrows_worker_failures_after_joining",
         TestSuite::unit,
         test_threaded_job_rethrows_worker_failures_after_joining},
        {"metadata_record_writer_writes_csv_and_text_inventory",
         TestSuite::unit,
         test_metadata_record_writer_writes_csv_and_text_inventory},
        {"file_metadata_generator_feeds_metadata_writer",
         TestSuite::unit,
         test_file_metadata_generator_feeds_metadata_writer},
        {"queue_job_accepts_custom_message_kinds_without_shared_header_changes",
         TestSuite::unit,
         test_queue_job_accepts_custom_message_kinds_without_shared_header_changes},
        {"spsc_ring_preserves_order_and_handles_cross_thread_transfer",
         TestSuite::unit,
         test_spsc_ring_preserves_order_and_handles_cross_thread_transfer},
        {"config_store_reads_sections_merges_defaults_and_reloads",
         TestSuite::unit,
         test_config_store_reads_sections_merges_defaults_and_reloads},
        {"nfs_backend_local_fallback_and_optional_libnfs_gate",
         TestSuite::unit,
         test_nfs_backend_local_fallback_and_optional_libnfs_gate},
        {"nfs_url_server_range_expansion", TestSuite::unit, test_nfs_url_server_range_expansion},
        {"privilege_utils_require_root_for_owner_change",
         TestSuite::unit,
         test_privilege_utils_require_root_for_owner_change},
        {"filesystem_utils_collect_and_apply_metadata_helpers",
         TestSuite::unit,
         test_filesystem_utils_collect_and_apply_metadata_helpers},
        {"local_target_backend_restores_directory_and_file_metadata",
         TestSuite::unit,
         test_local_target_backend_restores_directory_and_file_metadata},
        {"null_target_backend_discards_regular_and_batch_writes",
         TestSuite::unit,
         test_null_target_backend_discards_regular_and_batch_writes},
        {"real_libnfs_backend_directory_and_target_writer_paths",
         TestSuite::integration,
         test_real_libnfs_backend_directory_and_target_writer_paths},
        {"nfs_jobs_use_backend_for_local_sources", TestSuite::unit, test_nfs_jobs_use_backend_for_local_sources},
        {"real_libnfs_loopback_export_can_scan_and_transfer",
         TestSuite::integration,
         test_real_libnfs_loopback_export_can_scan_and_transfer},
        {"root_receiver_restores_local_standard_attributes",
         TestSuite::integration,
         test_root_receiver_restores_local_standard_attributes},
        {"root_receiver_restores_nfs_standard_attributes_and_skips_on_retry",
         TestSuite::integration,
         test_root_receiver_restores_nfs_standard_attributes_and_skips_on_retry},
        {"non_root_receiver_can_restore_nfs_ownership_via_remote_credentials",
         TestSuite::integration,
         test_non_root_receiver_can_restore_nfs_ownership_via_remote_credentials},
        {"phase1_runtime_transfers_directory_over_tcp", TestSuite::integration, test_phase1_runtime_transfers_directory_over_tcp},
        {"runtime_packs_small_files_over_tcp", TestSuite::integration, test_runtime_packs_small_files_over_tcp},
        {"phase2_runtime_spills_to_cache_and_honors_backpressure",
         TestSuite::integration,
         test_phase2_runtime_spills_to_cache_and_honors_backpressure},
        {"transfer_mode_transfers_small_and_large_files",
         TestSuite::unit,
         test_transfer_mode_transfers_small_and_large_files},
        {"transfer_mode_skips_via_scan_indexes", TestSuite::unit, test_transfer_mode_skips_via_scan_indexes},
        {"transfer_mode_can_skip_per_file_without_folder_fast_skip",
         TestSuite::unit,
         test_transfer_mode_can_skip_per_file_without_folder_fast_skip},
        {"transfer_mode_retries_then_succeeds", TestSuite::unit, test_transfer_mode_retries_then_succeeds},
        {"transfer_mode_can_exhaust_retries_and_fail", TestSuite::unit, test_transfer_mode_can_exhaust_retries_and_fail},
        {"transfer_mode_skip_verify_ignores_forced_failures",
         TestSuite::unit,
         test_transfer_mode_skip_verify_ignores_forced_failures},
        {"dry_run_uses_scans_and_metadata", TestSuite::unit, test_dry_run_uses_scans_and_metadata},
        {"diff_scan_indexes_reports_changes_and_target_only",
         TestSuite::unit,
         test_diff_scan_indexes_reports_changes_and_target_only},
        {"live_metadata_diff_compares_flat_folders", TestSuite::unit, test_live_metadata_diff_compares_flat_folders},
        {"live_metadata_diff_summary_only_counts_without_records",
         TestSuite::unit,
         test_live_metadata_diff_summary_only_counts_without_records},
        {"fake_remote_diff_benchmark_keeps_source_pipeline_independent",
         TestSuite::unit,
         test_fake_remote_diff_benchmark_keeps_source_pipeline_independent},
        {"scan_mode_builds_source_scan_rows", TestSuite::unit, test_scan_mode_builds_source_scan_rows},
        {"main_cli_scan_and_dry_run_smoke", TestSuite::integration, test_main_cli_scan_and_dry_run_smoke},
        {"main_cli_benchmark_meta_smoke", TestSuite::integration, test_main_cli_benchmark_meta_smoke},
        {"main_cli_benchmark_data_smoke", TestSuite::integration, test_main_cli_benchmark_data_smoke},
        {"main_cli_benchmark_data_write_folder_ready_discard_smoke",
         TestSuite::integration,
         test_main_cli_benchmark_data_write_folder_ready_discard_smoke},
        {"main_cli_benchmark_data_write_folder_ready_write_smoke",
         TestSuite::integration,
         test_main_cli_benchmark_data_write_folder_ready_write_smoke},
        {"main_cli_benchmark_data_hash_smoke", TestSuite::integration, test_main_cli_benchmark_data_hash_smoke},
        {"main_cli_benchmark_synthetic_profile_smoke",
         TestSuite::integration,
         test_main_cli_benchmark_synthetic_profile_smoke},
        {"main_cli_benchmark_synthetic_replay_smoke",
         TestSuite::integration,
         test_main_cli_benchmark_synthetic_replay_smoke},
        {"main_cli_benchmark_nfs_profile_smoke",
         TestSuite::integration,
         test_main_cli_benchmark_nfs_profile_smoke},
        {"main_cli_benchmark_hash_smoke", TestSuite::integration, test_main_cli_benchmark_hash_smoke},
        {"main_cli_benchmark_metadata_writer_smoke",
         TestSuite::integration,
         test_main_cli_benchmark_metadata_writer_smoke},
        {"main_cli_hash_smoke", TestSuite::integration, test_main_cli_hash_smoke},
        {"main_cli_status_command_smoke", TestSuite::integration, test_main_cli_status_command_smoke},
        {"main_cli_version_smoke", TestSuite::integration, test_main_cli_version_smoke},
        {"main_cli_send_and_receive_smoke", TestSuite::integration, test_main_cli_send_and_receive_smoke},
        {"main_cli_uses_yaml_runtime_defaults", TestSuite::integration, test_main_cli_uses_yaml_runtime_defaults},
        {"transfer_mode_handles_empty_file", TestSuite::unit, test_transfer_mode_handles_empty_file},
        {"engine_validates_config_and_buffer_capacity", TestSuite::unit, test_engine_validates_config_and_buffer_capacity},
    };

    const TestSelection selection = parse_test_selection(argc, argv);

    std::size_t selected = 0;
    std::size_t passed = 0;
    for (std::size_t index = 0; index < tests.size(); ++index) {
        const auto& test = tests[index];
        const std::size_t test_number = index + 1U;
        if (selection.suite.has_value() && test.suite != *selection.suite) {
            continue;
        }
        if (!selection.includes.empty() && !any_filter_matches(test, test_number, selection.includes)) {
            continue;
        }
        if (any_filter_matches(test, test_number, selection.excludes)) {
            continue;
        }
        ++selected;
        if (selection.list_only) {
            std::cout << '[' << test_number_id(test_number) << "] " << test.name << '\n';
            ++passed;
            continue;
        }
        try {
            test.run();
            ++passed;
            std::cout << "[PASS] [" << test_number_id(test_number) << "] " << test.name << '\n';
        } catch (const std::exception& ex) {
            std::cerr << "[FAIL] [" << test_number_id(test_number) << "] " << test.name << ": " << ex.what() << '\n';
            return 1;
        }
    }

    if (selection.list_only) {
        std::cout << selected << " tests listed\n";
    } else {
        std::cout << passed << "/" << selected << " tests passed\n";
    }
    return 0;
}
