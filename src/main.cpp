#include "hypersync.hpp"

#include <algorithm>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

std::string require_option(const std::vector<std::string>& args, std::size_t& index, std::string_view name) {
    if (index + 1 >= args.size()) {
        throw std::runtime_error("missing value for option " + std::string(name));
    }
    ++index;
    return args[index];
}

std::uint16_t parse_port(const std::string& value, std::string_view flag_name) {
    const int port = std::stoi(value);
    if (port < 1 || port > 65535) {
        throw std::runtime_error("invalid port for " + std::string(flag_name));
    }
    return static_cast<std::uint16_t>(port);
}

std::size_t parse_size_t_option(const std::string& value, std::string_view flag_name) {
    std::size_t consumed = 0;
    const unsigned long long parsed = std::stoull(value, &consumed);
    if (consumed != value.size() || parsed == 0) {
        throw std::runtime_error("invalid positive integer for " + std::string(flag_name));
    }
    return static_cast<std::size_t>(parsed);
}

std::uint64_t parse_u64_option(const std::string& value, std::string_view flag_name) {
    std::size_t consumed = 0;
    const unsigned long long parsed = std::stoull(value, &consumed);
    if (consumed != value.size()) {
        throw std::runtime_error("invalid integer for " + std::string(flag_name));
    }
    return static_cast<std::uint64_t>(parsed);
}

double parse_positive_double_option(const std::string& value, std::string_view flag_name) {
    std::size_t consumed = 0;
    const double parsed = std::stod(value, &consumed);
    if (consumed != value.size() || parsed <= 0.0) {
        throw std::runtime_error("invalid positive number for " + std::string(flag_name));
    }
    return parsed;
}

void print_usage() {
    std::cerr
        << "Usage:\n"
        << "  hypersync [--config <config.yaml>] receive --target <dir|nfs-url> [--bind-host <host>] [--priority-port <port>] [--data-port <port>] [--backpressure-window <bytes>] [--backpressure-pause-ms <ms>] [--skip-verify]\n"
        << "  hypersync status --socket <path>\n"
        << "  hypersync [--config <config.yaml>] send|sync|copy --source <dir|nfs-url> [--host <host>] [--priority-port <port>] [--data-port <port>] [--cache-path <dir>] [--cache-threshold <bytes>] [--skip-verify]\n"
        << "  hypersync [--config <config.yaml>] scan --source <dir|nfs-url> --output <scan.csv|txt|parquet> [--scan-side S|T] [--output-format text|csv|parquet] [--records all|files|folders] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--record-buffer-slots <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] diff (--source <dir|nfs-url> --target <dir|nfs-url> | --source-scan <scan.csv> --target-scan <scan.csv>) [--compare size|time|content] [--output <diff.csv>] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--max-duration-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] dry-run --source <dir|nfs-url> [--source-scan <scan.csv>] [--target-scan <scan.csv>] [--output <diff.csv>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-meta --source <dir|nfs-url> [--non-recursive] [--discard-after-checker|--keep-after-checker|--metadata-stats-discarder] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--metadata-output <path>] [--metadata-output-format text|csv|parquet] [--metadata-records all|files|folders] [--metadata-output-partitions <n>] [--metadata-output-partition-mode single|processes] [--record-buffer-slots <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-data --source <dir|nfs-url> [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--data-reader-threads <n>] [--data-outstanding-requests <n>] [--max-files-queued <n>] [--data-buffer-slots <n>] [--data-queue-depth <n>] [--data-copy-mode copy|no-copy] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-data-hash --source <dir|nfs-url> [--hash md5|sha256|xxh64|xxh3_64|xxh3_128] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--data-reader-threads <n>] [--data-outstanding-requests <n>] [--hash-threads <n>] [--hash-work-factor <n>] [--max-files-queued <n>] [--data-buffer-slots <n>] [--data-queue-depth <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-hash [--hash md5|sha256|xxh64|xxh3_64|xxh3_128] [--threads <n>] [--block-size <bytes>] [--duration-seconds <n>] [--min-gigabits-per-core <n>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-transport [--transports <n>] [--buffers-per-transport <n>] [--buffer-size <bytes>] [--pool-slots <n>] [--generator-threads <n>] [--sender-threads <n>] [--receiver-threads <n>] [--discarder-threads <n>] [--pattern zero|fast_text|xoshiro256] [--transport none|unix|tcp] [--shared-input] [--base-port <port>] [--socket-dir <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-metadata-writer --output <records.parquet|dataset-dir|csv|txt> [--output-format text|csv|parquet] [--file-count <n>] [--folder-count <n>] [--batch-size <n>] [--average-file-size <bytes>] [--duckdb-memory-limit <value>] [--duckdb-threads <n>] [--duckdb-checkpoint-threshold <value>] [--parquet-compression zstd|snappy|uncompressed] [--partitions <n>] [--partition-mode threads|processes|transport-processes|generate-discard|generate-hash-discard|pack-discard|folder-pack-discard|transport-discard]\n"
        << "  hypersync [--config <config.yaml>] hash --source <dir|nfs-url> --output <records.csv|txt|parquet> [--output-format text|csv|parquet] [--records all|files|folders] [--hash md5|sha256|xxh64|xxh3_64|xxh3_128] [--hash-mode file|blocks] [--hash-block-size <bytes>] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--data-reader-threads <n>] [--data-outstanding-requests <n>] [--hash-threads <n>] [--max-files-queued <n>] [--max-hash-chunks-queued <n>] [--max-duration-seconds <n>]\n"
        << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            print_usage();
            return 1;
        }

        const std::vector<std::string> raw_args(argv + 1, argv + argc);
        if (raw_args.size() == 1 && raw_args.front() == "--version") {
            std::cout << "hypersync " << hypersync::kVersion << '\n';
            return 0;
        }

        std::string config_path;
        std::vector<std::string> args;
        args.reserve(raw_args.size());
        for (std::size_t i = 0; i < raw_args.size(); ++i) {
            if (raw_args[i] == "--config") {
                config_path = require_option(raw_args, i, "--config");
                continue;
            }
            args.push_back(raw_args[i]);
        }
        if (args.empty()) {
            print_usage();
            return 1;
        }

        const std::string command = args.front();

        if (command == "status") {
            std::filesystem::path socket_path;
            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--socket" || args[i] == "--status-socket") {
                    socket_path = require_option(args, i, args[i]);
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (socket_path.empty()) {
                throw std::runtime_error("--socket is required");
            }
            std::cout << hypersync::request_status(socket_path);
            return 0;
        }

        hypersync::ConfigStore config_store(
            config_path.empty()
                ? std::vector<std::filesystem::path>{hypersync::default_config_path()}
                : std::vector<std::filesystem::path>{hypersync::default_config_path(), config_path});

        hypersync::EngineConfig engine_config = hypersync::load_engine_config(config_store);
        for (const auto& arg : args) {
            if (arg == "--skip-verify") {
                engine_config.skip_verify = true;
                break;
            }
        }
        hypersync::TransferEngine engine(engine_config, config_store);

        if (command == "receive") {
            hypersync::ReceiverRuntimeConfig runtime = hypersync::load_receiver_runtime_config(config_store);
            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--target") {
                    runtime.target_root = require_option(args, i, "--target");
                } else if (args[i] == "--bind-host") {
                    runtime.bind_host = require_option(args, i, "--bind-host");
                } else if (args[i] == "--priority-port") {
                    runtime.priority_port = parse_port(require_option(args, i, "--priority-port"), "--priority-port");
                } else if (args[i] == "--data-port") {
                    runtime.data_port = parse_port(require_option(args, i, "--data-port"), "--data-port");
                } else if (args[i] == "--backpressure-window") {
                    runtime.backpressure_window_bytes = static_cast<std::uint64_t>(
                        std::stoull(require_option(args, i, "--backpressure-window")));
                } else if (args[i] == "--backpressure-pause-ms") {
                    runtime.backpressure_pause_ms = static_cast<std::uint32_t>(
                        std::stoul(require_option(args, i, "--backpressure-pause-ms")));
                } else if (args[i] == "--skip-verify") {
                    // handled by pre-pass above
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (runtime.target_root.empty()) {
                throw std::runtime_error("--target is required");
            }
            engine.run_receiver(runtime);
            return 0;
        }

        if (command == "send" || command == "sync" || command == "copy") {
            hypersync::SenderRuntimeConfig runtime = hypersync::load_sender_runtime_config(config_store);
            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    runtime.source_root = require_option(args, i, "--source");
                } else if (args[i] == "--host") {
                    runtime.remote_host = require_option(args, i, "--host");
                } else if (args[i] == "--priority-port") {
                    runtime.priority_port = parse_port(require_option(args, i, "--priority-port"), "--priority-port");
                } else if (args[i] == "--data-port") {
                    runtime.data_port = parse_port(require_option(args, i, "--data-port"), "--data-port");
                } else if (args[i] == "--cache-path") {
                    runtime.cache_root = require_option(args, i, "--cache-path");
                } else if (args[i] == "--cache-threshold") {
                    runtime.cache_file_threshold_bytes = static_cast<std::uint64_t>(
                        std::stoull(require_option(args, i, "--cache-threshold")));
                } else if (args[i] == "--skip-verify") {
                    // handled by pre-pass above
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (runtime.source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.transfer_directory(runtime);
            std::cout << "files_total=" << report.files_total
                      << " transferred=" << report.files_transferred
                      << " skipped=" << report.files_skipped
                      << " failed=" << report.files_failed
                      << " bytes=" << report.bytes_transferred
                      << " chunks_sent=" << report.chunks_sent << '\n';
            return report.files_failed == 0 ? 0 : 2;
        }

        if (command == "scan") {
            std::filesystem::path output_path;
            char scan_side = 'S';
            std::filesystem::path source_root;
            bool recursive = true;
            bool metadata_scan = false;
            std::string output_format;
            std::string metadata_records = "all";
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::size_t record_buffer_slots = 0;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            std::filesystem::path status_socket_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                    metadata_scan = true;
                } else if (args[i] == "--scan-side") {
                    const std::string side = require_option(args, i, "--scan-side");
                    if (side.size() != 1U || (side[0] != 'S' && side[0] != 'T')) {
                        throw std::runtime_error("--scan-side must be S or T");
                    }
                    scan_side = side[0];
                } else if (args[i] == "--output-format" || args[i] == "--format") {
                    output_format = require_option(args, i, args[i]);
                    if (output_format == "auto") {
                        output_format.clear();
                    }
                    metadata_scan = true;
                } else if (args[i] == "--records") {
                    metadata_records = require_option(args, i, "--records");
                    metadata_scan = true;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                    metadata_scan = true;
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                    metadata_scan = true;
                } else if (args[i] == "--record-buffer-slots") {
                    record_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--record-buffer-slots"),
                                            "--record-buffer-slots");
                    metadata_scan = true;
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                    metadata_scan = true;
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                    metadata_scan = true;
                } else if (args[i] == "--status-socket") {
                    status_socket_path = require_option(args, i, "--status-socket");
                    metadata_scan = true;
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }
            if (output_path.empty()) {
                throw std::runtime_error("--output is required");
            }

            const std::string output_extension = output_path.extension().string();
            if (output_extension == ".parquet" || output_extension == ".txt") {
                metadata_scan = true;
            }

            if (metadata_scan) {
                const auto report = engine.benchmark_metadata_pipeline(source_root,
                                                                       recursive,
                                                                       true,
                                                                       true,
                                                                       meta_reader_threads,
                                                                       metadata_async_depth,
                                                                       output_path,
                                                                       output_format,
                                                                       metadata_records,
                                                                       max_duration_seconds,
                                                                       stats_interval_seconds,
                                                                       record_buffer_slots,
                                                                       1,
                                                                       "single",
                                                                       status_socket_path);
                std::cout << "scan_metadata_records files_found=" << report.files_seen
                          << " folders_found=" << report.folders_found
                          << " logical_size_bytes=" << report.logical_size_bytes
                          << " records_per_second=" << report.records_per_second
                          << " meta_reader_threads=" << report.meta_reader_threads
                          << " metadata_async_depth=" << report.metadata_async_depth
                          << " record_buffer_slots=" << report.record_buffer_slots
                          << " stats_interval_seconds=" << report.stats_interval_seconds
                          << " metadata_files_written=" << report.metadata_files_written
                          << " metadata_folders_written=" << report.metadata_folders_written
                          << " scan_run_id=" << report.scan_run_id
                          << " async_backend=" << (report.meta_reader_async ? "true" : "false")
                          << " elapsed_s=" << report.elapsed_seconds
                          << " output=" << output_path << '\n';
                return 0;
            }

            const auto index = engine.build_scan_index(source_root, scan_side, true);
            hypersync::TransferEngine::write_scan_csv(index, output_path);
            std::cout << "scan_rows=" << index.rows().size() << " output=" << output_path << '\n';
            return 0;
        }

        if (command == "dry-run") {
            std::filesystem::path source_root;
            std::string source_scan_path;
            std::string target_scan_path;
            std::string output_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--source-scan") {
                    source_scan_path = require_option(args, i, "--source-scan");
                } else if (args[i] == "--target-scan") {
                    target_scan_path = require_option(args, i, "--target-scan");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            std::optional<hypersync::ScanIndex> source_scan;
            std::optional<hypersync::ScanIndex> target_scan;
            if (!source_scan_path.empty()) {
                source_scan = hypersync::TransferEngine::load_scan_csv(source_scan_path);
            }
            if (!target_scan_path.empty()) {
                target_scan = hypersync::TransferEngine::load_scan_csv(target_scan_path);
            }

            const auto report = engine.dry_run_directory(source_root,
                                                         source_scan ? &*source_scan : nullptr,
                                                         target_scan ? &*target_scan : nullptr,
                                                         true);
            if (!output_path.empty()) {
                hypersync::TransferEngine::write_diff_csv(report, output_path);
            }
            std::cout << "files_total=" << report.files_total
                      << " skipped=" << report.files_skipped
                      << " bytes_planned=" << report.bytes_planned << '\n';
            if (output_path.empty()) {
                std::cout << report.diff_csv;
            }
            return 0;
        }

        if (command == "diff") {
            std::filesystem::path source_root;
            std::filesystem::path target_root;
            std::string source_scan_path;
            std::string target_scan_path;
            std::string output_path;
            std::string compare_mode = "time";
            bool recursive = true;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            double max_duration_seconds = 0.0;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--target") {
                    target_root = require_option(args, i, "--target");
                } else if (args[i] == "--source-scan") {
                    source_scan_path = require_option(args, i, "--source-scan");
                } else if (args[i] == "--target-scan") {
                    target_scan_path = require_option(args, i, "--target-scan");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else if (args[i] == "--compare" || args[i] == "--mode") {
                    compare_mode = require_option(args, i, args[i]);
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads = parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                                              "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth = parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                                               "--metadata-async-depth");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds = parse_positive_double_option(
                        require_option(args, i, "--max-duration-seconds"),
                        "--max-duration-seconds");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if ((!source_root.empty() || !target_root.empty()) &&
                (!source_scan_path.empty() || !target_scan_path.empty())) {
                throw std::runtime_error("use either --source/--target or --source-scan/--target-scan, not both");
            }

            hypersync::TransferReport report;
            if (!source_root.empty() || !target_root.empty()) {
                if (source_root.empty()) {
                    throw std::runtime_error("--source is required");
                }
                if (target_root.empty()) {
                    throw std::runtime_error("--target is required");
                }
                report = engine.diff_metadata_trees(source_root,
                                                    target_root,
                                                    compare_mode,
                                                    recursive,
                                                    meta_reader_threads,
                                                    metadata_async_depth,
                                                    max_duration_seconds);
            } else {
                if (source_scan_path.empty()) {
                    throw std::runtime_error("--source-scan is required");
                }
                if (target_scan_path.empty()) {
                    throw std::runtime_error("--target-scan is required");
                }
                const auto source_scan = hypersync::TransferEngine::load_scan_csv(source_scan_path);
                const auto target_scan = hypersync::TransferEngine::load_scan_csv(target_scan_path);
                report = engine.diff_scan_indexes(source_scan, target_scan, compare_mode);
            }
            if (!output_path.empty()) {
                hypersync::TransferEngine::write_diff_csv(report, output_path);
            }

            std::size_t changed = 0;
            std::size_t created = 0;
            std::size_t target_only = 0;
            for (const auto& [_, outcome] : report.files) {
                if (outcome.diff == hypersync::DiffKind::changed) {
                    ++changed;
                } else if (outcome.diff == hypersync::DiffKind::new_file) {
                    ++created;
                } else if (outcome.diff == hypersync::DiffKind::target_only) {
                    ++target_only;
                }
            }
            std::cout << "diff_records=" << report.files_total
                      << " same=" << report.files_skipped
                      << " changed=" << changed
                      << " new=" << created
                      << " target_only=" << target_only
                      << " bytes_planned=" << report.bytes_planned
                      << " compare_mode=" << compare_mode << '\n';
            if (output_path.empty()) {
                std::cout << report.diff_csv;
            }
            return 0;
        }

        if (command == "benchmark-hash" || command == "hash-speed") {
            std::string hash_algorithm = "xxh64";
            std::size_t worker_threads = std::max<unsigned>(1U, std::thread::hardware_concurrency());
            std::size_t block_size = 1024U * 1024U;
            double duration_seconds = 5.0;
            double min_gigabits_per_core_second = 0.0;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--hash" || args[i] == "--hash-algorithm") {
                    hash_algorithm = require_option(args, i, args[i]);
                } else if (args[i] == "--threads" || args[i] == "--hash-threads") {
                    worker_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--block-size" || args[i] == "--hash-block-size") {
                    block_size = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--duration-seconds" || args[i] == "--max-duration-seconds") {
                    duration_seconds =
                        parse_positive_double_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--min-gigabits-per-core" ||
                           args[i] == "--min-gbits-per-core" ||
                           args[i] == "--min-gigabits-per-core-second") {
                    min_gigabits_per_core_second =
                        parse_positive_double_option(require_option(args, i, args[i]), args[i]);
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            const auto report = engine.benchmark_hash_speed(hash_algorithm,
                                                            worker_threads,
                                                            block_size,
                                                            duration_seconds,
                                                            min_gigabits_per_core_second);
            std::cout << "hash_speed hash_algorithm=" << report.hash_algorithm
                      << " worker_threads=" << report.worker_threads
                      << " hardware_threads=" << report.hardware_threads
                      << " block_size=" << report.block_size
                      << " iterations=" << report.iterations
                      << " bytes_hashed=" << report.bytes_hashed
                      << " bytes_per_second=" << report.bytes_per_second
                      << " gigabits_per_second=" << report.gigabits_per_second
                      << " bytes_per_core_second=" << report.bytes_per_core_second
                      << " gigabits_per_core=" << report.gigabits_per_core_second
                      << " gigabits_per_hardware_core=" << report.gigabits_per_hardware_core_second
                      << " min_gigabits_per_core=" << report.min_gigabits_per_core_second
                      << " passed=" << (report.passed ? "true" : "false")
                      << " digest_mix=" << report.digest_mix
                      << " duration_s=" << report.duration_seconds
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return report.passed ? 0 : 2;
        }

        if (command == "benchmark-transport" || command == "transport-speed") {
            std::size_t transports = 1;
            std::uint64_t buffers_per_transport = 1024;
            std::size_t buffer_size = 1024U * 1024U;
            std::size_t pool_slots = 256;
            std::size_t generator_threads = 1;
            std::size_t sender_threads = 1;
            std::size_t receiver_threads = 1;
            std::size_t discarder_threads = 1;
            std::string pattern = "xoshiro256";
            std::string transport_kind = "unix";
            std::uint16_t base_port = 39000;
            std::filesystem::path socket_dir;
            bool shared_input_queue = false;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--transports" || args[i] == "--transport-count") {
                    transports = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--buffers-per-transport" ||
                           args[i] == "--buffer-count" ||
                           args[i] == "--buffers") {
                    buffers_per_transport = parse_u64_option(require_option(args, i, args[i]), args[i]);
                    if (buffers_per_transport == 0U) {
                        throw std::runtime_error("invalid positive integer for " + args[i]);
                    }
                } else if (args[i] == "--buffer-size" || args[i] == "--block-size") {
                    buffer_size = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--pool-slots" || args[i] == "--pool-slots-per-transport") {
                    pool_slots = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--generator-threads") {
                    generator_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--sender-threads") {
                    sender_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--receiver-threads") {
                    receiver_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--discarder-threads") {
                    discarder_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--pattern") {
                    pattern = require_option(args, i, "--pattern");
                } else if (args[i] == "--transport") {
                    transport_kind = require_option(args, i, "--transport");
                } else if (args[i] == "--shared-input" || args[i] == "--shared-input-queue") {
                    shared_input_queue = true;
                } else if (args[i] == "--base-port") {
                    base_port = parse_port(require_option(args, i, "--base-port"), "--base-port");
                } else if (args[i] == "--socket-dir") {
                    socket_dir = require_option(args, i, "--socket-dir");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            const auto report = engine.benchmark_buffer_transport(transports,
                                                                   buffers_per_transport,
                                                                   buffer_size,
                                                                   pool_slots,
                                                                   generator_threads,
                                                                   sender_threads,
                                                                   receiver_threads,
                                                                   discarder_threads,
                                                                   pattern,
                                                                   transport_kind,
                                                                   base_port,
                                                                   socket_dir,
                                                                   shared_input_queue);
            std::cout << "buffer_transport_benchmark"
                      << " transport=" << report.transport_kind
                      << " pattern=" << report.pattern
                      << " transports=" << report.transports
                      << " generator_threads=" << report.generator_threads
                      << " sender_threads=" << report.sender_threads
                      << " receiver_threads=" << report.receiver_threads
                      << " discarder_threads=" << report.discarder_threads
                      << " buffer_size=" << report.buffer_size
                      << " pool_slots_per_transport=" << report.pool_slots_per_transport
                      << " buffers_per_transport=" << report.buffers_per_transport
                      << " buffers_generated=" << report.buffers_generated
                      << " buffers_sent=" << report.buffers_sent
                      << " buffers_received=" << report.buffers_received
                      << " buffers_discarded=" << report.buffers_discarded
                      << " payload_bytes_sent=" << report.payload_bytes_sent
                      << " payload_bytes_received=" << report.payload_bytes_received
                      << " bytes_per_second=" << report.bytes_per_second
                      << " GB_per_second=" << report.gigabytes_per_second
                      << " GiB_per_second=" << report.gibibytes_per_second
                      << " Gbit_per_second=" << report.gigabits_per_second
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-metadata-writer" || command == "benchmark-parquet") {
            std::filesystem::path output_path;
            std::string output_format = "parquet";
            std::uint64_t file_count = 1'000'000;
            std::uint64_t folder_count = 1'000;
            std::size_t batch_size = 65'536;
            std::uint64_t average_file_size = 32U * 1024U;
            std::string duckdb_memory_limit;
            std::size_t duckdb_threads = 0;
            std::string duckdb_checkpoint_threshold;
            std::string parquet_compression;
            std::size_t partitions = 1;
            std::string partition_mode = "threads";

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else if (args[i] == "--output-format" || args[i] == "--format") {
                    output_format = require_option(args, i, args[i]);
                } else if (args[i] == "--file-count" || args[i] == "--files") {
                    file_count = parse_u64_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--folder-count" || args[i] == "--folders") {
                    folder_count = parse_u64_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--batch-size") {
                    batch_size = parse_size_t_option(require_option(args, i, "--batch-size"), "--batch-size");
                } else if (args[i] == "--average-file-size") {
                    average_file_size = parse_u64_option(require_option(args, i, "--average-file-size"),
                                                         "--average-file-size");
                } else if (args[i] == "--duckdb-memory-limit") {
                    duckdb_memory_limit = require_option(args, i, "--duckdb-memory-limit");
                } else if (args[i] == "--duckdb-threads") {
                    duckdb_threads = parse_size_t_option(require_option(args, i, "--duckdb-threads"),
                                                         "--duckdb-threads");
                } else if (args[i] == "--duckdb-checkpoint-threshold") {
                    duckdb_checkpoint_threshold = require_option(args, i, "--duckdb-checkpoint-threshold");
                } else if (args[i] == "--parquet-compression") {
                    parquet_compression = require_option(args, i, "--parquet-compression");
                } else if (args[i] == "--partitions") {
                    partitions = parse_size_t_option(require_option(args, i, "--partitions"), "--partitions");
                } else if (args[i] == "--partition-mode") {
                    partition_mode = require_option(args, i, "--partition-mode");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (output_path.empty()) {
                throw std::runtime_error("--output is required");
            }

            const auto report = engine.benchmark_metadata_writer(output_path,
                                                                  output_format,
                                                                  file_count,
                                                                  folder_count,
                                                                  batch_size,
                                                                  average_file_size,
                                                                  duckdb_memory_limit,
                                                                  duckdb_threads,
                                                                  duckdb_checkpoint_threshold,
                                                                  parquet_compression,
                                                                  partitions,
                                                                  partition_mode);
            std::cout << "metadata_writer_benchmark"
                      << " output_format=" << report.output_format
                      << " output=" << report.output_path
                      << " files_generated=" << report.files_generated
                      << " folders_generated=" << report.folders_generated
                      << " records_written=" << report.records_written
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " records_per_second=" << report.records_per_second
                      << " files_per_second=" << report.files_per_second
                      << " batch_size=" << report.batch_size
                      << " partitions=" << report.partitions
                      << " partition_mode=" << report.partition_mode
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "hash") {
            std::filesystem::path source_root;
            std::filesystem::path output_path;
            std::string output_format;
            std::string records = "all";
            std::string hash_algorithm = "xxh64";
            std::string hash_mode = "file";
            bool recursive = true;
            std::size_t meta_reader_threads = 1;
            std::size_t metadata_async_depth = 16;
            std::size_t data_reader_threads = 0;
            std::size_t data_outstanding_requests = 0;
            std::size_t hash_worker_threads = 0;
            std::size_t max_files_queued = 1024;
            std::size_t max_hash_chunks_queued = 4096;
            std::size_t hash_block_size = 1024U * 1024U;
            double max_duration_seconds = 0.0;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else if (args[i] == "--output-format" || args[i] == "--metadata-output-format") {
                    output_format = require_option(args, i, args[i]);
                } else if (args[i] == "--records" || args[i] == "--metadata-records") {
                    records = require_option(args, i, args[i]);
                } else if (args[i] == "--hash" || args[i] == "--hash-algorithm") {
                    hash_algorithm = require_option(args, i, args[i]);
                } else if (args[i] == "--hash-mode") {
                    hash_mode = require_option(args, i, "--hash-mode");
                } else if (args[i] == "--hash-block-size") {
                    hash_block_size =
                        parse_size_t_option(require_option(args, i, "--hash-block-size"),
                                            "--hash-block-size");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--data-reader-threads") {
                    data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--data-reader-threads"),
                                            "--data-reader-threads");
                } else if (args[i] == "--data-outstanding-requests") {
                    data_outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--data-outstanding-requests"),
                                            "--data-outstanding-requests");
                } else if (args[i] == "--hash-threads" || args[i] == "--hash-worker-threads") {
                    hash_worker_threads =
                        parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--max-files-queued") {
                    max_files_queued =
                        parse_size_t_option(require_option(args, i, "--max-files-queued"),
                                            "--max-files-queued");
                } else if (args[i] == "--max-hash-chunks-queued") {
                    max_hash_chunks_queued =
                        parse_size_t_option(require_option(args, i, "--max-hash-chunks-queued"),
                                            "--max-hash-chunks-queued");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }
            if (output_path.empty()) {
                throw std::runtime_error("--output is required");
            }

            const auto report = engine.hash_inventory_pipeline(source_root,
                                                               recursive,
                                                               hash_algorithm,
                                                               meta_reader_threads,
                                                               metadata_async_depth,
                                                               data_reader_threads,
                                                               data_outstanding_requests,
                                                               hash_worker_threads,
                                                               max_files_queued,
                                                               max_hash_chunks_queued,
                                                               hash_mode,
                                                               hash_block_size,
                                                               output_path,
                                                               output_format,
                                                               records,
                                                               max_duration_seconds);
            std::cout << "hash_inventory files_found=" << report.files_found
                      << " folders_found=" << report.folders_found
                      << " files_hashed=" << report.files_hashed
                      << " files_failed=" << report.files_failed
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " bytes_read=" << report.bytes_read
                      << " bytes_hashed=" << report.bytes_hashed
                      << " read_bytes_per_second=" << report.read_bytes_per_second
                      << " read_gigabits_per_second=" << report.read_gigabits_per_second
                      << " bytes_per_second=" << report.bytes_per_second
                      << " hash_mode=" << report.hash_mode
                      << " hash_algorithm=" << report.hash_algorithm
                      << " metadata_files_written=" << report.metadata_files_written
                      << " metadata_folders_written=" << report.metadata_folders_written
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " data_reader_threads=" << report.data_reader_threads
                      << " data_outstanding_requests=" << report.data_outstanding_requests
                      << " hash_threads=" << report.hash_worker_threads
                      << " max_files_queued=" << report.max_files_queued
                      << " max_hash_chunks_queued=" << report.max_hash_chunks_queued
                      << " hash_block_size=" << report.hash_block_size
                      << " meta_async=" << (report.meta_reader_async ? "true" : "false")
                      << " data_async=" << (report.data_reader_async ? "true" : "false")
                      << " read_elapsed_s=" << report.read_elapsed_seconds
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return report.files_failed == 0 ? 0 : 2;
        }

        if (command == "benchmark-meta") {
            std::filesystem::path source_root;
            bool recursive = true;
            bool discard_after_checker =
                hypersync::load_checker_config(config_store).discard_checked_records;
            bool use_stats_discarder =
                hypersync::load_metadata_stats_discarder_config(config_store).enabled;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::filesystem::path metadata_output_path;
            std::string metadata_output_format;
            std::string metadata_records;
            std::size_t metadata_output_partitions = 1;
            std::string metadata_output_partition_mode = "single";
            std::size_t record_buffer_slots = 0;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            std::filesystem::path status_socket_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--discard-after-checker") {
                    discard_after_checker = true;
                } else if (args[i] == "--keep-after-checker") {
                    discard_after_checker = false;
                } else if (args[i] == "--metadata-stats-discarder") {
                    use_stats_discarder = true;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--metadata-output") {
                    metadata_output_path = require_option(args, i, "--metadata-output");
                    use_stats_discarder = true;
                } else if (args[i] == "--metadata-output-format") {
                    metadata_output_format = require_option(args, i, "--metadata-output-format");
                } else if (args[i] == "--metadata-records") {
                    metadata_records = require_option(args, i, "--metadata-records");
                } else if (args[i] == "--metadata-output-partitions") {
                    metadata_output_partitions =
                        parse_size_t_option(require_option(args, i, "--metadata-output-partitions"),
                                            "--metadata-output-partitions");
                } else if (args[i] == "--metadata-output-partition-mode") {
                    metadata_output_partition_mode = require_option(args, i, "--metadata-output-partition-mode");
                } else if (args[i] == "--record-buffer-slots") {
                    record_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--record-buffer-slots"),
                                            "--record-buffer-slots");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--status-socket") {
                    status_socket_path = require_option(args, i, "--status-socket");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.benchmark_metadata_pipeline(source_root,
                                                                   recursive,
                                                                   discard_after_checker,
                                                                   use_stats_discarder,
                                                                   meta_reader_threads,
                                                                   metadata_async_depth,
                                                                   metadata_output_path,
                                                                   metadata_output_format,
                                                                   metadata_records,
                                                                   max_duration_seconds,
                                                                   stats_interval_seconds,
                                                                   record_buffer_slots,
                                                                   metadata_output_partitions,
                                                                   metadata_output_partition_mode,
                                                                   status_socket_path);
            std::cout << "files_seen=" << report.files_seen
                      << " checker_emitted=" << report.checker_emitted
                      << " checker_discarded=" << report.checker_discarded
                      << " folders_found=" << report.folders_found
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " records_per_second=" << report.records_per_second
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " record_buffer_slots=" << report.record_buffer_slots
                      << " stats_interval_seconds=" << report.stats_interval_seconds
                      << " metadata_files_written=" << report.metadata_files_written
                      << " metadata_folders_written=" << report.metadata_folders_written
                      << " metadata_output_partitions=" << report.metadata_output_partitions
                      << " scan_run_id=" << report.scan_run_id
                      << " async_backend=" << (report.meta_reader_async ? "true" : "false")
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-data") {
            std::filesystem::path source_root;
            bool recursive = true;
            std::size_t meta_reader_threads = 1;
            std::size_t metadata_async_depth = 16;
            std::size_t data_reader_threads = 0;
            std::size_t data_outstanding_requests = 0;
            std::size_t max_files_queued = 1024;
            std::size_t data_buffer_slots = 0;
            std::size_t data_queue_depth = 0;
            std::string data_copy_mode;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            std::filesystem::path status_socket_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--data-reader-threads") {
                    data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--data-reader-threads"),
                                            "--data-reader-threads");
                } else if (args[i] == "--data-outstanding-requests") {
                    data_outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--data-outstanding-requests"),
                                            "--data-outstanding-requests");
                } else if (args[i] == "--max-files-queued") {
                    max_files_queued =
                        parse_size_t_option(require_option(args, i, "--max-files-queued"),
                                            "--max-files-queued");
                } else if (args[i] == "--data-buffer-slots") {
                    data_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--data-buffer-slots"),
                                            "--data-buffer-slots");
                } else if (args[i] == "--data-queue-depth") {
                    data_queue_depth =
                        parse_size_t_option(require_option(args, i, "--data-queue-depth"),
                                            "--data-queue-depth");
                } else if (args[i] == "--data-copy-mode") {
                    data_copy_mode = require_option(args, i, "--data-copy-mode");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--status-socket") {
                    status_socket_path = require_option(args, i, "--status-socket");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.benchmark_data_read_pipeline(source_root,
                                                                    recursive,
                                                                    meta_reader_threads,
                                                                    metadata_async_depth,
                                                                    data_reader_threads,
                                                                    data_outstanding_requests,
                                                                    max_files_queued,
                                                                    data_buffer_slots,
                                                                    data_queue_depth,
                                                                    data_copy_mode,
                                                                    max_duration_seconds,
                                                                    stats_interval_seconds,
                                                                    status_socket_path);
            std::cout << "data_benchmark files_found=" << report.files_found
                      << " folders_found=" << report.folders_found
                      << " files_read=" << report.files_read
                      << " files_failed=" << report.files_failed
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " bytes_read=" << report.bytes_read
                      << " bytes_per_second=" << report.bytes_per_second
                      << " gigabits_per_second=" << report.gigabits_per_second
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " data_reader_threads=" << report.data_reader_threads
                      << " data_outstanding_requests=" << report.data_outstanding_requests
                      << " max_files_queued=" << report.max_files_queued
                      << " data_buffer_slots=" << report.data_buffer_slots
                      << " data_queue_depth=" << report.data_queue_depth
                      << " data_copy_mode=" << report.data_copy_mode
                      << " meta_async=" << (report.meta_reader_async ? "true" : "false")
                      << " data_async=" << (report.data_reader_async ? "true" : "false")
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-data-hash") {
            std::filesystem::path source_root;
            bool recursive = true;
            std::string hash_algorithm = "xxh64";
            std::size_t meta_reader_threads = 1;
            std::size_t metadata_async_depth = 16;
            std::size_t data_reader_threads = 0;
            std::size_t data_outstanding_requests = 0;
            std::size_t hash_worker_threads = 0;
            std::size_t hash_work_factor = 1;
            std::size_t max_files_queued = 1024;
            std::size_t data_buffer_slots = 0;
            std::size_t data_queue_depth = 0;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            std::filesystem::path status_socket_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--hash") {
                    hash_algorithm = require_option(args, i, "--hash");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--data-reader-threads") {
                    data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--data-reader-threads"),
                                            "--data-reader-threads");
                } else if (args[i] == "--data-outstanding-requests") {
                    data_outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--data-outstanding-requests"),
                                            "--data-outstanding-requests");
                } else if (args[i] == "--hash-threads") {
                    hash_worker_threads =
                        parse_size_t_option(require_option(args, i, "--hash-threads"),
                                            "--hash-threads");
                } else if (args[i] == "--hash-work-factor") {
                    hash_work_factor =
                        parse_size_t_option(require_option(args, i, "--hash-work-factor"),
                                            "--hash-work-factor");
                } else if (args[i] == "--max-files-queued") {
                    max_files_queued =
                        parse_size_t_option(require_option(args, i, "--max-files-queued"),
                                            "--max-files-queued");
                } else if (args[i] == "--data-buffer-slots") {
                    data_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--data-buffer-slots"),
                                            "--data-buffer-slots");
                } else if (args[i] == "--data-queue-depth") {
                    data_queue_depth =
                        parse_size_t_option(require_option(args, i, "--data-queue-depth"),
                                            "--data-queue-depth");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--status-socket") {
                    status_socket_path = require_option(args, i, "--status-socket");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.benchmark_data_hash_pipeline(source_root,
                                                                    recursive,
                                                                    hash_algorithm,
                                                                    meta_reader_threads,
                                                                    metadata_async_depth,
                                                                    data_reader_threads,
                                                                    data_outstanding_requests,
                                                                    hash_worker_threads,
                                                                    max_files_queued,
                                                                    data_buffer_slots,
                                                                    data_queue_depth,
                                                                    hash_work_factor,
                                                                    max_duration_seconds,
                                                                    stats_interval_seconds,
                                                                    status_socket_path);
            std::cout << "data_hash_benchmark files_found=" << report.files_found
                      << " folders_found=" << report.folders_found
                      << " files_read=" << report.files_read
                      << " files_failed=" << report.files_failed
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " bytes_read=" << report.bytes_read
                      << " bytes_hashed=" << report.bytes_hashed
                      << " read_bytes_per_second=" << report.read_bytes_per_second
                      << " read_gigabits_per_second=" << report.read_gigabits_per_second
                      << " hash_bytes_per_second=" << report.hash_bytes_per_second
                      << " hash_gigabits_per_second=" << report.hash_gigabits_per_second
                      << " hash_algorithm=" << report.hash_algorithm
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " data_reader_threads=" << report.data_reader_threads
                      << " data_outstanding_requests=" << report.data_outstanding_requests
                      << " hash_worker_threads=" << report.hash_worker_threads
                      << " hash_work_factor=" << report.hash_work_factor
                      << " max_files_queued=" << report.max_files_queued
                      << " data_buffer_slots=" << report.data_buffer_slots
                      << " data_queue_depth=" << report.data_queue_depth
                      << " meta_async=" << (report.meta_reader_async ? "true" : "false")
                      << " data_async=" << (report.data_reader_async ? "true" : "false")
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        print_usage();
        return 1;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
