#!/usr/bin/env python3

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import platform
import shutil
import shlex
import socket
import subprocess
import sys
import typing


def parse_csv_u64(value: str) -> list[int]:
    result: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        parsed = int(part)
        if parsed <= 0:
            raise ValueError(f"parallelism values must be positive: {value}")
        result.append(parsed)
    if not result:
        raise ValueError(f"no values provided: {value}")
    return result


def parse_report_fields(stdout: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for token in stdout.replace("\n", " ").split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key.strip()] = value.strip().strip('"')
    return fields


def run_command(args: list[str], timeout_seconds: float = 0.0, allow_failure: bool = False) -> dict[str, typing.Any]:
    try:
        completed = subprocess.run(args,
                                   capture_output=True,
                                   text=True,
                                   timeout=timeout_seconds if timeout_seconds > 0.0 else None)
    except subprocess.TimeoutExpired as exc:
        return {
            "args": args,
            "stdout": (exc.stdout or "").strip() if isinstance(exc.stdout, str) else "",
            "stderr": (exc.stderr or "").strip() if isinstance(exc.stderr, str) else "",
            "fields": {},
            "returncode": -1,
            "timed_out": True,
        }
    fields = parse_report_fields(completed.stdout)
    if completed.returncode != 0 and not allow_failure:
        raise RuntimeError(
            "command failed\n"
            f"args={shlex.join(args)}\n"
            f"stdout={completed.stdout}\n"
            f"stderr={completed.stderr}"
        )
    return {
        "args": args,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "fields": fields,
        "returncode": completed.returncode,
        "timed_out": False,
        "failed": completed.returncode != 0,
    }


def field_float(fields: dict[str, str], *names: str) -> float:
    for name in names:
        if name in fields:
            return float(fields[name])
    return 0.0


def field_int(fields: dict[str, str], name: str) -> int:
    return int(float(fields.get(name, "0")))


def machine_info() -> dict[str, typing.Any]:
    info: dict[str, typing.Any] = {
        "host": socket.gethostname(),
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "platform": platform.platform(),
        "hardware_threads": os.cpu_count() or 0,
        "numa_nodes": 0,
        "cpu": "",
    }
    try:
        lscpu = subprocess.run(["lscpu"], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        lscpu = None
    if lscpu is not None and lscpu.returncode == 0:
        for line in lscpu.stdout.splitlines():
            if line.startswith("Model name:"):
                info["cpu"] = line.split(":", 1)[1].strip()
            elif line.startswith("NUMA node(s):"):
                try:
                    info["numa_nodes"] = int(line.split(":", 1)[1].strip())
                except ValueError:
                    pass
    return info


def transport_row(command_result: dict[str, typing.Any], extra: dict[str, typing.Any]) -> dict[str, typing.Any]:
    fields = command_result["fields"]
    row: dict[str, typing.Any] = {
        **extra,
        "status": "timeout" if command_result.get("timed_out") else "ok",
        "gb_s": field_float(fields, "GB_per_second"),
        "gib_s": field_float(fields, "GiB_per_second"),
        "gbit_s": field_float(fields, "Gbit_per_second"),
        "buffers_s": field_float(fields, "bytes_per_second") / max(field_int(fields, "buffer_size"), 1),
        "buffers_generated": field_int(fields, "buffers_generated"),
        "buffers_received": field_int(fields, "buffers_received"),
        "elapsed_s": field_float(fields, "elapsed_s"),
    }
    return row


def hash_row(command_result: dict[str, typing.Any], extra: dict[str, typing.Any]) -> dict[str, typing.Any]:
    fields = command_result["fields"]
    return {
        **extra,
        "status": "timeout" if command_result.get("timed_out") else "ok",
        "gb_s": field_float(fields, "bytes_per_second") / 1_000_000_000.0,
        "gbit_s": field_float(fields, "gigabits_per_second"),
        "bytes_hashed": field_int(fields, "bytes_hashed"),
        "elapsed_s": field_float(fields, "elapsed_s"),
    }


def metadata_writer_row(command_result: dict[str, typing.Any], extra: dict[str, typing.Any]) -> dict[str, typing.Any]:
    fields = command_result["fields"]
    if command_result.get("failed"):
        status = "failed"
    elif command_result.get("timed_out"):
        status = "timeout"
    else:
        status = "ok"
    return {
        **extra,
        "status": status,
        "records_s": field_float(fields, "records_per_second"),
        "files_s": field_float(fields, "files_per_second"),
        "records_written": field_int(fields, "records_written"),
        "files_generated": field_int(fields, "files_generated"),
        "folders_generated": field_int(fields, "folders_generated"),
        "elapsed_s": field_float(fields, "elapsed_s"),
    }


def run_transport(app: pathlib.Path,
                  transport: str,
                  pattern: str,
                  transports: int,
                  buffers_per_transport: int,
                  buffer_size: int,
                  pool_slots: int,
                  generator_threads: int,
                  sender_threads: int = 1,
                  receiver_threads: int = 1,
                  discarder_threads: int = 1,
                  shared_input: bool = False,
                  base_port: int = 39000,
                  timeout_seconds: float = 0.0) -> dict[str, typing.Any]:
    args = [
        str(app),
        "benchmark-transport",
        "--transport",
        transport,
        "--pattern",
        pattern,
        "--transports",
        str(transports),
        "--buffers-per-transport",
        str(buffers_per_transport),
        "--buffer-size",
        str(buffer_size),
        "--pool-slots",
        str(pool_slots),
        "--generator-threads",
        str(generator_threads),
        "--sender-threads",
        str(sender_threads),
        "--receiver-threads",
        str(receiver_threads),
        "--discarder-threads",
        str(discarder_threads),
        "--base-port",
        str(base_port),
    ]
    if shared_input:
        args.append("--shared-input")
    return run_command(args, timeout_seconds)


def run_hash(app: pathlib.Path,
             algorithm: str,
             threads: int,
             block_size: int,
             duration_seconds: float,
             timeout_seconds: float = 0.0) -> dict[str, typing.Any]:
    return run_command([
        str(app),
        "benchmark-hash",
        "--hash",
        algorithm,
        "--threads",
        str(threads),
        "--block-size",
        str(block_size),
        "--duration-seconds",
        str(duration_seconds),
    ], timeout_seconds)


def run_metadata_writer(app: pathlib.Path,
                        output_path: pathlib.Path,
                        output_format: str,
                        file_count: int,
                        folder_count: int,
                        batch_size: int,
                        average_file_size: int,
                        partitions: int,
                        partition_mode: str,
                        duckdb_memory_limit: str,
                        duckdb_threads: int,
                        duckdb_checkpoint_threshold: str,
                        parquet_compression: str,
                        timeout_seconds: float = 0.0) -> dict[str, typing.Any]:
    args = [
        str(app),
        "benchmark-metadata-writer",
        "--output",
        str(output_path),
        "--output-format",
        output_format,
        "--file-count",
        str(file_count),
        "--folder-count",
        str(folder_count),
        "--batch-size",
        str(batch_size),
        "--average-file-size",
        str(average_file_size),
        "--partitions",
        str(partitions),
        "--partition-mode",
        partition_mode,
    ]
    if duckdb_memory_limit:
        args.extend(["--duckdb-memory-limit", duckdb_memory_limit])
    if duckdb_threads > 0:
        args.extend(["--duckdb-threads", str(duckdb_threads)])
    if duckdb_checkpoint_threshold:
        args.extend(["--duckdb-checkpoint-threshold", duckdb_checkpoint_threshold])
    if parquet_compression:
        args.extend(["--parquet-compression", parquet_compression])
    return run_command(args, timeout_seconds, allow_failure=True)


def add_table(lines: list[str], headers: list[str], rows: list[dict[str, typing.Any]]) -> None:
    widths = [len(header) for header in headers]
    rendered_rows: list[list[str]] = []
    for row in rows:
        rendered: list[str] = []
        for header in headers:
            value = row.get(header, "")
            if isinstance(value, float):
                text = f"{value:.3f}"
            else:
                text = str(value)
            rendered.append(text)
        rendered_rows.append(rendered)
        widths = [max(width, len(text)) for width, text in zip(widths, rendered)]

    lines.append("  ".join(header.ljust(width) for header, width in zip(headers, widths)))
    lines.append("  ".join("-" * width for width in widths))
    for row in rendered_rows:
        lines.append("  ".join(text.rjust(width) if text.replace(".", "", 1).isdigit() else text.ljust(width)
                               for text, width in zip(row, widths)))


def render_text(report: dict[str, typing.Any]) -> str:
    lines: list[str] = []
    machine = report["machine"]
    config = report["config"]
    lines.append("wsync_job_performance")
    lines.append(f"host={machine['host']}")
    lines.append(f"timestamp_utc={machine['timestamp_utc']}")
    lines.append(f"cpu=\"{machine.get('cpu', '')}\"")
    lines.append(f"hardware_threads={machine['hardware_threads']}")
    lines.append(f"numa_nodes={machine['numa_nodes']}")
    lines.append(f"buffer_size={config['buffer_size']}")
    lines.append(f"pool_slots={config['pool_slots']}")
    lines.append(f"build={config['build']}")
    lines.append("")

    for benchmark in report["benchmarks"]:
        lines.append(f"job={benchmark['job']}")
        for key in ("pattern", "transport", "pipeline", "queue_topology", "units"):
            if key in benchmark:
                lines.append(f"{key}={benchmark[key]}")
        add_table(lines, benchmark["headers"], benchmark["rows"])
        lines.append("")
    return "\n".join(lines)


def build_report(args: argparse.Namespace) -> dict[str, typing.Any]:
    app = pathlib.Path(args.app).resolve()
    if not app.exists():
        raise FileNotFoundError(f"application binary not found: {app}")

    report: dict[str, typing.Any] = {
        "machine": machine_info(),
        "config": {
            "buffer_size": args.buffer_size,
            "pool_slots": args.pool_slots,
            "buffers_per_lane": args.buffers_per_lane,
            "hash_duration_seconds": args.hash_duration_seconds,
            "build": args.build_label,
        },
        "benchmarks": [],
    }

    generator_rows: list[dict[str, typing.Any]] = []
    for lanes in args.parallelism:
        result = run_transport(app,
                               "none",
                               args.pattern,
                               lanes,
                               args.buffers_per_lane,
                               args.buffer_size,
                               args.pool_slots,
                               1,
                               discarder_threads=1,
                               timeout_seconds=args.command_timeout_seconds)
        generator_rows.append(transport_row(result, {
            "threads": lanes,
            "queues": lanes,
            "consumers": lanes,
        }))
    report["benchmarks"].append({
        "job": "BufferGeneratorJob",
        "pattern": args.pattern,
        "pipeline": "BufferGeneratorJob -> ShardedBufQueue -> BufferDiscarderJob",
        "queue_topology": "sharded_lanes",
        "units": "GB/s,Gbit/s,buffers/s",
        "headers": ["threads", "queues", "consumers", "status", "gb_s", "gbit_s", "buffers_s"],
        "rows": generator_rows,
    })

    discarder_rows: list[dict[str, typing.Any]] = []
    for discarder_threads in args.discarder_threads:
        result = run_transport(app,
                               "none",
                               args.pattern,
                               discarder_threads,
                               args.buffers_per_lane,
                               args.buffer_size,
                               args.pool_slots,
                               1,
                               discarder_threads=1,
                               timeout_seconds=args.command_timeout_seconds)
        discarder_rows.append(transport_row(result, {
            "generator_threads": discarder_threads,
            "discarder_threads": discarder_threads,
            "queues": discarder_threads,
        }))
    report["benchmarks"].append({
        "job": "BufferDiscarderJob",
        "pattern": args.pattern,
        "pipeline": "BufferGeneratorJob -> ShardedBufQueue -> BufferDiscarderJob",
        "queue_topology": "sharded_lanes",
        "units": "GB/s,Gbit/s,buffers/s",
        "headers": ["generator_threads", "discarder_threads", "queues", "status", "gb_s", "gbit_s", "buffers_s"],
        "rows": discarder_rows,
    })

    for transport in args.transports:
        rows: list[dict[str, typing.Any]] = []
        for endpoints in args.endpoints:
            generator_threads = max(1, args.transport_generator_threads // endpoints)
            result = run_transport(app,
                                   transport,
                                   args.pattern,
                                   endpoints,
                                   args.transport_buffers_total // endpoints,
                                   args.buffer_size,
                                   args.pool_slots,
                                   generator_threads,
                                   sender_threads=1,
                                   receiver_threads=1,
                                   discarder_threads=1,
                                   base_port=args.base_port,
                                   timeout_seconds=args.command_timeout_seconds)
            rows.append(transport_row(result, {
                "endpoints": endpoints,
                "streams": endpoints,
                "generator_threads_total": generator_threads * endpoints,
            }))
        report["benchmarks"].append({
            "job": "BufferSenderJob+BufferReceiverJob",
            "transport": transport,
            "pattern": args.pattern,
            "pipeline": f"BufferGeneratorJob -> BufferSenderJob -> {transport} -> BufferReceiverJob -> BufferDiscarderJob",
            "queue_topology": "sharded",
            "units": "GB/s,Gbit/s,buffers/s",
            "headers": ["endpoints", "streams", "generator_threads_total", "status", "gb_s", "gbit_s", "buffers_s"],
            "rows": rows,
        })

    hash_rows: list[dict[str, typing.Any]] = []
    for algorithm in args.hash_algorithms:
        for threads in args.hash_threads:
            result = run_hash(app,
                              algorithm,
                              threads,
                              args.hash_block_size,
                              args.hash_duration_seconds,
                              timeout_seconds=args.command_timeout_seconds)
            hash_rows.append(hash_row(result, {
                "algorithm": algorithm,
                "threads": threads,
            }))
    report["benchmarks"].append({
        "job": "HashSpeed",
        "pipeline": "CPU hash benchmark",
        "units": "GB/s,Gbit/s",
        "headers": ["algorithm", "threads", "status", "gb_s", "gbit_s"],
        "rows": hash_rows,
    })

    writer_output_dir = pathlib.Path(args.writer_output_dir)
    if writer_output_dir.exists() and args.clean_writer_output:
        shutil.rmtree(writer_output_dir)
    writer_output_dir.mkdir(parents=True, exist_ok=True)
    writer_rows: list[dict[str, typing.Any]] = []
    writer_extensions = {"text": "txt", "csv": "csv", "parquet": "parquet"}
    for output_format in args.writer_formats:
        for partition_mode in args.writer_partition_modes:
            for partitions in args.writer_partitions:
                extension = writer_extensions.get(output_format, output_format)
                output_name = f"{output_format}-{partition_mode}-{partitions}"
                output_path = writer_output_dir / (
                    output_name if partitions > 1 else f"{output_name}.{extension}")
                if output_path.exists() and args.clean_writer_output:
                    if output_path.is_dir():
                        shutil.rmtree(output_path)
                    else:
                        output_path.unlink()
                result = run_metadata_writer(app,
                                             output_path,
                                             output_format,
                                             args.writer_file_count,
                                             args.writer_folder_count,
                                             args.writer_batch_size,
                                             args.writer_average_file_size,
                                             partitions,
                                             partition_mode,
                                             args.duckdb_memory_limit,
                                             args.duckdb_threads,
                                             args.duckdb_checkpoint_threshold,
                                             args.parquet_compression,
                                             timeout_seconds=args.command_timeout_seconds)
                writer_rows.append(metadata_writer_row(result, {
                    "format": output_format,
                    "partition_mode": partition_mode,
                    "partitions": partitions,
                }))
    report["benchmarks"].append({
        "job": "MetadataRecordWriterJob",
        "pipeline": "FileMetadataGenerator -> MetadataRecordWriter",
        "units": "records/s,files/s",
        "headers": ["format", "partition_mode", "partitions", "status", "records_s", "files_s", "records_written", "elapsed_s"],
        "rows": writer_rows,
    })

    return report


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Produce per-host WSync job performance report")
    parser.add_argument("--app", default="build/hypersync")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--output-text", default="")
    parser.add_argument("--build-label", default="local")
    parser.add_argument("--buffer-size", type=int, default=1024 * 1024)
    parser.add_argument("--pool-slots", type=int, default=256)
    parser.add_argument("--buffers-per-lane", type=int, default=256)
    parser.add_argument("--parallelism", type=parse_csv_u64, default=parse_csv_u64("1,2,4"))
    parser.add_argument("--discarder-threads", type=parse_csv_u64, default=parse_csv_u64("1,4,8"))
    parser.add_argument("--discarder-generator-threads", type=int, default=4)
    parser.add_argument("--endpoints", type=parse_csv_u64, default=parse_csv_u64("1,2"))
    parser.add_argument("--transports", type=lambda value: [item.strip() for item in value.split(",") if item.strip()],
                        default=["unix", "tcp"])
    parser.add_argument("--transport-generator-threads", type=int, default=4)
    parser.add_argument("--transport-buffers-total", type=int, default=256)
    parser.add_argument("--base-port", type=int, default=53000)
    parser.add_argument("--pattern", default="xoshiro256")
    parser.add_argument("--hash-algorithms", type=lambda value: [item.strip() for item in value.split(",") if item.strip()],
                        default=["xxh64", "sha256"])
    parser.add_argument("--hash-threads", type=parse_csv_u64, default=parse_csv_u64("1"))
    parser.add_argument("--hash-block-size", type=int, default=1024 * 1024)
    parser.add_argument("--hash-duration-seconds", type=float, default=0.2)
    parser.add_argument("--writer-formats", type=lambda value: [item.strip() for item in value.split(",") if item.strip()],
                        default=["csv", "parquet"])
    parser.add_argument("--writer-partition-modes", type=lambda value: [item.strip() for item in value.split(",") if item.strip()],
                        default=["threads"])
    parser.add_argument("--writer-partitions", type=parse_csv_u64, default=parse_csv_u64("1"))
    parser.add_argument("--writer-file-count", type=int, default=100_000)
    parser.add_argument("--writer-folder-count", type=int, default=1_000)
    parser.add_argument("--writer-batch-size", type=int, default=65_536)
    parser.add_argument("--writer-average-file-size", type=int, default=32 * 1024)
    parser.add_argument("--writer-output-dir", default="build/job-performance-writer-output")
    parser.add_argument("--clean-writer-output", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--duckdb-memory-limit", default="")
    parser.add_argument("--duckdb-threads", type=int, default=0)
    parser.add_argument("--duckdb-checkpoint-threshold", default="")
    parser.add_argument("--parquet-compression", default="zstd")
    parser.add_argument("--command-timeout-seconds", type=float, default=120.0)
    parsed = parser.parse_args(argv)

    report = build_report(parsed)
    text = render_text(report)
    print(text)

    if parsed.output_json:
        output_json = pathlib.Path(parsed.output_json)
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if parsed.output_text:
        output_text = pathlib.Path(parsed.output_text)
        output_text.parent.mkdir(parents=True, exist_ok=True)
        output_text.write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
