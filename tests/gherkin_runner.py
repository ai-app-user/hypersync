#!/usr/bin/env python3

from __future__ import annotations

import dataclasses
import csv
import hashlib
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import typing


@dataclasses.dataclass
class Step:
    keyword: str
    text: str
    table: list[dict[str, str]]


@dataclasses.dataclass
class Scenario:
    feature_name: str
    name: str
    steps: list[Step]
    path: pathlib.Path


@dataclasses.dataclass
class CommandResult:
    name: str
    args: list[str]
    returncode: int
    stdout: str
    stderr: str
    elapsed_seconds: float
    report_fields: dict[str, str]


class StepFailure(RuntimeError):
    pass


class SkipScenario(RuntimeError):
    pass


class ScopedNfsExport:
    def __init__(self, export_path: pathlib.Path) -> None:
        self.export_path = export_path
        self.export_host = "127.0.0.1"
        self.active = False
        self.stop_rpcbind_on_teardown = False
        self.stop_nfs_server_on_teardown = False

    def __enter__(self) -> "ScopedNfsExport":
        missing = [tool for tool in ("systemctl", "sudo", "exportfs", "showmount") if shutil.which(tool) is None]
        if missing:
            raise SkipScenario(f"loopback NFS export tools are unavailable: {', '.join(missing)}")
        if not command_succeeds(["sudo", "-n", "true"], check=False):
            raise SkipScenario("passwordless sudo is unavailable for loopback NFS export")

        self.stop_rpcbind_on_teardown = not command_succeeds(
            ["systemctl", "-q", "is-active", "rpcbind"], check=False
        )
        self.stop_nfs_server_on_teardown = not command_succeeds(
            ["systemctl", "-q", "is-active", "nfs-server"], check=False
        )
        command_succeeds(["sudo", "-n", "systemctl", "start", "rpcbind", "nfs-server"])
        command_succeeds(
            [
                "sudo",
                "-n",
                "exportfs",
                "-i",
                "-o",
                "rw,sync,no_subtree_check,no_root_squash,insecure",
                f"{self.export_host}:{self.export_path}",
            ]
        )
        showmount = subprocess.run(
            ["showmount", "-e", self.export_host],
            check=True,
            capture_output=True,
            text=True,
        )
        if str(self.export_path) not in showmount.stdout:
            self.__exit__(None, None, None)
            raise StepFailure(f"NFS export {self.export_path} did not become visible")
        self.active = True
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self.active:
            return
        subprocess.run(
            ["sudo", "-n", "exportfs", "-u", f"{self.export_host}:{self.export_path}"],
            check=False,
            capture_output=True,
            text=True,
        )
        if self.stop_nfs_server_on_teardown:
            subprocess.run(
                ["sudo", "-n", "systemctl", "stop", "nfs-server"],
                check=False,
                capture_output=True,
                text=True,
            )
        if self.stop_rpcbind_on_teardown:
            subprocess.run(
                ["sudo", "-n", "systemctl", "stop", "rpcbind.service", "rpcbind.socket"],
                check=False,
                capture_output=True,
                text=True,
            )
        self.active = False

    @property
    def url(self) -> str:
        return f"nfs://{self.export_host}{self.export_path}"


class ScenarioWorld:
    def __init__(self, scenario: Scenario) -> None:
        self.scenario = scenario
        self.repo_root = find_repo_root(pathlib.Path(__file__).resolve())
        self.app = self.repo_root / "build" / "hypersync"
        self.temp_root = pathlib.Path(tempfile.mkdtemp(prefix="hypersync_gherkin_"))
        self.logs_dir = self.temp_root / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.coverage_dir = self.temp_root / "coverage"
        self.coverage_dir.mkdir(parents=True, exist_ok=True)
        self.source_dir: pathlib.Path | None = None
        self.target_dir: pathlib.Path | None = None
        self.target_arg: str | None = None
        self.target_export: ScopedNfsExport | None = None
        self.results: list[CommandResult] = []
        self.last_scan_csv: pathlib.Path | None = None
        self.last_diff_csv: pathlib.Path | None = None
        self.last_hash_csv: pathlib.Path | None = None
        self.receiver_use_sudo = False

    def cleanup(self) -> None:
        if self.target_export is not None:
            self.target_export.__exit__(None, None, None)
            self.target_export = None
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def ensure_source_dir(self) -> pathlib.Path:
        if self.source_dir is None:
            self.source_dir = self.temp_root / "source"
            self.source_dir.mkdir(parents=True, exist_ok=True)
        return self.source_dir

    def ensure_target_dir(self) -> pathlib.Path:
        if self.target_dir is None:
            self.target_dir = self.temp_root / "target"
            self.target_dir.mkdir(parents=True, exist_ok=True)
        return self.target_dir

    def gcov_env(self, label: str) -> dict[str, str]:
        env = os.environ.copy()
        prefix = self.coverage_dir / label
        prefix.mkdir(parents=True, exist_ok=True)
        env["GCOV_PREFIX"] = str(prefix)
        env["GCOV_PREFIX_STRIP"] = "0"
        return env

    def run_command(self, name: str, args: list[str], env: dict[str, str] | None = None) -> CommandResult:
        start = time.monotonic()
        completed = subprocess.run(args, capture_output=True, text=True, env=env, cwd=self.repo_root)
        elapsed = time.monotonic() - start
        result = CommandResult(
            name=name,
            args=args,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            elapsed_seconds=elapsed,
            report_fields=parse_report_fields(completed.stdout),
        )
        self.results.append(result)
        return result

    def launch_receiver(self, priority_port: int, data_port: int) -> tuple[subprocess.Popen[str], pathlib.Path]:
        if self.target_arg is None:
            raise StepFailure("target is not configured before transfer")

        args = [
            str(self.app),
            "receive",
            "--target",
            self.target_arg,
            "--bind-host",
            "127.0.0.1",
            "--priority-port",
            str(priority_port),
            "--data-port",
            str(data_port),
        ]
        env = self.gcov_env(f"receiver_{len(self.results)}")
        if self.receiver_use_sudo:
            args = ["sudo", "-n", "env", f"GCOV_PREFIX={env['GCOV_PREFIX']}", "GCOV_PREFIX_STRIP=0", *args]
            env = None

        log_path = self.logs_dir / f"receiver_{len(self.results)}.log"
        log_file = log_path.open("w", encoding="utf-8")
        process = subprocess.Popen(args, stdout=log_file, stderr=log_file, text=True, cwd=self.repo_root, env=env)
        time.sleep(0.3)
        log_file.close()
        return process, log_path

    def run_transfer(self, cache_threshold: int | None = None, command: str = "send") -> CommandResult:
        if self.source_dir is None:
            raise StepFailure("source tree is not configured before transfer")
        priority_port = pick_unused_port()
        data_port = pick_unused_port(exclude={priority_port})
        receiver, receiver_log = self.launch_receiver(priority_port, data_port)
        args = [
            str(self.app),
            command,
            "--source",
            str(self.source_dir),
            "--host",
            "127.0.0.1",
            "--priority-port",
            str(priority_port),
            "--data-port",
            str(data_port),
        ]
        if cache_threshold is not None:
            cache_path = self.temp_root / f"cache_{len(self.results)}"
            cache_path.mkdir(parents=True, exist_ok=True)
            args.extend(["--cache-path", str(cache_path), "--cache-threshold", str(cache_threshold)])

        result = self.run_command("transfer", args, env=self.gcov_env(f"sender_{len(self.results)}"))
        try:
            receiver_returncode = receiver.wait(timeout=20.0)
        except subprocess.TimeoutExpired as exc:
            receiver.kill()
            raise StepFailure(f"receiver did not exit in time: {receiver_log.read_text(encoding='utf-8')}") from exc
        receiver_output = receiver_log.read_text(encoding="utf-8")
        if result.returncode != 0:
            raise StepFailure(f"sender failed with {result.returncode}: {result.stdout}{result.stderr}")
        if receiver_returncode != 0:
            raise StepFailure(f"receiver failed with {receiver_returncode}: {receiver_output}")
        return result

    def run_scan(self) -> CommandResult:
        if self.source_dir is None:
            raise StepFailure("source tree is not configured before scan")
        self.last_scan_csv = self.temp_root / f"scan_{len(self.results)}.csv"
        return self.run_command(
            "scan",
            [str(self.app), "scan", "--source", str(self.source_dir), "--output", str(self.last_scan_csv)],
            env=self.gcov_env(f"scan_{len(self.results)}"),
        )

    def run_diff(self, compare_mode: str = "size") -> CommandResult:
        if self.source_dir is None:
            raise StepFailure("source tree is not configured before diff")
        if self.target_arg is None:
            raise StepFailure("target is not configured before diff")
        self.last_diff_csv = self.temp_root / f"diff_{len(self.results)}.csv"
        return self.run_command(
            "diff",
            [
                str(self.app),
                "diff",
                "--source",
                str(self.source_dir),
                "--target",
                self.target_arg,
                "--compare",
                compare_mode,
                "--output",
                str(self.last_diff_csv),
            ],
            env=self.gcov_env(f"diff_{len(self.results)}"),
        )

    def run_hash(self, algorithm: str, mode: str = "file", block_size: int | None = None) -> CommandResult:
        if self.source_dir is None:
            raise StepFailure("source tree is not configured before hash")
        self.last_hash_csv = self.temp_root / f"hash_{len(self.results)}.csv"
        args = [
            str(self.app),
            "hash",
            "--source",
            str(self.source_dir),
            "--output",
            str(self.last_hash_csv),
            "--output-format",
            "csv",
            "--records",
            "all",
            "--hash",
            algorithm,
            "--hash-mode",
            mode,
            "--meta-reader-threads",
            "1",
            "--metadata-async-depth",
            "4",
            "--data-reader-threads",
            "2",
            "--data-outstanding-requests",
            "4",
            "--hash-threads",
            "4",
            "--max-hash-chunks-queued",
            "32",
            "--max-files-queued",
            "4",
            "--max-duration-seconds",
            "10",
        ]
        if block_size is not None:
            args.extend(["--hash-block-size", str(block_size)])
        return self.run_command(
            "hash",
            args,
            env=self.gcov_env(f"hash_{len(self.results)}"),
        )

    def run_buffer_transport_benchmark(self, options: dict[str, str]) -> CommandResult:
        args = [str(self.app), "benchmark-transport"]
        option_map = {
            "transport": "--transport",
            "pattern": "--pattern",
            "transports": "--transports",
            "buffers_per_transport": "--buffers-per-transport",
            "buffer_size": "--buffer-size",
            "pool_slots": "--pool-slots",
            "generator_threads": "--generator-threads",
            "sender_threads": "--sender-threads",
            "receiver_threads": "--receiver-threads",
            "discarder_threads": "--discarder-threads",
            "base_port": "--base-port",
        }
        for key, flag in option_map.items():
            if key in options and options[key] != "":
                args.extend([flag, options[key]])
        if options.get("shared_input", "").lower() in {"1", "true", "yes"}:
            args.append("--shared-input")
        return self.run_command(
            "benchmark-transport",
            args,
            env=self.gcov_env(f"benchmark_transport_{len(self.results)}"),
        )

    def run_hash_speed_benchmark(self, options: dict[str, str]) -> CommandResult:
        args = [str(self.app), "benchmark-hash"]
        option_map = {
            "hash": "--hash",
            "threads": "--threads",
            "block_size": "--block-size",
            "duration_seconds": "--duration-seconds",
            "min_gigabits_per_core": "--min-gigabits-per-core",
        }
        for key, flag in option_map.items():
            if key in options and options[key] != "":
                args.extend([flag, options[key]])
        return self.run_command(
            "benchmark-hash",
            args,
            env=self.gcov_env(f"benchmark_hash_{len(self.results)}"),
        )

    def run_pipeline_autoscale_data_benchmark(self) -> CommandResult:
        if self.source_dir is None:
            raise StepFailure("source tree is not configured before data benchmark")
        args = [
            str(self.app),
            "benchmark-data",
            "--source",
            str(self.source_dir),
            "--split-small-large",
            "--small-file-threshold-bytes",
            "4096",
            "--meta-reader-threads",
            "1",
            "--metadata-async-depth",
            "4",
            "--data-reader-threads",
            "1",
            "--small-data-reader-threads",
            "4",
            "--large-data-reader-threads",
            "4",
            "--large-data-outstanding-requests",
            "1",
            "--pipeline-autoscale",
            "--autoscale-interval-ms",
            "5",
            "--autoscale-profile",
            "gherkin_pipeline_autoscale",
            "--autoscale-settings",
            str(self.temp_root / "autoscale.yaml"),
            "--max-files-queued",
            "64",
            "--data-buffer-slots",
            "64",
            "--data-queue-depth",
            "32",
            "--max-duration-seconds",
            "10",
            "--stats-interval-seconds",
            "1",
        ]
        return self.run_command(
            "benchmark-data-pipeline-autoscale",
            args,
            env=self.gcov_env(f"benchmark_data_autoscale_{len(self.results)}"),
        )


StepHandler = typing.Callable[[ScenarioWorld, Step, re.Match[str]], None]
STEP_HANDLERS: list[tuple[re.Pattern[str], StepHandler]] = []


def step(pattern: str) -> typing.Callable[[StepHandler], StepHandler]:
    regex = re.compile(pattern)

    def register(func: StepHandler) -> StepHandler:
        STEP_HANDLERS.append((regex, func))
        return func

    return register


def command_succeeds(args: list[str], check: bool = True) -> bool:
    try:
        completed = subprocess.run(args, capture_output=True, text=True)
    except FileNotFoundError as exc:
        if check:
            raise StepFailure(f"command not found: {args[0]}") from exc
        return False
    if check and completed.returncode != 0:
        raise StepFailure(f"command failed: {' '.join(args)}\n{completed.stdout}{completed.stderr}")
    return completed.returncode == 0


def parse_report_fields(stdout: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for token in stdout.replace("\n", " ").split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key.strip()] = value.strip()
    return fields


def read_csv_rows(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as input_file:
        return list(csv.DictReader(input_file))


def pick_unused_port(exclude: set[int] | None = None) -> int:
    excluded = exclude or set()
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        if port not in excluded:
            return port


def stat_snapshot(path: pathlib.Path) -> os.stat_result:
    return path.stat()


def find_repo_root(start: pathlib.Path) -> pathlib.Path:
    current = start if start.is_dir() else start.parent
    for candidate in [current, *current.parents]:
        if (candidate / "Makefile").exists() and (candidate / "hypersync").is_dir():
            return candidate
    return pathlib.Path(__file__).resolve().parents[2]


def count_regular_files(root: pathlib.Path) -> int:
    return sum(1 for candidate in root.rglob("*") if candidate.is_file())


def parse_table(lines: list[str]) -> list[dict[str, str]]:
    if not lines:
        return []
    headers = [part.strip() for part in lines[0].strip().strip("|").split("|")]
    rows: list[dict[str, str]] = []
    for line in lines[1:]:
        values = [part.strip() for part in line.strip().strip("|").split("|")]
        if len(values) != len(headers):
            raise StepFailure(f"table row has {len(values)} cells but expected {len(headers)}: {line}")
        rows.append(dict(zip(headers, values)))
    return rows


def parse_feature(path: pathlib.Path) -> list[Scenario]:
    feature_name = path.stem
    scenarios: list[Scenario] = []
    current: Scenario | None = None
    lines = path.read_text(encoding="utf-8").splitlines()
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()
        if not stripped or stripped.startswith("#"):
            index += 1
            continue
        if stripped.startswith("Feature:"):
            feature_name = stripped.split(":", 1)[1].strip()
            index += 1
            continue
        if stripped.startswith("Scenario:"):
            current = Scenario(feature_name=feature_name,
                               name=stripped.split(":", 1)[1].strip(),
                               steps=[],
                               path=path)
            scenarios.append(current)
            index += 1
            continue

        match = re.match(r"^(Given|When|Then|And|But)\s+(.*)$", stripped)
        if match is None:
            raise StepFailure(f"unsupported line in {path}: {lines[index]}")
        if current is None:
            raise StepFailure(f"step declared before scenario in {path}: {lines[index]}")

        table_lines: list[str] = []
        index += 1
        while index < len(lines) and lines[index].strip().startswith("|"):
            table_lines.append(lines[index])
            index += 1
        current.steps.append(Step(keyword=match.group(1), text=match.group(2), table=parse_table(table_lines)))
    return scenarios


def apply_mode_and_mtime(path: pathlib.Path, mode_text: str | None, mtime_text: str | None) -> None:
    if mode_text:
        os.chmod(path, int(mode_text, 8))
    if mtime_text:
        mtime_ns = int(mtime_text)
        os.utime(path, ns=(mtime_ns, mtime_ns))


def build_source_tree(world: ScenarioWorld, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise StepFailure("source tree table must not be empty")

    source_dir = world.ensure_source_dir()
    directories: list[dict[str, str]] = []
    files: list[dict[str, str]] = []
    for row in rows:
        entry_type = row.get("type", "file")
        if entry_type == "dir":
            directories.append(row)
        elif entry_type == "file":
            files.append(row)
        else:
            raise StepFailure(f"unsupported entry type: {entry_type}")

    for row in directories:
        (source_dir / row["path"]).mkdir(parents=True, exist_ok=True)

    for row in files:
        target = source_dir / row["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(row.get("content", ""), encoding="utf-8")
        apply_mode_and_mtime(target, row.get("mode"), row.get("mtime_ns"))

    directories.sort(key=lambda row: len(pathlib.PurePosixPath(row["path"]).parts), reverse=True)
    for row in directories:
        target = source_dir / row["path"]
        target.mkdir(parents=True, exist_ok=True)
        apply_mode_and_mtime(target, row.get("mode"), row.get("mtime_ns"))


def generated_content(index: int, size: int) -> str:
    seed = f"{index:08d}_"
    repeats = (size // len(seed)) + 1
    return (seed * repeats)[:size]


def last_result(world: ScenarioWorld) -> CommandResult:
    if not world.results:
        raise StepFailure("no commands have been executed yet")
    return world.results[-1]


def transfer_result(world: ScenarioWorld, ordinal: str) -> CommandResult:
    if ordinal == "latest":
        return last_result(world)
    if ordinal == "first":
        index = 0
    elif ordinal == "second":
        index = 1
    else:
        raise StepFailure(f"unsupported ordinal: {ordinal}")
    if index >= len(world.results):
        raise StepFailure(f"missing {ordinal} command result")
    return world.results[index]


def resolve_expected_identity(source_path: pathlib.Path, raw_value: str, kind: str) -> int:
    if raw_value == "source":
        stat_result = source_path.stat()
        return stat_result.st_uid if kind == "uid" else stat_result.st_gid
    return int(raw_value)


@step(r"^a source tree with entries:$")
def given_source_tree(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del match
    build_source_tree(world, step_data.table)


@step(r"^a generated source tree with (\d+) files of (\d+) bytes each$")
def given_generated_tree(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    file_count = int(match.group(1))
    file_size = int(match.group(2))
    rows = [
        {
            "type": "file",
            "path": f"group_{index // 16:02d}/file_{index:04d}.bin",
            "content": generated_content(index, file_size),
            "mode": "0644",
            "mtime_ns": str(1_700_100_000_000_000_000 + index * 1_000_000),
        }
        for index in range(file_count)
    ]
    build_source_tree(world, rows)


@step(r"^a local target directory$")
def given_local_target(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    target_dir = world.ensure_target_dir()
    world.target_arg = str(target_dir)


@step(r'^an exported NFS target with remote credentials "([^"]+)"$')
def given_exported_nfs_target(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    target_dir = world.ensure_target_dir()
    export = ScopedNfsExport(target_dir)
    world.target_export = export.__enter__()
    world.target_arg = f"{export.url}?{match.group(1)}"


@step(r"^I transfer the source tree via the CLI$")
def when_transfer_once(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    world.run_transfer()


@step(r"^I sync the source tree via the CLI$")
def when_sync_once(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    world.run_transfer(command="sync")


@step(r"^I transfer the source tree via the CLI twice$")
def when_transfer_twice(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    world.run_transfer()
    world.run_transfer()


@step(r"^I transfer the source tree via the CLI with cache threshold (\d+)$")
def when_transfer_with_cache(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    world.run_transfer(cache_threshold=int(match.group(1)))


@step(r"^I scan the source tree via the CLI$")
def when_scan(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    world.run_scan()


@step(r"^I diff the source and target via the CLI$")
def when_diff(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    world.run_diff()


@step(r"^I hash the source tree as CSV with ([a-zA-Z0-9_-]+)$")
def when_hash(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    world.run_hash(match.group(1))


@step(r"^I hash the source tree as CSV with ([a-zA-Z0-9_-]+) block hashes of size (\d+)$")
def when_hash_blocks(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    world.run_hash(match.group(1), mode="blocks", block_size=int(match.group(2)))


@step(r"^I run buffer transport benchmarks:$")
def when_buffer_transport_benchmarks(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del match
    for row in step_data.table:
        world.run_buffer_transport_benchmark(row)


@step(r"^I run hash speed benchmarks:$")
def when_hash_speed_benchmarks(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del match
    for row in step_data.table:
        world.run_hash_speed_benchmark(row)


@step(r"^I run a pipeline autoscale data benchmark$")
def when_pipeline_autoscale_data_benchmark(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    world.run_pipeline_autoscale_data_benchmark()


@step(r"^the latest command should succeed$")
def then_latest_succeeds(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    result = last_result(world)
    if result.returncode != 0:
        raise StepFailure(f"latest command failed with {result.returncode}: {result.stdout}{result.stderr}")


@step(r"^all benchmark commands should succeed$")
def then_all_benchmarks_succeed(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    if not world.results:
        raise StepFailure("no benchmark commands have been executed")
    for result in world.results:
        if result.returncode != 0:
            raise StepFailure(f"{result.name} failed with {result.returncode}: {result.stdout}{result.stderr}")
        if result.name.startswith("benchmark") and result.stdout.strip():
            print(f"[BENCH] {result.stdout.strip()}")


@step(r"^each benchmark should report at least ([0-9.]+) Gbit/s$")
def then_each_benchmark_reports_min_gbits(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    minimum = float(match.group(1))
    if not world.results:
        raise StepFailure("no benchmark commands have been executed")
    for result in world.results:
        value_text = result.report_fields.get("Gbit_per_second") or result.report_fields.get("gigabits_per_second")
        if value_text is None:
            raise StepFailure(f"{result.name} did not report Gbit/s: {result.stdout}")
        observed = float(value_text)
        if observed < minimum:
            raise StepFailure(f"{result.name} reported {observed:.6f} Gbit/s below {minimum:.6f}: {result.stdout}")


@step(r"^each buffer transport benchmark should move all generated buffers$")
def then_transport_benchmarks_move_all_buffers(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    for result in world.results:
        if result.name != "benchmark-transport":
            continue
        fields = result.report_fields
        generated = fields.get("buffers_generated")
        sent = fields.get("buffers_sent")
        received = fields.get("buffers_received")
        discarded = fields.get("buffers_discarded")
        if len({generated, sent, received, discarded}) != 1:
            raise StepFailure(f"buffer counts diverged: {result.stdout}")


@step(r"^the (latest|first|second) transfer should report \"([^\"]+)\"$")
def then_transfer_report_contains(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    result = transfer_result(world, match.group(1))
    fragment = match.group(2)
    if fragment not in result.stdout:
        raise StepFailure(f"expected '{fragment}' in transfer output: {result.stdout}")


@step(r"^the latest command should report \"([^\"]+)\"$")
def then_latest_report_contains(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    result = last_result(world)
    fragment = match.group(1)
    if fragment not in result.stdout:
        raise StepFailure(f"expected '{fragment}' in command output: {result.stdout}")


@step(r"^the latest transfer should send fewer chunks than files$")
def then_transfer_packs_chunks(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data, match
    result = last_result(world)
    chunks_sent = int(result.report_fields.get("chunks_sent", "0"))
    files_total = int(result.report_fields.get("files_total", "0"))
    if files_total <= 0:
        raise StepFailure(f"transfer did not report files_total: {result.stdout}")
    if chunks_sent >= files_total:
        raise StepFailure(f"expected chunks_sent < files_total, saw {chunks_sent} >= {files_total}: {result.stdout}")


@step(r"^the (latest|first|second) command should finish within ([0-9.]+) seconds$")
def then_command_finishes_quickly(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    ordinal = match.group(1)
    result = last_result(world) if ordinal == "latest" else transfer_result(world, ordinal)
    budget = float(match.group(2))
    if result.elapsed_seconds > budget:
        raise StepFailure(f"command took {result.elapsed_seconds:.3f}s but budget was {budget:.3f}s")


@step(r"^the scan output should contain (\d+) rows$")
def then_scan_row_count(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    if world.last_scan_csv is None or not world.last_scan_csv.exists():
        raise StepFailure("scan output file was not created")
    line_count = len(world.last_scan_csv.read_text(encoding="utf-8").splitlines())
    rows = max(line_count - 1, 0)
    expected = int(match.group(1))
    if rows != expected:
        raise StepFailure(f"expected {expected} scan rows but saw {rows}")


@step(r"^the diff output should contain (\d+) rows$")
def then_diff_row_count(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    if world.last_diff_csv is None or not world.last_diff_csv.exists():
        raise StepFailure("diff output file was not created")
    line_count = len(world.last_diff_csv.read_text(encoding="utf-8").splitlines())
    rows = max(line_count - 1, 0)
    expected = int(match.group(1))
    if rows != expected:
        raise StepFailure(f"expected {expected} diff rows but saw {rows}")


@step(r"^the hash output should match sha256 for:$")
def then_hash_output_matches_sha256(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del match
    if world.last_hash_csv is None or not world.last_hash_csv.exists():
        raise StepFailure("hash output file was not created")

    rows = read_csv_rows(world.last_hash_csv)
    file_rows = {row["rel_path"]: row for row in rows if row.get("record_type") == "file"}
    for expected in step_data.table:
        rel_path = expected["path"]
        row = file_rows.get(rel_path)
        if row is None:
            raise StepFailure(f"missing hash row for {rel_path}")
        expected_hash = hashlib.sha256(expected.get("content", "").encode("utf-8")).hexdigest()
        if row.get("hash_algorithm") != "sha256":
            raise StepFailure(f"expected sha256 algorithm for {rel_path}, saw {row.get('hash_algorithm')}")
        if row.get("content_hash") != expected_hash:
            raise StepFailure(f"hash mismatch for {rel_path}: expected {expected_hash}, saw {row.get('content_hash')}")


@step(r"^the hash output should contain md5 block hashes of size (\d+) for:$")
def then_hash_output_contains_md5_blocks(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    if world.last_hash_csv is None or not world.last_hash_csv.exists():
        raise StepFailure("hash output file was not created")

    block_size = int(match.group(1))
    rows = read_csv_rows(world.last_hash_csv)
    file_rows = {row["rel_path"]: row for row in rows if row.get("record_type") == "file"}
    for expected in step_data.table:
        rel_path = expected["path"]
        content = expected.get("content", "").encode("utf-8")
        row = file_rows.get(rel_path)
        if row is None:
            raise StepFailure(f"missing block hash row for {rel_path}")
        expected_blocks = [
            hashlib.md5(content[offset:offset + block_size]).hexdigest()
            for offset in range(0, len(content), block_size)
        ]
        if row.get("hash_block_size") != str(block_size):
            raise StepFailure(f"block size mismatch for {rel_path}: saw {row.get('hash_block_size')}")
        if row.get("hash_block_count") != str(len(expected_blocks)):
            raise StepFailure(f"block count mismatch for {rel_path}: saw {row.get('hash_block_count')}")
        if row.get("block_hash_algorithm") != "md5":
            raise StepFailure(f"expected md5 block hashes for {rel_path}, saw {row.get('block_hash_algorithm')}")
        if row.get("block_hashes") != ";".join(expected_blocks):
            raise StepFailure(f"block hashes mismatch for {rel_path}: saw {row.get('block_hashes')}")


@step(r"^the hash output should contain xxh3_64 hashes for:$")
def then_hash_output_contains_xxh3_64(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del match
    if world.last_hash_csv is None or not world.last_hash_csv.exists():
        raise StepFailure("hash output file was not created")

    rows = read_csv_rows(world.last_hash_csv)
    file_rows = {row["rel_path"]: row for row in rows if row.get("record_type") == "file"}
    for expected in step_data.table:
        rel_path = expected["path"]
        row = file_rows.get(rel_path)
        if row is None:
            raise StepFailure(f"missing hash row for {rel_path}")
        if row.get("hash_algorithm") != "xxh3_64":
            raise StepFailure(f"expected xxh3_64 algorithm for {rel_path}, saw {row.get('hash_algorithm')}")
        content_hash = row.get("content_hash", "")
        if len(content_hash) != 16 or not re.fullmatch(r"[0-9a-f]{16}", content_hash):
            raise StepFailure(f"expected 16 lowercase hex chars for {rel_path}, saw {content_hash}")


@step(r"^the hash output should contain folder metadata:$")
def then_hash_output_contains_folder_metadata(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del match
    if world.last_hash_csv is None or not world.last_hash_csv.exists():
        raise StepFailure("hash output file was not created")

    rows = read_csv_rows(world.last_hash_csv)
    folder_rows = {row["rel_path"]: row for row in rows if row.get("record_type") == "folder"}
    for expected in step_data.table:
        rel_path = expected["path"]
        row = folder_rows.get(rel_path)
        if row is None:
            raise StepFailure(f"missing folder row for {rel_path}")
        for column in ("flat_file_count", "flat_logical_size_bytes"):
            if row.get(column) != expected[column]:
                raise StepFailure(f"{column} mismatch for {rel_path}: expected {expected[column]}, saw {row.get(column)}")


@step(r"^the target should contain (\d+) files$")
def then_target_file_count(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    target_dir = world.ensure_target_dir()
    observed = count_regular_files(target_dir)
    expected = int(match.group(1))
    if observed != expected:
        raise StepFailure(f"expected {expected} files but saw {observed}")


@step(r"^the measured throughput should be at least ([0-9.]+) MiB/s$")
def then_throughput_budget(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del step_data
    result = last_result(world)
    bytes_transferred = int(result.report_fields.get("bytes", "0"))
    mib_per_second = (bytes_transferred / (1024.0 * 1024.0)) / max(result.elapsed_seconds, 1e-9)
    minimum = float(match.group(1))
    if mib_per_second < minimum:
        raise StepFailure(f"throughput {mib_per_second:.3f} MiB/s was below {minimum:.3f} MiB/s")


@step(r"^the target tree should match:$")
def then_target_tree_matches(world: ScenarioWorld, step_data: Step, match: re.Match[str]) -> None:
    del match
    target_dir = world.ensure_target_dir()
    source_dir = world.ensure_source_dir()
    for row in step_data.table:
        entry_type = row.get("type", "file")
        target_path = target_dir / row["path"]
        source_path = source_dir / row["path"]
        if not target_path.exists():
            raise StepFailure(f"missing target path: {target_path}")
        if entry_type == "file":
            if not target_path.is_file():
                raise StepFailure(f"expected file at {target_path}")
            expected_content = row.get("content", "")
            actual_content = target_path.read_text(encoding="utf-8")
            if actual_content != expected_content:
                raise StepFailure(f"content mismatch for {target_path}: expected {expected_content!r}, saw {actual_content!r}")
        elif entry_type == "dir":
            if not target_path.is_dir():
                raise StepFailure(f"expected directory at {target_path}")
        else:
            raise StepFailure(f"unsupported entry type: {entry_type}")

        stat_result = stat_snapshot(target_path)
        if row.get("mode"):
            actual_mode = stat_result.st_mode & 0o777
            expected_mode = int(row["mode"], 8)
            if actual_mode != expected_mode:
                raise StepFailure(f"mode mismatch for {target_path}: expected {expected_mode:o}, saw {actual_mode:o}")
        if row.get("mtime_ns"):
            actual_mtime = stat_result.st_mtime_ns
            expected_mtime = int(row["mtime_ns"])
            if actual_mtime != expected_mtime:
                raise StepFailure(f"mtime mismatch for {target_path}: expected {expected_mtime}, saw {actual_mtime}")
        if row.get("uid"):
            expected_uid = resolve_expected_identity(source_path, row["uid"], "uid")
            if stat_result.st_uid != expected_uid:
                raise StepFailure(f"uid mismatch for {target_path}: expected {expected_uid}, saw {stat_result.st_uid}")
        if row.get("gid"):
            expected_gid = resolve_expected_identity(source_path, row["gid"], "gid")
            if stat_result.st_gid != expected_gid:
                raise StepFailure(f"gid mismatch for {target_path}: expected {expected_gid}, saw {stat_result.st_gid}")


def execute_step(world: ScenarioWorld, step_data: Step) -> None:
    for regex, handler in STEP_HANDLERS:
        match = regex.match(step_data.text)
        if match is not None:
            handler(world, step_data, match)
            return
    raise StepFailure(f"no step handler matched: {step_data.keyword} {step_data.text}")


def run_scenario(scenario: Scenario) -> None:
    world = ScenarioWorld(scenario)
    try:
        for step_data in scenario.steps:
            execute_step(world, step_data)
    finally:
        world.cleanup()


def discover_features(root: pathlib.Path) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for feature_path in sorted(root.rglob("*.feature")):
        scenarios.extend(parse_feature(feature_path))
    return scenarios


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Usage: gherkin_runner.py <features-dir>", file=sys.stderr)
        return 1

    features_root = pathlib.Path(argv[1]).resolve()
    scenarios = discover_features(features_root)
    if not scenarios:
        print(f"no scenarios found under {features_root}", file=sys.stderr)
        return 1

    passed = 0
    skipped = 0
    for scenario in scenarios:
        try:
            run_scenario(scenario)
            passed += 1
            print(f"[PASS] {scenario.feature_name} :: {scenario.name}")
        except SkipScenario as exc:
            skipped += 1
            print(f"[SKIP] {scenario.feature_name} :: {scenario.name}: {exc}")
        except Exception as exc:  # noqa: BLE001
            print(f"[FAIL] {scenario.feature_name} :: {scenario.name}: {exc}", file=sys.stderr)
            return 1

    print(f"{passed}/{len(scenarios)} gherkin scenarios passed, {skipped} skipped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
