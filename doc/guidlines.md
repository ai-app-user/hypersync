# Development Guidelines

This document defines general development principles for the application. It should stay mostly independent of any single feature so it can guide scanner, hasher, generator, perf, diff, copy, and sync work equally.

## Core Principles

- Keep requirements, UX, design, implementation, and tests separate.
- Use `dev` as the default working branch for every project repository. Push
  normal iteration commits to `dev` after each completed iteration.
- Merge or push changes back to `main` only when the maintainer explicitly asks
  for a main merge or release promotion. Do not treat successful tests or a
  finished task as implicit permission to update `main`.
- Prefer simple, observable behavior over hidden cleverness.
- Make every long-running workflow interruptible, measurable, and resumable where practical.
- Treat failures as first-class output: count them, report them, and make strict versus best-effort behavior explicit.
- Prefer reusable jobs and shared libraries over command-specific one-off code.
- Keep pipeline jobs independent: a job receives typed input, performs its own work with its own threading, and emits typed output without knowing which job produced the input or which job will consume the output.
- Keep generic job lifecycle mechanics in shared abstractions. Concrete jobs should implement payload-specific work and queue/pool ownership behavior, not duplicate start/stop/wait/thread bookkeeping.
- Keep generic producer mechanics in shared abstractions. Concrete buffer producers should not duplicate count limiting, sequence assignment, buffer acquisition, output queue push, or release-on-failed-push behavior.
- Keep generic consumer mechanics in shared abstractions. Concrete buffer consumers should not duplicate input queue pop/drain loops, queue wakeup, pool lookup, or common consumed-buffer stats.
- Use sharded queues for hot producer-to-consumer data paths. Consumers should prefer their assigned shard but be able to steal from other shards so imbalanced workloads do not strand buffers.
- Development and performance tests should exercise sharded queues for hot job-to-job paths. Keep single `BufQueue` tests only for the primitive itself or explicit contention experiments.
- Keep domain work in the responsible job. For example, NFS readers read data buffers; hashers hash data buffers and finalize file hashes.
- Keep command behavior stable enough for automation.
- Add configuration only when it has a clear operational purpose.
- Keep defaults safe for small local tests, with explicit options for high-performance runs.
- Keep deployment UX brutally simple: prefer one-command install/run flows and
  flat deploy bundles over nested layout when the artifact contains only a few
  files.
- Move file payload buffers by ownership between jobs; do not copy data bytes into queue messages.
- New hot-path pipeline jobs should consume and emit `BufferHandle` values through `BufQueue` and operate on `RawBufferPool` slots. Typed payload interpretation belongs at the element/view level only.
- Preallocate payload buffers for high-volume data paths; do not allocate or free per-chunk payload memory during steady-state reads, writes, hashing, or transfer.
- Do not create large payload buffers as stack temporaries. Reset or initialize
  preallocated buffers by updating their small headers/counters and only clear
  payload bytes when correctness requires it.
- Treat buffer payloads as byte wire formats. Encode/decode multi-field
  payload headers with structured copy helpers such as `memcpy`; do not rely on
  reinterpreting byte arrays as aligned C++ structs.
- Treat borrowed backend buffers as callback-lifetime data that must not cross job boundaries.
- When a backend returns borrowed data, copy it at the backend boundary into an owned preallocated slot if another job must process it asynchronously.
- Benchmark modes that intentionally skip copying payload bytes must be explicit, labeled as invalid for downstream consumers that inspect data, and limited to discard/performance isolation paths.
- Async backend shutdown must be cooperative where possible: stop queueing new backend work, drain or safely cancel already in-flight operations, then release owned buffers. Do not release buffers still referenced by pending callbacks.

## Recommended Folder Structure

The repository should keep top-level concerns clear:

- `piper/`: reusable asynchronous pipeline infrastructure. It owns generic
  buffer pools, queues, generic job base classes, generic buffer jobs,
  transport helpers, config parsing, and helper monitoring code.
- `hypersync/`: the WSync/Hypersync product. It owns NFS backends, metadata
  schemas, hashing/sync/copy/scanner jobs, CLI commands, product config,
  product documentation, and integration/functional/performance tests.
- `piper/src/` and `hypersync/src/`: production source code for each library or
  product.
- `piper/doc/` and `hypersync/doc/`: design, requirements, UX, and operational
  documentation at the correct ownership boundary.
- `piper/tests/` and `hypersync/tests/`: tests owned by each component.
- `hypersync/config/`: default and example product configuration files.
- `build/`: generated build output only; never source-of-truth content.

New top-level folders should be rare. Prefer extending `piper/` or
`hypersync/` unless the new folder represents a durable product concern.

## Important Files

Documentation files should have clear ownership:

- `hypersync/doc/requirement.md`: what the product must do and what is done or not done.
- `hypersync/doc/ux.md`: how users should install, configure, and run the product.
- `hypersync/doc/design.md`: architecture and design decisions.
- `hypersync/doc/performance.md`: observed job-level and pipeline performance baselines.
- `hypersync/doc/claude.md`: imported implementation notes and external design guidance.
- `hypersync/doc/guidlines.md`: general development rules and quality bar.
- `hypersync/doc/ai/`: tracked AI session handoff context. Keep `chat.md`
  for timestamped User/Codex conversation history, `kb.md` for durable facts
  and operational state, and `scripts/` for reusable session scripts.
- `piper/doc/`: reusable pipeline infrastructure documentation.

Project files should stay predictable:

- Build files define how to build, test, and package.
- Default config files document safe defaults.
- Test entry points must make it easy to run unit, integration, functional, and performance tests separately.

## Documentation Rules

- Treat durable, non-feature-specific instructions from maintainers as project guidelines. When a maintainer gives a general rule about architecture, comments, file organization, testing, documentation, performance practices, or design principles, update this file as part of the same change.
- Update `hypersync/doc/requirement.md` when scope, status, or product behavior changes.
- Update `hypersync/doc/ux.md` when command names, options, config shape, installation, or runtime behavior changes.
- Update design documentation when architecture, job composition, data flow, or major tradeoffs change.
- Update `hypersync/doc/performance.md` when benchmark commands, expected job-level
  throughput, or production performance assumptions change.
- Documentation should say whether a capability is implemented, partial, or not started.
- Requirements should describe what is needed, not how it is implemented.
- UX docs should describe the user-facing flow, not internal code paths.
- Design docs may describe internals, tradeoffs, and implementation constraints.

## Code Organization Rules

- Put reusable behavior in shared libraries or reusable jobs.
- Commands should compose reusable jobs instead of duplicating pipeline logic.
- Performance proof knobs that add synthetic CPU work must live inside the job being tested and must not change upstream byte counts or downstream ownership semantics.
- A job should have one clear responsibility.
- Shared data structures should live in common code and have tests.
- Command parsing should stay thin and delegate real work to library code.
- Keep generated files out of source folders.
- Do not mix unrelated refactors with feature work.

## Configuration Rules

- Every operationally important limit should be configurable.
- Every job should expose its operational concurrency, queue depths, batching,
  rate limits, and backpressure limits through config when those knobs affect
  throughput, memory, or fairness.
- Commands must not bake in job parallelism or queue-size defaults. They should
  load job config first and let command-line options override it for that run.
- Command-line values should override config file values.
- Config names should be consistent across tools.
- Defaults should be conservative and safe.
- High-throughput settings should be explicit and visible in final summaries.
- Config errors should fail early with clear messages.

## Testing Requirements

Every change should be covered at the lowest practical level.

Unit tests:
- Required for shared utilities, parsers, data structures, formats, and job behavior.
- Should cover success, edge cases, and failure behavior.
- Should not require external services.
- Should be deterministic and fast.
- Test binaries should support listing tests and selecting/excluding tests by stable short id or name substring.

Integration tests:
- Required when multiple jobs, commands, backends, or processes interact.
- Should verify real command-line behavior where possible.
- Should include local-path coverage so tests can run without external infrastructure.

Functional tests:
- Should describe user-visible behavior in a readable scenario format.
- Use a Given/When/Then style for setup, action, and expected outcome.
- Should avoid implementation details.
- Should cover complete workflows such as scan, diff, copy, sync, and performance smoke runs.

Performance tests:
- Should define workload, duration, concurrency, source and target type, and expected metric.
- Should report enough context to compare runs.
- Should print benchmark summaries in normal successful output, not only on
  failure, so job-level results are visible in logs.
- Should avoid pretending a noisy benchmark is deterministic.
- Should distinguish warm-up, steady-state, and graceful-drain measurements.

Coverage expectations:
- New shared logic must have unit coverage.
- New commands must have at least one command-line smoke test.
- New pipeline behavior must have integration coverage.
- New user-facing workflows should have functional scenarios.
- Bug fixes should include a regression test when practical.

## Functional Test Language

Functional tests should read like product behavior.

Preferred style:

```gherkin
Feature: NFS scan inventory

  Scenario: Write file and folder inventory
    Given a source tree with files and folders
    When the user scans the source tree as CSV
    Then the output contains file records
    And the output contains folder records
    And the final summary reports the files and folders found
```

Rules:
- Use domain words users recognize.
- Keep scenarios focused on one behavior.
- Avoid naming internal classes or functions.
- Use tables for repeated inputs and expected outputs.
- Add tags for slow, external, destructive, or performance scenarios.

## Error Handling Rules

- Prefer explicit error policy over hidden behavior.
- Inventory and performance tools should default to best-effort when that is safe.
- Copy and sync tools should be conservative by default.
- Destructive operations must require explicit user intent.
- Error summaries must include counts.
- Detailed error logs should include enough context to identify the endpoint and path.

## Performance Rules

- Performance-sensitive paths must expose their concurrency settings.
- Final summaries must include the settings used for the run.
- Progress stats should be printed periodically during long runs.
- Generic job monitoring should report cumulative rate, recent/current rate,
  first observed startup rate, mid-run historical rate, peak observed rate, tail
  rate after a job stops, queue fullness, and worker wait-state percentages.
- Instrumentation should be low overhead: avoid per-buffer timestamps when a
  non-blocking queue or pool fast path succeeds; enter timed states only for
  actual waits or owned external I/O.
- A timer should stop new work gracefully and still print a final summary.
- Timer expiry should stop new input and new backend requests first, then drain bounded in-flight work before final stats.
- Benchmarks should count work as it completes, not only after large batches finish.
- Avoid unbounded read-ahead unless explicitly requested.

## Review Checklist

Before considering work complete:

- Requirements updated when product behavior changed.
- UX updated when user-visible behavior changed.
- Tests added or updated at the right level.
- Local tests run and results recorded.
- New config options have defaults.
- Final command output includes useful summary fields.
- Failure paths are handled and reported.
- No unrelated refactors or generated artifacts are included.
