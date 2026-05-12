# HyperSync — Codex Implementation Instructions

## Context

HyperSync is a C++20 NFS-to-NFS high-speed file transfer engine. This document describes
the design decisions and required changes for the metadata pipeline and async NFS layer.
Read this before touching `nfs_backend.cpp`, `nfs_meta_reader.*`, `queue_job.hpp`, or
`scan_index.*`.

---

## 1. Design: Threads vs Async Concurrency

This is the most important concept to understand before making any change.

**The distinction:**

```
THREADS-AS-CONCURRENCY (wrong for this codebase)
  Thread 1 → nfs_opendir() → blocks waiting for NFS RPC → wakes → processes → next
  Thread 2 → nfs_opendir() → blocks waiting for NFS RPC → wakes → processes → next
  ...
  Thread 96 → nfs_opendir() → blocks ...

  96 threads = 96 in-flight NFS calls
  Problem: 96 OS threads, 96 stacks, 96× context switch overhead

ASYNC-AS-CONCURRENCY (correct model)
  Thread 1:
    submit req_1 via nfs_opendir_async()
    submit req_2 via nfs_opendir_async()
    ...
    submit req_96 via nfs_opendir_async()
    epoll_wait(nfs_get_fd(nfs), ...)   ← single wait point for all 96
    nfs_service(nfs, revents)          ← drives whichever completed
    harvest completions, submit more

  1 thread = 96 in-flight NFS calls
  Benefit: 1 OS thread, 1 stack, no context switch overhead
```

**Rule for this codebase:**
- Thread count = number of independent *contexts* needing concurrent work at the same time
- Async depth (in-flight request count) = how many NFS RPCs are overlapped within one context
- These are orthogonal. Never conflate them.

---

## 2. NfsMetaReader: Correct Architecture

### 2.1 What the design doc requires (§6.2)

N NfsMetaReader instances, each processing **one flat folder** at a time:

```
InputProvider  ←──── child folder FolderRecords
     │
     │ pull(FolderRecord)  ×N
     ▼
MetaReader[0]   MetaReader[1]  ...  MetaReader[N-1]
  folder A         folder B              folder N
  (flat scan)      (flat scan)           (flat scan)
     │                │                     │
     │ RecBuf per file (via QueueJob)
     ▼
  Checker[0..N-1]
```

- Each MetaReader pulls one FolderRecord, scans it completely (non-recursive), emits one
  RecBuf per file, emits child FolderRecords back to InputProvider, then pulls the next.
- No MetaReader does recursive descent. Recursion is handled by re-inserting child
  FolderRecords into InputProvider's queue.
- N is configurable (default: 8). This controls how many folders are scanned in parallel.

### 2.2 Why not 96 threads

For NfsMetaReader specifically, the async API is used for `nfs_opendir_async` (the RTT of
opening a directory handle), but `nfs_readdir` itself is an in-memory iterator over results
already returned by the server — no extra RPCs. So one folder scan = one outstanding
opendir RPC + synchronous readdir iteration.

This means:
- Each MetaReader thread has **1 in-flight opendir** at any moment (not 96)
- Parallelism comes from N concurrent threads, each on a different folder
- N=8 threads → 8 concurrent folder scans → sufficient for 350K+ records/sec

96 in-flight NFS calls are needed for **NfsDataReader** (reading file data), not for
NfsMetaReader. See §4.

### 2.3 What to change in NfsMetaReader

The class already has the right shape (`begin_folder`, `publish_record`,
`discover_child_folder`, `finish_folder`). The missing piece is wiring N instances to a
shared InputProvider in TransferEngine and running each in its own thread.

**`NfsMetaReaderConfig`** — add `worker_count`:

```cpp
struct NfsMetaReaderConfig {
    std::size_t recbuf_window;
    std::size_t streaming_mode_threshold;
    bool async_readdir;
    std::string source_root;
    bool recursive;
    std::size_t worker_count;  // ADD: number of parallel flat-folder workers (default 8)
};
```

Add to `config/default.yaml` under `jobs.nfs_meta_reader`:
```yaml
  nfs_meta_reader:
    worker_count: 8
```

**`publish_tree()`** — this method drives the entire scan from a single call. It must
become a flat-folder scan loop, not a recursive tree walk:

```cpp
// publish_tree() must do:
//   1. Ask InputProvider for a FolderRecord
//   2. Call backend().visit_folder(folder.rel_path, recursive=false, visitor)
//      where visitor emits RecBufs and discovers children
//   3. Push children back to InputProvider
//   4. Call finish_folder(files_total)
//   5. Repeat until InputProvider is drained
```

**TransferEngine** — create `worker_count` NfsMetaReader instances, run each in its own
`std::thread`, join all on stop. Do not create a single shared NfsMetaReader and call
`publish_tree()` from one thread.

### 2.4 visit_folder vs visit_files

`NfsBackend` currently has `visit_files(bool recursive, visitor)` which still recurses
inside `LibNfsBackend::visit_files_impl`. Add a non-recursive variant:

```cpp
// NfsBackend interface — add:
virtual void visit_folder(std::string_view rel_path,
                          const std::function<void(FileSpec)>& file_visitor,
                          const std::function<void(FileSpec)>& dir_visitor) const;
```

`LibNfsBackend::visit_folder` opens exactly one directory (`/` + rel_path), iterates its
entries, calls `file_visitor` for files and `dir_visitor` for subdirectories. No recursion.
The MetaReader's dir_visitor calls `discover_child_folder(child)`.

This replaces the current `visit_files(recursive=true)` call in `publish_tree()`.

---

## 3. ScanIndex: Replace std::map with std::unordered_map

**File:** `src/common/scan_index.hpp` and `src/common/scan_index.cpp`

**Change:** `rows_by_path_` is declared as `std::map<std::string, FileSnapshot>`.

```cpp
// scan_index.hpp — current (WRONG):
std::map<std::string, FileSnapshot> rows_by_path_;

// scan_index.hpp — fix:
std::unordered_map<std::string, FileSnapshot> rows_by_path_;
```

**Why this matters:**
- `std::map` lookup: O(log N) with cache-miss pointer chasing through tree nodes
- `std::unordered_map` lookup: O(1) average, ~50 ns
- At 350K records/sec with 1M-file trees: map costs 800 ns/lookup = 28% of budget;
  unordered_map costs 50 ns/lookup = 1.75% of budget
- The `folder_index_` member is already `unordered_map` — `rows_by_path_` must match

**Remove the `#include <map>` include once changed.** The `<unordered_map>` include is
already present.

No changes to `.cpp` logic — the API (`find`, `add`, `rows`) stays identical.

---

## 4. NfsDataReader: True Async Polling with N In-Flight Reads

### 4.1 Current problem

Every read operation in `nfs_backend.cpp` uses this pattern:

```cpp
// Current pump_nfs_until_done — services ONE state at a time:
void pump_nfs_until_done(struct nfs_context* nfs, AsyncReadState& state) {
    while (!state.done) {
        // poll → nfs_service → repeat
    }
}

// Called as:
AsyncReadState state;
nfs_pread_async(nfs, handle, offset, size, callback, &state);
pump_nfs_until_done(nfs, state);  // blocks here until THIS ONE request is done
// → only 1 NFS read RPC in-flight at any time
```

The config `outstanding_requests: 256` is completely dead — only 1 request is ever
in-flight regardless of this setting.

### 4.2 Required: multi-pending poll loop

Add a new helper that drives an arbitrary number of in-flight requests on one
`nfs_context`. The key insight: `nfs_service()` drives **all** outstanding requests on a
context, not just one. Submit N requests, then service the shared fd — all N complete
when ready.

```cpp
// Add to nfs_backend.cpp (inside anonymous namespace, after existing pumpers):

// State for one outstanding async read
struct PendingRead {
    AsyncReadState result;
    std::size_t slot_index = 0;
    std::uint64_t file_offset = 0;
    // add any fields NfsDataReader needs to correlate completion to work item
};

// Drive nfs_context until at least one pending entry is marked done.
// Returns immediately if revents > 0 (caller already polled).
// Call nfs_service() which drains ALL ready completions on the context.
inline void service_nfs_context(struct nfs_context* nfs) {
    const int fd = nfs_get_fd(nfs);
    const int wanted = nfs_which_events(nfs);
    struct pollfd pfd{fd, static_cast<short>(wanted), 0};
    if (fd >= 0) {
        ::poll(&pfd, 1, 1);  // 1 ms timeout — don't block if nothing ready
    }
    nfs_service(nfs, pfd.revents);
}
```

**Pattern for NfsDataReader's read loop:**

```cpp
// Pseudocode — adapt to actual NfsDataReader structure:

constexpr std::size_t MAX_IN_FLIGHT = 256;  // from config outstanding_requests

std::vector<PendingRead> slots(MAX_IN_FLIGHT);
std::vector<bool> in_use(MAX_IN_FLIGHT, false);
std::size_t in_flight = 0;

while (more_work_to_submit || in_flight > 0) {

    // Submit new reads into free slots
    while (in_flight < MAX_IN_FLIGHT && more_work_to_submit) {
        std::size_t free_slot = find_free_slot(in_use);
        PendingRead& p = slots[free_slot];
        p.result = {};                     // clear
        p.result.done = false;
        // submit the async read:
        nfs_pread_async(nfs, handle, file_offset, chunk_size,
                        read_nfs_callback, &p.result);
        in_use[free_slot] = true;
        ++in_flight;
        advance_work_cursor();
    }

    // Service the context — drives ALL in-flight requests
    service_nfs_context(nfs);

    // Harvest completions
    for (std::size_t i = 0; i < MAX_IN_FLIGHT; ++i) {
        if (in_use[i] && slots[i].result.done) {
            process_completed_read(slots[i]);  // build RecBuf, compute hash chunk, etc.
            in_use[i] = false;
            --in_flight;
        }
    }
}
```

### 4.3 NfsDataReader thread count

With proper async, NfsDataReader needs only **2–4 threads** total:
- Each thread owns one `nfs_context` (one socket) and drives up to 256 in-flight reads
- Total in-flight: 2–4 threads × 256 = 512–1024 outstanding NFS reads
- This saturates any NFS cluster at 400 Gbit/s

Do **not** use one thread per in-flight request. That is the pattern being replaced.

Add `data_reader_worker_count` to `NfsDataReaderConfig` (default: 2):
```yaml
  nfs_data_reader:
    data_reader_worker_count: 2
    outstanding_requests: 256   # per worker thread
```

---

## 5. QueueJob: Remove std::any and std::mutex from Hot Path

### 5.1 Current problem

`QueueJob` in `src/jobs/queue_job.hpp` uses:
- `std::any` to wrap the payload → heap allocation for every `RecBuf` (460 bytes > 24-byte
  SBO limit → guaranteed `malloc` per message)
- `std::deque<JobMessage>` → node-based allocation, poor cache locality
- `std::mutex` on every `pull()` and `push_back()` → futex contention between producer
  and consumer threads

At 350K records/sec this is the primary CPU bottleneck in the message-passing layer.

### 5.2 Fix: typed lock-free SPSC ring per producer-consumer pair

`src/common/preallocated_ring.hpp` already has `PreallocatedRing<T>` but it is not
thread-safe. Add an SPSC (single-producer single-consumer) variant:

**New file: `src/common/spsc_ring.hpp`**

```cpp
#ifndef HYPERSYNC_COMMON_SPSC_RING_HPP
#define HYPERSYNC_COMMON_SPSC_RING_HPP

#include <atomic>
#include <cassert>
#include <cstddef>
#include <vector>

namespace hypersync {

// Lock-free single-producer single-consumer ring buffer.
// T must be trivially destructible or caller must ensure no live items on destroy.
// Capacity must be a power of two.
template <typename T>
class SpscRing {
public:
    explicit SpscRing(std::size_t capacity) : mask_(capacity - 1), slots_(capacity) {
        assert((capacity & mask_) == 0 && "capacity must be a power of two");
    }

    // Producer side — returns false if full (caller should back off and retry)
    bool try_push(T value) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next = (head + 1) & ~std::size_t{0};
        if ((next & mask_) == (tail_.load(std::memory_order_acquire) & mask_) &&
            next != tail_.load(std::memory_order_acquire)) {
            // full
            return false;
        }
        slots_[head & mask_] = std::move(value);
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    // Consumer side — returns false if empty
    bool try_pop(T& out) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) {
            return false;  // empty
        }
        out = std::move(slots_[tail & mask_]);
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return tail_.load(std::memory_order_acquire) ==
               head_.load(std::memory_order_acquire);
    }

private:
    const std::size_t mask_;
    std::vector<T> slots_;
    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
};

}  // namespace hypersync

#endif
```

### 5.3 Typed inter-job channel

Between MetaReader[i] and Checker[i], replace `QueueJob::pull(JobMessage&)` with a typed
`SpscRing<RecBuf>`. Each MetaReader/Checker pair shares one ring. The ring capacity should
be 8192–65536 (power of two, sized to hold a few milliseconds of production).

`TypedQueueJob<RecBuf>` can remain as the external API for compatibility, but internally
use `SpscRing<RecBuf>` instead of `std::deque<JobMessage>` with mutex. The `publish_item`
method writes to the ring; `pull()` reads from it.

The `std::any` wrapping in `JobMessage` can be removed for all typed inter-job channels.
Keep `JobMessage` only for control messages on the priority channel where type-erasure is
needed.

---

## 6. Receiver Skip Detection: No NFS Stat RPCs

### 6.1 Current problem

On the receiver side, determining whether a file should be skipped (already exists on
target) can trigger NFS stat RPCs. At 200 µs per stat RPC, this caps skip throughput at
5,000 files/sec — completely incompatible with the 350K records/sec target.

### 6.2 Fix

Load the target scan CSV into a `ScanIndex` at startup (already supported). When a
`MSG_FILE_RECORD` arrives:

1. Check `target_scan.find(record.rel_path)` — O(1) with `unordered_map` (see §3)
2. If found and `metadata_matches` (size + mtime) → send `MSG_FILE_SKIP`, do not write
3. If not found or mismatch → proceed with write

**Never issue a live NFS stat RPC in the skip decision path.** NFS stat is only acceptable
as a fallback when no scan CSV is available and only for debugging/correctness modes, not
in the throughput path.

The `Checker::should_skip()` method already implements this logic correctly. Ensure it is
wired up and that `target_scan_` is populated from the CSV on receiver startup.

---

## 7. What Not to Change

- **`pump_nfs_until_done`** — do not delete. It is still used for single-shot operations
  like `nfs_mount_async`, `nfs_mkdir2_async`, `nfs_close_async`. Only data reads need the
  multi-pending loop. Replace only in `NfsDataReader`'s read path.

- **`QueueJob` interface** (`start`, `stop`, `pull`, `push_back`, `stats`) — keep it.
  Other jobs (DataSender, DataWriter, DataCacher) use it and must not be broken. Change
  the internal implementation, not the interface.

- **`scan_index.cpp` logic** — only change `std::map` to `std::unordered_map` in the
  header and remove the `#include <map>`. No algorithm changes.

- **`visit_files(bool recursive, visitor)`** — keep for backward compatibility and for
  the scan/dry-run modes. Add `visit_folder` alongside it, do not replace.

- **Tests** — all 25 unit + 11 integration + 2 performance tests must continue to pass.
  The `nfs_jobs_use_backend_for_local_sources` and `transfer_mode_*` tests exercise
  MetaReader and Checker directly. Run `make` before committing.

---

## 8. Summary of Changes — Ordered by Priority

| # | File(s) | Change | Impact |
|---|---|---|---|
| 1 | `src/common/scan_index.hpp` | `std::map` → `std::unordered_map` for `rows_by_path_` | Skip detection 50 ns vs 800 ns |
| 2 | `src/jobs/nfs_meta_reader/nfs_meta_reader.*` | Add `worker_count` config; `publish_tree` becomes flat-folder loop; add `visit_folder` to NfsBackend | N parallel folder scans |
| 3 | `src/core/nfs_backend.*` | Add `visit_folder(rel_path, file_visitor, dir_visitor)` to `NfsBackend` and `LibNfsBackend` | Non-recursive per-folder scan |
| 4 | `src/jobs/transfer_engine/transfer_engine.*` | Instantiate N MetaReader + N Checker pairs; run each pair in its own thread | Actual parallel execution |
| 5 | `src/common/spsc_ring.hpp` (new) | `SpscRing<T>` lock-free SPSC ring | Zero-allocation message passing |
| 6 | `src/jobs/queue_job.hpp` | `TypedQueueJob<T>` uses `SpscRing<T>` internally instead of `deque+mutex+any` | 10× throughput in message layer |
| 7 | `src/core/nfs_backend.cpp` | Add `service_nfs_context()` helper; NfsDataReader uses multi-pending poll loop | 256 reads in-flight vs 1 |
| 8 | `src/jobs/nfs_data_reader/nfs_data_reader.*` | Add `data_reader_worker_count`; run N async poll loops | Saturate NFS data bandwidth |
| 9 | `config/default.yaml` | Add `worker_count: 8` under `nfs_meta_reader`; `data_reader_worker_count: 2` under `nfs_data_reader` | Expose tuning knobs |

Changes 1–4 unlock the 350K records/sec metadata target.
Changes 5–6 eliminate per-record heap allocation.
Changes 7–8 unlock the 400 Gbit/s data target.
