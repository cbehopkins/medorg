# 05: Investigate SQLite Backup Inventory Scan Throughput

**What to build:** Measure the cost of backup inventory ingestion, evaluate bounded transaction batching against the existing per-file commits, and record throughput, memory use and correctness without assuming the dominant bottleneck.

**Status:** resolved

## Resolution (2026-11-10)

Closed at the maintainer's direction. Transaction batching reduced commit overhead and produced an initial throughput improvement, but the subsequent slowdown remains unresolved. Each inventory addition still takes the processor mutex across a checksum lookup and write; the mutex profile confirms contention attributed to destination ingestion. A larger ingestion/datastore redesign is deferred to separate work, rather than extending this batching experiment.

Database growth increasing lookup cost is a hypothesis, not a measured cause: checksums have a primary-key index, so these lookups are not unindexed full-table scans. The design review should distinguish lookup/preparation cost, transaction and storage latency, and producer backpressure before choosing a replacement. Remaining unchecked measurements below are explicitly uncompleted, not prerequisites for this agreed closure.

- [ ] Establish a reproducible ingestion benchmark using the same representative input for the baseline and experiment; record file count, directory count, duplicate-checksum distribution, storage location, settings and elapsed time.
- [x] Distinguish files per second from directories per second. The current progress counter increments on directory entry, before all processing for that directory completes.
- [ ] Investigate the observed commit frequency, waits and frequent `nanosleep` calls sufficiently to distinguish evidence from hypotheses; do not interpret aggregate syscall percentages as wall-clock application time.
- [x] Evaluate bounded transaction batching for inventory writes, preserving checksum deduplication, merged backup destinations, source priority selection and visibility to subsequent inventory queries.
- [x] Keep batching memory bounded and preserve error propagation, shutdown handling and cleanup; an ingestion failure must not permit orphan deletion using incomplete source inventory.
- [x] Verify inventory results and backup decisions, including orphan classification and copy selection, are unchanged between baseline and experiment. Reuse existing behavioural tests rather than asserting SQLite implementation details.
- [ ] Compare ingestion throughput and memory use before and after the experiment. Keep checksum policy, traversal concurrency and filesystem settings unchanged to isolate the effect of batching.
- [x] Record representative Raspberry Pi results, or explicitly record that hardware or workload measurements remain unavailable. Coordinate memory and end-to-end parity results with [ticket 04](04-raspberry-pi-memory-measurement.md).

## Reported Evidence

Reported on 2026-11-10 during a Raspberry Pi backup run:

```text
Initial scan for destination inventory ... 3512 / 35337 ... 9.94% 9 p/s
PID 2277: 14.9% CPU, RES 23368 KiB, 0.3% memory
```

The progress unit is a directory, not a file. The rate therefore does not establish file ingestion throughput.

The environment uses Linux `6.12.109+rpt-rpi-v8` and SMB1 source access. SMB1 is a fixed constraint, not an upgrade option. Record the actual destination and temporary SQLite database filesystems during measurement rather than assuming either is on the network share or SD card.

A short `strace -f -c` sample of the running process reported:

| Syscall | Aggregate Time (s) | Calls | Errors |
| --- | ---: | ---: | ---: |
| futex | 7.090612 | 3389 | 170 |
| epoll_pwait | 1.846152 | 605 | 1 |
| nanosleep | 1.318406 | 27468 | 0 |
| fsync | 0.604778 | 2115 | 0 |
| pwrite64 | 0.160270 | 7908 | 0 |
| fcntl | 0.117094 | 8213 | 0 |
| newfstatat | 0.085130 | 3851 | 2164 |
| unlinkat | 0.075134 | 540 | 0 |
| openat | 0.067775 | 1095 | 0 |

All syscalls totalled 11.510721 aggregate seconds across 62473 calls. These are aggregate measurements across traced threads, not an elapsed-time breakdown; tracing also adds overhead. Error counts alone do not establish faults, since existence checks and interrupted waits can legitimately return errors.

## Reported Follow-up (2026-11-10)

After the batching change, a Raspberry Pi run reported `96 p/s`, compared with the earlier `9 p/s` progress rate. The walker progress counter is directory-based, so this is approximately a 10.7x increase in directory progress if the run settings and workload are comparable; it is not a files-per-second measurement. The matching file count and elapsed wall time were not provided.

A short `strace -f -c` sample from the follow-up run reported:

| Syscall | Aggregate Time (s) | Calls | Errors |
| --- | ---: | ---: | ---: |
| futex | 8.547031 | 30845 | 1143 |
| nanosleep | 2.543608 | 19885 | 0 |
| epoll_pwait | 2.345464 | 2416 | 3 |
| newfstatat | 0.562918 | 12654 | 173 |
| pwrite64 | 0.279291 | 18917 | 0 |
| openat | 0.248403 | 3016 | 0 |
| fsync | 0.178064 | 207 | 0 |
| write | 0.097016 | 1037 | 1 |
| unlinkat | 0.090258 | 983 | 0 |
| getdents64 | 0.083352 | 2111 | 0 |
| read | 0.070574 | 2749 | 0 |
| epoll_ctl | 0.059024 | 3728 | 0 |
| fcntl | 0.054269 | 4374 | 0 |
| close | 0.051900 | 3016 | 0 |
| sched_yield | 0.025145 | 785 | 0 |
| getpid | 0.008738 | 371 | 0 |
| tgkill | 0.007616 | 319 | 0 |
| rt_sigreturn | 0.004925 | 319 | 7 |
| madvise | 0.004418 | 233 | 0 |
| pread64 | 0.004398 | 261 | 0 |
| ioctl | 0.000873 | 74 | 0 |
| fstat | 0.000840 | 104 | 0 |
| restart_syscall | 0.000419 | 1 | 0 |
| geteuid | 0.000343 | 52 | 0 |
| sched_getaffinity | 0.000128 | 15 | 0 |

The sample totals 15.269015 aggregate seconds across 108472 calls, compared with 11.510721 seconds across 62473 calls in the earlier sample. Aggregate syscall time can exceed elapsed time because it sums work across threads; the two totals do not establish a regression or explain the throughput change. Tracing overhead and sample durations were not supplied, so this remains directional evidence only.

## Reported Runtime Samples (2026-11-10)

`pidstat` samples for PID 2502 (`mdbackup`) reported:

| Time | User % | System % | CPU % | Wait % | RSS (KiB) | Major faults/s | Write KiB/s | I/O delay |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 13:01:35 | 28.0 | 8.8 | 36.8 | 0.0 | 21,404 | 0 | 3,026.81 | 0 |
| 13:01:40 | 55.6 | 12.6 | 68.2 | 0.0 | 21,808 | 0 | 1,835.12 | 0 |

The accompanying `iostat` output showed `mmcblk0` at 36-38% utilization, with 4.6-7.5 ms write await in the displayed reports; the interval report showed about 2.4 MiB/s written. `sda` utilization was low. The first `iostat` report is since-boot data by default unless `-y` was used, so it must not be compared as a same-length interval with later reports. `sar -n DEV` showed eth0 around 0.9-1.0 KiB/s received and 2.7-4.0 KiB/s transmitted at 13:01:55-13:02:00.

These short samples show stable RSS, no reported process wait or I/O delay, and no obvious `mmcblk0` saturation. They do not establish the cause of the slowdown: no files/s, bytes/s, or directory-progress samples were timestamped alongside them, and low network byte-rate does not rule out SMB metadata latency. Collect throughput and resource counters over the same fixed windows; use `iostat -t -y -xz 5` to get timestamped interval reports without the since-boot first report.

## Go pprof Follow-up (2026-11-10)

The early and late CPU profiles have the same build ID (`4a1a089bc8ce88290ccd997a0e6c4f5874acce4a`). The late profile was requested with `curl .../debug/pprof/profile?seconds=30`, but `go tool pprof` reports `Duration: 60s`; the requested and reported durations therefore disagree and need reconciliation. The early profile spends notable CPU in Linux syscalls and SQLite; do not compare the `-base` per-function deltas as rates until the actual capture windows are confirmed. Capture equal-duration CPU windows during fast and slow periods before interpreting per-function changes.

The block-profile difference reports 9.84 aggregate hours of delay, of which 8.66 hours is `runtime.chanrecv2`; 7.74 hours is attributed to `core.newWorkerPool.func1`. Worker-pool goroutines block receiving from `workCh` while idle, so this large cumulative channel wait is not equivalent to wall-clock time lost by the backup. Block delay is summed across goroutines and can exceed process elapsed time.

The mutex-profile difference reports 111.16 seconds of aggregate contention delay, with 105.48 seconds attributed to `BackupProcessor.addDstFile` and 106.79 seconds cumulatively under `DirectoryWalker.dirVisitor`. In the current implementation, `addDstFile` holds the processor mutex across the SQLite transaction lookup, destination merge, and write. The directory walker runs directory visitors concurrently, so this is a strong, testable indication that destination ingestion is serialized at that lock. It does not by itself prove that lock contention explains the late-run slowdown; the profile interval and matching throughput were not provided.

Next discriminating capture: keep mutex/block profiling enabled, capture cumulative snapshots at the start and end of a fixed 30-60 second window, and record files/s and bytes/s over that exact window. Repeat once while fast and once while slow. A rising mutex-delay delta per destination file as throughput falls would support the lock-contention hypothesis. If confirmed, evaluate coarser-grained destination ingestion rather than weakening synchronization around the shared transaction.

## Current Code Observations

- Destination ingestion in `pkg/consumers/backup_processor_sqlite.go` performs a checksum lookup followed by an individual `INSERT` or `UPDATE`, without an explicit batching transaction. Repeated commits are consistent with the observed journal and synchronisation activity, but their contribution to elapsed scan time remains unmeasured.
- The progress walker in `pkg/core/directory_walker.go` performs a directory-counting pass before the inventory traversal.
- Inventory traversal reads directory entries, file properties and `.medorg.xml` metadata. The shared walker also invokes `ChecksumCalc`, which can calculate missing checksums; this phase is not guaranteed to be metadata-only.
- Low CPU utilisation is consistent with waiting, but the trace does not establish whether SQLite, filesystem latency, runtime parking or another mechanism dominates. Frequent `nanosleep` calls warrant investigation without assuming a particular retry or polling loop.

## Local Experiment (2026-11-10)

`go test ./pkg/consumers -run '^$' -bench '^BenchmarkBackupInventoryWriteBatching$' -benchtime=1x -benchmem -count=1 -v` compared identical synthetic inputs on Windows/amd64, an 11th Gen Intel Core i7-11800H at 2.30 GHz. Each run ingested 4,096 file records across 64 synthetic directories, with 1,024 unique checksums (four records per checksum), to a temporary database under `C:\Users\Chris\AppData\Local\Temp`; SQLite cache size was 2,048 KiB.

| Mode | Batch size | Elapsed | Files/s | Directories/s | Go allocations/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Single-record commit | 1 | 40.839 s | 100.3 | 1.567 | 22,385,736 B |
| Batched | 128 | 0.804 s | 5,096 | 79.63 | 12,608,872 B |

The synthetic ingestion rate improved by about 50.8x and Go allocated bytes fell by about 43.7%. Allocated bytes are not peak resident memory. This benchmark excludes directory traversal, checksum calculation, SMB access, and Pi storage characteristics; it does not resolve the reported `nanosleep` or wait attribution. The representative Pi measurements and peak RSS comparison remain unavailable.

## Comments

This ticket records a performance investigation, not a confirmed root cause or a measured speed-up. Do not mask metadata read errors, alter orphan semantics, or introduce SMB protocol changes as part of the batching experiment.
