# Performance log

Record complete-run experiments here. Each entry must include source commit, build flags, machine, dataset seed/counts, input size, correctness result, wall time, peak memory, and decision.

## Local observations, November 20, 2026

Machine: Apple Silicon Mac, 24 GiB physical memory, macOS. Release Rust 1.85. These are exploratory single runs and are not scorer timings. No official score has been obtained.

| Variant | Dataset | Wall | Peak RSS | Verification |
| --- | --- | --- | --- | --- |
| owned event candidates, single-threaded | seed 1, 1M events, 250k clicks | 1.26 s | 552 MiB | 36 fixtures; published dev answers exact |
| mapped-row candidates, single-threaded | same | 1.55 s | 301 MiB | 36 fixtures; published dev answers exact |
| mapped rows, single-threaded | seed 3, 20M events, 5M clicks | 54.22 s | 3.17 GiB | compared equal to next variant |
| faster hash and parallel sort | same | 34.11 s | 3.49 GiB | exact equality with previous output; all fixtures and dev answers pass |
| parallel event reduction | same | 24.57 s | 4.07 GiB | exact equality with previous output |
| parallel reduction and file-order validation | same | 19.44 s | 4.58 GiB | exact equality with baseline |
| compact event/user IDs, fixed-width timestamps, packed event sort | seed 3, 20M events, 5M clicks | 15.49 s | 4.53 GiB | exact equality with prior 20M output; all fixtures pass |
| staged parallel parse/reduction, compact click IDs, streaming session state | seed 3, 20M events, 5M clicks | 3.31 s warm local best | 4.53 GiB | exact equality with prior 20M output; all native and Linux/AMD64 fixtures pass |
| serial reduction, faster hash and parallel sort | seed 4, 60M events, 15M clicks | 172.11 s | 9.26 GiB | output totals/invariants; baseline for comparison |
| parallel reduction, unordered validation | same | 236.49 s | 7.36 GiB | exact equality of all output rows with serial run |
| parallel reduction, file-order validation | same | 159.50 s | 7.86 GiB | exact equality of all output rows with serial run |

The 20M tests had different concurrent background load, so the timings do not establish a controlled speedup. The first overlapped an emulated Docker build. The implementation remains far from the observed leaderboard and has not been measured on scorer hardware. Mapped input is borrowed read-only through deduplication; mappings are released once surviving events have been materialized.

The 60M run with parallel reduction was slower overall despite faster deduplication: mapped-page faults and event validation increased substantially. File-order validation improved that run. Peak RSS on macOS excludes some memory counted in footprint; serial, parallel unordered, and parallel file-order 60M peak memory footprints were 10.66 GiB, 9.92 GiB, and 13.59 GiB respectively. Docker Desktop on this Mac has an 8 GiB VM limit, so the 24 GiB Linux cgroup limit has not been verified at full scale. These are single observations under varying cache and machine load; the parallel file-order path is the default because it was fastest locally, but requires scorer-like Linux measurements and more seeds.

Correctness as of this log: all 36 fixtures and the 1M published answers pass both natively and in the linux/amd64 baseline image. A separate Python reference matched five 10k-event seeds. The 60M generated seed has no independent official expected output; comparisons are between implementations and invariants.

The latest 20M timing ran with warm input pages, so it is useful for rejecting regressions but is not comparable to earlier cold or contended local timings. The change is deliberately confined to measured hot paths: compact keys avoid repeated byte hashing for generated IDs while retaining exact byte-string fallbacks, timestamp parsing avoids generic iterator overhead for fixed-width ISO forms, and event sorting uses one packed comparison only when the input window and person count fit its proven bounds. The generated 20M output is exactly equal to the prior implementation's logical Parquet tables.

The latest implementation profiles the stages separately. On a warm, otherwise idle local run the remaining work was distributed across event reduction and identity interning, click reduction, session aggregation, catalog/link loading, and Parquet writing; no single stage dominated after the staged parallel passes. Later repeated runs varied substantially with desktop load and page-cache state, including slower than prior observations. A sub-two-second score cannot be inferred from these local measurements. Reaching that target would require replacing the remaining general-purpose maps and Parquet writer with a scorer-specialized low-level engine, then measuring it on comparable Linux hardware.

