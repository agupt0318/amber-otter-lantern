# Performance log

Record complete-run experiments here. Each entry must include source commit, build flags, machine, dataset seed/counts, input size, correctness result, wall time, peak memory, and decision.

## Local observations, November 20, 2026

Machine: Apple Silicon Mac, 24 GiB physical memory, macOS. Release Rust 1.85. These are exploratory single runs and are not scorer timings. An earlier published build scored 10.55 s officially; this newer build has no official score yet.

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

## Measured optimization pass, November 20, 2026

The previous published commit is `227447a`. The 20M practice input has 1.6 GiB of events and 232 MiB of clicks. All measurements below are full runs on the same Apple Silicon Mac; they vary with background load and page cache. The official score for the previous commit was 10.55 s, but no official score exists yet for this change.

| Change or comparison | 20M wall time | Evidence |
| --- | ---: | --- |
| Previous commit, alternating comparison | 4.746, 7.660 s | Same warmed input, each followed by a changed build |
| Cached decoded payload in every dedup candidate | 7.700, 8.669 s | Slower in both alternating comparisons; reverted |
| Previous commit, second alternating comparison | 3.637, 8.228 s | Same warmed input, each followed by the retained build |
| Compact direct user IDs, borrowed click users, plain Parquet | 3.425, 5.862 s | Faster in both comparisons; retained |
| Retained build with stage metrics | 4.40 s; 4.66 GiB peak RSS | 36/36 fixtures and exact 20M output-table equality |

The cached-payload trial completed a 60M run in 34.54 s with 14.9 GiB peak memory footprint and exact output-table equality, but that result is not attributable to the final implementation because the trial was reverted. The retained build completed the same 60M input in 30.01 s with 10.4 GiB peak memory footprint and exact equality of both output tables against the earlier reference. This is a local result, not a scorer prediction. The retained changes preserve the baseline event dedup and file-order validation. They avoid interning ordinary unlinked base-36 users and avoid copying retained click user strings. Parquet dictionary construction and statistics are disabled; this cut one local output stage from 0.38 to 0.29 s, while increasing the 20M output from 35 MiB to 94 MiB. The scorer's output disk may change that tradeoff.

### Outside techniques reviewed and next measurements

* [Speed Demon track C](https://github.com/LippInc/speed-demon-2026/tree/main/track-c) uses owner-based hash partitions with batches and private tables. Our 64 locked shards are a close but not identical design. A private-owner prototype is the clearest next dedup experiment, with exact event IDs and the scorer's 24 GiB limit checked before adoption.
* [Global Hash Tables Strike Back](https://www.vldb.org/pvldb/vol19/p523-marcus.pdf) shows that a tuned shared group-by table can match partitioned aggregation; partitioning is not automatically faster. Benchmark both designs on the generated 20M and 60M distributions rather than assuming one wins.
* [simd-csv](https://docs.rs/simd-csv/latest/simd_csv/) and [simdcsv](https://github.com/minio/simdcsv) show SIMD and hybrid scanning approaches. Our restricted CSV dialect and fixed field counts make a specialized delimiter scanner a plausible experiment. It must preserve quote and malformed-row semantics on all fixtures.
* [Parquet format](https://github.com/apache/parquet-format) permits plain, uncompressed pages. The current writer still materializes whole output columns and uses the general-purpose Parquet library; a bounded direct writer may save CPU and peak memory, but needs independent reader validation.
* [Polars hash-table changes](https://github.com/pola-rs/polars/releases) include blocked prefetch and key-table layout work. A dedicated packed-ID table could reduce the remaining random accesses, but needs a measured advantage over `hashbrown` and exact fallback keys.

The current measured stage distribution on the retained 20M run was: catalogs and links 0.406 s; event dedup 0.678 s; candidate ordering 0.250 s; validation 0.401 s; user interning and resolution 0.306 s; event sorting 0.145 s; click pipeline 0.642 s; sessionization 0.885 s; output 0.606 s. Future work should prioritize sessionization and event dedup, then test whether output page writing or catalog/link processing can be made cheaper. Language changes alone do not remove these data movement costs.

## Parallel session aggregation follow-up

Events and clicks are already grouped by person before sessionization. The latest change splits the event array only at person boundaries, processes those ranges on the eight workers, and sums the small campaign/day maps. It keeps the serial path for fewer than 100,000 events. Against the immediately preceding published build, alternating 20M runs were 8.760 vs 6.323 s and 7.493 vs 7.187 s (published vs parallel). On one run the session stage fell from 0.885 to 0.260 s. The 60M case completed with a 10.2 GiB peak memory footprint; its outputs matched both baseline tables exactly. Its wall time of 43.99 s was during heavy desktop contention and is not comparable to the earlier 30.01 s run. All 36 fixtures still pass.

## Private event tables

Event parsing now batches candidates by key into shared staging vectors; each partition is then reduced by a single worker in a private table. Newline scanning uses memchr. In three alternating warm 20M comparisons against the scanning-only version, dedup took 0.551/0.569/0.611 s before and 0.433/0.440/0.499 s after. Whole runs were 2.802/2.862/3.613 s before and 2.643/4.329/3.688 s after, so the total benefit is inconclusive under varying desktop load. Both 20M and 60M tables match the prior reference, and all 36 fixtures pass. The 60M run took 23.14 s locally with a 10.1 GB peak footprint; it is not an official score or a controlled comparison. Equal-ingest copies are specified as identical in rules.md.

