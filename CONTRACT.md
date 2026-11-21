# Execution contract

- Input: `/data`, read-only; output: `/output`, initially empty; scratch: `/scratch`.
- Environment: `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU=8`, `MEM_GB=24`.
- Scorer: Linux amd64, Google Cloud n2 Intel Xeon, Cascade Lake or newer, AVX-512 available.
- Limits: 8 vCPUs, 24 GB RAM, no swap, no network, read-only root except 64 MB `/tmp`.
- Process: uid 1000. Container `CMD` takes no arguments. Runtime includes process start through exit.
- Output: one or more direct `*.parquet` files in `user_daily/` and `campaign_daily/`.
- Build time is not scored. Rules override starter documentation.

Official page checked November 20, 2026: submit a public GitHub repository of at most 50 MB, with a root Dockerfile, and an optional commit SHA. Build has network access and a ten-minute limit; the image limit is 8 GB. Output and scratch together are limited to 50 GB. Two submissions are allowed per hour. Practice uses approximately 20M events; runs under 20 seconds are repeated three times and the median is scored. The final runs three times on approximately 60M new events with a three-minute limit for each run; any timeout yields no final score. At least the top ten are retimed seven times alone. The observed practice leader was 1.17 seconds when checked; standings change.

Unknowns requiring official calibration: page-cache state and performance on the scorer. The available browser session was signed out; team details and competition sign-in are needed for an official submission.

