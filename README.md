# Shopping Sessions

This repository builds an amd64 Docker image for the Builders Shopping Sessions track.

The pipeline parses compact event records, partitions them by event ID for deduplication, routes survivors by person, and processes person partitions in parallel. It writes multiple Parquet parts. The Docker build targets x86-64-v3; the scorer CPU must support that instruction set.

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
IMAGE=shopping-sessions:latest scripts/docker_run.sh /path/to/data /path/to/output
```

Local verification against the preceding implementation: 36 fixtures, the published 1M dev answers inside an amd64 container, and exact logical output equality on 20M and 60M generated inputs. Local timing is not a leaderboard score. See [PERF.md](PERF.md) for prior experiments and [THIRD_PARTY_PERFORMANCE.md](THIRD_PARTY_PERFORMANCE.md) for this candidate's measurements and limitations.
