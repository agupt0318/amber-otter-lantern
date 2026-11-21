# Shopping Sessions

Native Rust implementation of the Builders Shopping Sessions track.

```sh
cargo build --release
DATA_DIR=/path/to/input OUTPUT_DIR=/path/to/output target/release/shopping-sessions
```

The executable also accepts input and output as its first two arguments for the supplied fixture harness. Set `SHOPPING_METRICS=1` for stage timings on stderr. `NCPU` controls the sort worker pool.

## Verification

With Python 3.12+ and Polars 2.1 installed:

```sh
cargo test --locked
cargo build --release --locked
python scripts/run_fixtures.py fixtures -- target/release/shopping-sessions
python scripts/compare.py /path/to/output /path/to/dev-answer
python tests/reference.py /path/to/small-input /path/to/output
python tests/metamorphic.py target/release/shopping-sessions /path/to/generate
```

The reference implementation is deliberately slow and intended for small generated datasets. The fixture and output-comparison scripts come from the supplied starter kit.

Build the scorer-platform image with `docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .`. Run it with `IMAGE=shopping-sessions:latest scripts/docker_run.sh /path/to/input /path/to/output`.

## Performance status

The program is a correctness-first native implementation. See `PERF.md` for measured results and remaining work. It has not yet achieved a competitive official score. The default uses parallel event reduction and validates rows in file order. Set `SHOPPING_SERIAL_DEDUP=1` or `SHOPPING_UNORDERED_ROWS=1` to compare alternative paths. Do not assume local timings predict scorer timings.

