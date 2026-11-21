#!/usr/bin/env python3
"""Compare a generated data set with semantics-preserving input changes."""
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import tempfile

import polars as pl

binary, generator = (str(Path(p).resolve()) for p in sys.argv[1:3])
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    original = root / "original"
    changed = root / "changed"
    subprocess.run([generator, "--seed", "13", "--events", "10000",
                    "--clicks", "2500", "--output", str(original)], check=True)
    shutil.copytree(original, changed)
    rng = random.Random(17)
    for kind in ("events", "clicks", "links"):
        paths = sorted((changed / kind).glob("*.csv"))
        for path in paths:
            lines = path.read_bytes().splitlines(keepends=True)
            if len(lines) <= 1:
                continue
            header, body = lines[0], lines[1:]
            rng.shuffle(body)
            # An identical click or event copy is safe; duplicate links are safe too.
            body.extend(body[:min(5, len(body))])
            path.write_bytes(header + b"".join(body))
        if paths:
            header = paths[0].read_bytes().splitlines(keepends=True)[0]
            (changed / kind / "extra-empty.csv").write_bytes(header)
    for name, data in (("a", original), ("b", changed)):
        subprocess.run([binary, str(data), str(root / name)],
                       env=dict(os.environ, NCPU="2"), check=True)
    for table, keys in (("user_daily", ["user_id", "day"]),
                        ("campaign_daily", ["campaign_id", "day"])):
        a = pl.read_parquet(root / "a" / table / "*.parquet").sort(keys)
        b = pl.read_parquet(root / "b" / table / "*.parquet").sort(keys)
        if not a.equals(b, null_equal=True):
            raise SystemExit(f"{table}: metamorphic mismatch")
    print("metamorphic comparison passed")
