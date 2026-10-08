"""Determinism tests.

The brief requires that the same input produces the same output on every run.
Determinism is a property that is easy to claim and easy to lose: an unordered
window function, a hash-join ordering, a ``now()`` column, or a local timezone can
all silently break it. These tests fail loudly when that happens.

Note the comparison is on *content* hashes of the sorted rows, not on raw file
bytes. Parquet encodes writer metadata and block layout that can legitimately vary;
what we promise downstream is identical data, not identical bytes.
"""

from __future__ import annotations

import hashlib
import json

import duckdb
import pytest

from conftest import build

ARTIFACTS = [
    "bronze/events.parquet",
    "silver/events.parquet",
    "quarantine/rejected_events.parquet",
    "gold/orders.parquet",
    "gold/daily_metrics.parquet",
]


def _content_hash(path) -> str:
    """Hash the full contents of a Parquet file in a stable, row-order-free way.

    Each row is cast to its text form, hashed independently, and the per-row hashes
    are sorted before being folded together. That makes the hash sensitive to every
    value in the table but insensitive to physical row order -- so this function
    tests data equality, while the separate row-order test below tests ordering.
    """
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT to_json(t) FROM read_parquet('{path.as_posix()}') AS t"
        ).fetchall()
    finally:
        con.close()
    digests = sorted(hashlib.sha256(row[0].encode()).hexdigest() for row in rows)
    return hashlib.sha256("".join(digests).encode()).hexdigest()


def _row_order_hash(path) -> str:
    """Hash the rows in the order they are physically stored in the file."""
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT to_json(t) FROM read_parquet('{path.as_posix()}') AS t"
        ).fetchall()
    finally:
        con.close()
    return hashlib.sha256("".join(row[0] for row in rows).encode()).hexdigest()


@pytest.mark.parametrize("seed", [42, 2026])
def test_two_runs_of_the_same_seed_are_identical(tmp_path, seed):
    first = tmp_path / "run_a"
    second = tmp_path / "run_b"
    _s1, report_a, out_a = build(first, seed=seed, orders=400)
    _s2, report_b, out_b = build(second, seed=seed, orders=400)

    assert report_a == report_b, "report.json differs between identical runs"

    for name in ARTIFACTS:
        assert _content_hash(out_a / name) == _content_hash(out_b / name), (
            f"{name} content differs between identical runs"
        )
        assert _row_order_hash(out_a / name) == _row_order_hash(out_b / name), (
            f"{name} row order differs between identical runs"
        )


def test_report_contains_no_wall_clock_values(baseline):
    """A run timestamp in report.json would make every run differ from the last.

    Run metadata belongs in logs or an orchestration layer, not in a data artifact
    whose job is to be byte-comparable across runs.
    """
    _stats, _report, output_dir = baseline
    report = json.loads((output_dir / "report.json").read_text())
    forbidden = {"generated_at", "run_at", "timestamp", "run_timestamp", "created_at"}
    assert not (forbidden & set(report))
    assert all(isinstance(v, (int, float, dict)) for v in report.values()), (
        "report values should be numeric or nested counts, not free-form strings"
    )


def test_different_seeds_produce_different_data(tmp_path):
    """Guards against the opposite failure: a pipeline that is 'deterministic'
    because it is accidentally ignoring its input."""
    _s1, _r1, out_a = build(tmp_path / "a", seed=42, orders=400)
    _s2, _r2, out_b = build(tmp_path / "b", seed=7, orders=400)
    assert _content_hash(out_a / "gold/orders.parquet") != _content_hash(
        out_b / "gold/orders.parquet"
    )
