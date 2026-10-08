"""Correctness tests against the generator's ground truth.

This is the core of the validation strategy.

The brief forbids hard-coding expected metric values, and asserting on numbers I
have personally eyeballed would only prove the pipeline still does what it did
yesterday. Instead these tests use the generator as an *oracle*: it can be asked for
the canonical events and the true per-order outcome it intended to produce, and the
pipeline's Gold layer is then compared against that truth field by field.

Because the oracle is regenerated per seed, this verifies behaviour on seeds the
implementation was never tuned against -- which is exactly the property the
evaluator's hidden seeds will test.
"""

from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from conftest import SEEDS, build

ORDER_COLUMNS = [
    "order_id",
    "created_at",
    "first_allocated_at",
    "accepted_at",
    "started_pickup_at",
    "delivered_at",
    "cancelled_at",
    "final_status",
    "allocation_attempts",
    "unique_riders",
    "city",
    "supplier_id",
]

TIMESTAMP_COLUMNS = [
    "created_at",
    "first_allocated_at",
    "accepted_at",
    "started_pickup_at",
    "delivered_at",
    "cancelled_at",
]


def _as_naive_utc(value: str | None) -> datetime | None:
    """Convert the generator's '...Z' ISO strings to the naive-UTC form Gold uses."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def _load_orders(output_dir) -> dict[str, dict]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT {', '.join(ORDER_COLUMNS)} "
            f"FROM read_parquet('{(output_dir / 'gold' / 'orders.parquet').as_posix()}')"
        ).fetchall()
    finally:
        con.close()
    return {row[0]: dict(zip(ORDER_COLUMNS, row)) for row in rows}


@pytest.mark.parametrize("seed", SEEDS)
def test_gold_orders_match_generator_truth(tmp_path, seed):
    """Every Gold order must match the outcome the generator actually simulated."""
    stats, _report, output_dir = build(tmp_path, seed=seed, orders=400, include_truth=True)
    truth = stats["truth_orders"]
    actual = _load_orders(output_dir)

    assert set(actual) == set(truth), "Gold orders and truth orders differ in membership"

    mismatches: list[str] = []
    for order_id, expected in truth.items():
        got = actual[order_id]
        for column in ORDER_COLUMNS:
            want = expected[column]
            if column in TIMESTAMP_COLUMNS:
                want = _as_naive_utc(want)
            if got[column] != want:
                mismatches.append(
                    f"{order_id}.{column}: expected {want!r}, got {got[column]!r}"
                )

    assert not mismatches, "Gold diverged from ground truth:\n" + "\n".join(mismatches[:25])


@pytest.mark.parametrize("seed", SEEDS)
def test_silver_recovers_exactly_the_canonical_stream(tmp_path, seed):
    """Silver must equal the canonical event set: no survivors lost, no noise kept.

    This is the strongest statement that can be made about the validation and
    deduplication stages together. If dedup were too aggressive (e.g. keyed only on
    order_id + event_type) reallocation events would vanish; if it were too timid,
    injected duplicates would survive. Both show up here.
    """
    stats, report, output_dir = build(tmp_path, seed=seed, orders=400, include_truth=True)

    assert report["silver_events"] == stats["canonical_events"]
    assert report["rejected_events"] == stats["invalid_records_injected"]
    assert report["exact_duplicates_removed"] == stats["exact_duplicates_injected"]
    assert report["semantic_duplicates_removed"] == stats["semantic_duplicates_injected"]
    assert report["late_events"] == stats["late_canonical_events"]

    # Identity, not just cardinality: compare the actual business keys.
    canonical_keys = {
        (e["order_id"], e["event_type"], e["event_time"], e["rider_id"], e["supplier_id"])
        for e in stats["canonical_rows"]
    }
    con = duckdb.connect()
    try:
        silver_keys = set(
            con.execute(
                f"""
                SELECT order_id, event_type,
                       strftime(event_time, '%Y-%m-%dT%H:%M:%SZ'),
                       rider_id, supplier_id
                FROM read_parquet('{(output_dir / 'silver' / 'events.parquet').as_posix()}')
                """
            ).fetchall()
        )
    finally:
        con.close()

    assert silver_keys == canonical_keys


@pytest.mark.parametrize("seed", SEEDS)
def test_daily_metrics_are_internally_consistent(tmp_path, seed):
    """Daily metrics must be derivable from Gold orders, with no invented rows."""
    _stats, report, output_dir = build(tmp_path, seed=seed, orders=400)

    con = duckdb.connect()
    try:
        orders = (output_dir / "gold" / "orders.parquet").as_posix()
        metrics = (output_dir / "gold" / "daily_metrics.parquet").as_posix()
        diff = con.execute(
            f"""
            WITH expected AS (
                SELECT cast(created_at AS DATE) AS date,
                       count(*) AS orders_created,
                       count(*) FILTER (WHERE final_status = 'DELIVERED') AS orders_delivered,
                       count(*) FILTER (WHERE final_status = 'CANCELLED') AS orders_cancelled,
                       count(*) FILTER (WHERE first_allocated_at IS NOT NULL) AS orders_allocated,
                       count(*) FILTER (WHERE accepted_at IS NOT NULL) AS orders_accepted
                FROM read_parquet('{orders}')
                WHERE created_at IS NOT NULL
                GROUP BY 1
            )
            SELECT count(*) FROM expected e
            FULL OUTER JOIN read_parquet('{metrics}') m USING (date)
            WHERE e.orders_created    IS DISTINCT FROM m.orders_created
               OR e.orders_delivered  IS DISTINCT FROM m.orders_delivered
               OR e.orders_cancelled  IS DISTINCT FROM m.orders_cancelled
               OR e.orders_allocated  IS DISTINCT FROM m.orders_allocated
               OR e.orders_accepted   IS DISTINCT FROM m.orders_accepted
            """
        ).fetchone()[0]

        # Totals across all days must account for every attributable order.
        total_created = con.execute(
            f"SELECT sum(orders_created) FROM read_parquet('{metrics}')"
        ).fetchone()[0]
    finally:
        con.close()

    assert diff == 0, "daily_metrics disagrees with gold/orders"
    assert total_created == report["unique_orders"] - report["orders_missing_created_at"]
