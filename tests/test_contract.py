"""Output-contract tests.

These assert the things a downstream consumer is entitled to rely on: the files
exist, the columns are present, the grain is what we claim, and the value domains
are respected. They are deliberately independent of the ground-truth oracle, so a
bug in the test oracle cannot mask a contract break.
"""

from __future__ import annotations

import json

import duckdb
import pytest

from conftest import build

REQUIRED_FILES = [
    "bronze/events.parquet",
    "silver/events.parquet",
    "quarantine/rejected_events.parquet",
    "gold/orders.parquet",
    "gold/daily_metrics.parquet",
    "report.json",
]

REQUIRED_ORDER_COLUMNS = {
    "order_id", "created_at", "first_allocated_at", "accepted_at",
    "started_pickup_at", "delivered_at", "cancelled_at", "final_status",
    "allocation_attempts", "unique_riders", "city", "supplier_id",
}

REQUIRED_METRIC_COLUMNS = {
    "date", "orders_created", "orders_delivered", "orders_cancelled",
    "orders_allocated", "orders_accepted", "fulfilment_rate",
    "acceptance_rate", "avg_allocation_attempts",
}

REQUIRED_REPORT_FIELDS = {
    "raw_events", "silver_events", "rejected_events", "duplicate_events_removed",
    "late_events", "unique_orders", "delivered_orders", "cancelled_orders",
    "open_orders",
}


def _columns(path) -> set[str]:
    con = duckdb.connect()
    try:
        return {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')"
            ).fetchall()
        }
    finally:
        con.close()


def test_all_required_outputs_exist(baseline):
    _stats, _report, output_dir = baseline
    for name in REQUIRED_FILES:
        assert (output_dir / name).exists(), f"missing output: {name}"


def test_required_columns_present(baseline):
    _stats, _report, output_dir = baseline
    assert REQUIRED_ORDER_COLUMNS <= _columns(output_dir / "gold" / "orders.parquet")
    assert REQUIRED_METRIC_COLUMNS <= _columns(
        output_dir / "gold" / "daily_metrics.parquet"
    )


def test_report_has_required_fields(baseline):
    _stats, _report, output_dir = baseline
    report = json.loads((output_dir / "report.json").read_text())
    assert REQUIRED_REPORT_FIELDS <= set(report)


def test_orders_grain_is_exactly_one_row_per_order(baseline):
    _stats, _report, output_dir = baseline
    con = duckdb.connect()
    try:
        total, distinct, nulls = con.execute(
            f"""
            SELECT count(*), count(DISTINCT order_id), count(*) FILTER (WHERE order_id IS NULL)
            FROM read_parquet('{(output_dir / 'gold' / 'orders.parquet').as_posix()}')
            """
        ).fetchone()
    finally:
        con.close()
    assert total == distinct
    assert nulls == 0


def test_value_domains_and_invariants(baseline):
    """Rates are NULL or within [0, 1]; statuses are from the allowed set; the
    status breakdown adds up; milestones never contradict the status."""
    _stats, report, output_dir = baseline
    orders = (output_dir / "gold" / "orders.parquet").as_posix()
    metrics = (output_dir / "gold" / "daily_metrics.parquet").as_posix()

    con = duckdb.connect()
    try:
        bad_status = con.execute(
            f"SELECT count(*) FROM read_parquet('{orders}') "
            "WHERE final_status NOT IN ('DELIVERED','CANCELLED','OPEN')"
        ).fetchone()[0]

        bad_rates = con.execute(
            f"""
            SELECT count(*) FROM read_parquet('{metrics}')
            WHERE (fulfilment_rate IS NOT NULL AND (fulfilment_rate < 0 OR fulfilment_rate > 1))
               OR (acceptance_rate IS NOT NULL AND (acceptance_rate < 0 OR acceptance_rate > 1))
            """
        ).fetchone()[0]

        # A DELIVERED order must have a delivered_at; an OPEN order must have neither
        # terminal timestamp. Status and milestones cannot disagree.
        contradictions = con.execute(
            f"""
            SELECT count(*) FROM read_parquet('{orders}')
            WHERE (final_status = 'DELIVERED' AND delivered_at IS NULL)
               OR (final_status = 'CANCELLED' AND cancelled_at IS NULL)
               OR (final_status = 'OPEN' AND (delivered_at IS NOT NULL OR cancelled_at IS NOT NULL))
            """
        ).fetchone()[0]

        # unique_riders can never exceed the number of allocation attempts.
        impossible_riders = con.execute(
            f"SELECT count(*) FROM read_parquet('{orders}') "
            "WHERE unique_riders > allocation_attempts"
        ).fetchone()[0]
    finally:
        con.close()

    assert bad_status == 0
    assert bad_rates == 0
    assert contradictions == 0
    assert impossible_riders == 0
    assert (
        report["delivered_orders"] + report["cancelled_orders"] + report["open_orders"]
        == report["unique_orders"]
    )


def test_report_counts_reconcile(baseline):
    """Every raw row is published, quarantined or deduplicated -- nothing vanishes."""
    _stats, report, _output_dir = baseline
    assert (
        report["silver_events"]
        + report["rejected_events"]
        + report["duplicate_events_removed"]
        == report["raw_events"]
    )
    assert (
        report["exact_duplicates_removed"] + report["semantic_duplicates_removed"]
        == report["duplicate_events_removed"]
    )


def test_bronze_preserves_every_input_line(baseline):
    """Bronze is lossless: one row per non-empty input line, nothing filtered."""
    _stats, report, output_dir = baseline
    con = duckdb.connect()
    try:
        bronze_rows = con.execute(
            f"SELECT count(*) FROM read_parquet("
            f"'{(output_dir / 'bronze' / 'events.parquet').as_posix()}')"
        ).fetchone()[0]
    finally:
        con.close()
    assert bronze_rows == report["raw_events"]


def test_rejected_rows_keep_their_reason_and_payload(baseline):
    """Quarantine must be actionable: a reason code and the original payload."""
    _stats, report, output_dir = baseline
    con = duckdb.connect()
    try:
        rows, missing = con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (WHERE reject_reason IS NULL OR _raw_payload IS NULL)
            FROM read_parquet(
                '{(output_dir / 'quarantine' / 'rejected_events.parquet').as_posix()}')
            """
        ).fetchone()
    finally:
        con.close()
    assert rows == report["rejected_events"]
    assert missing == 0


@pytest.mark.parametrize("orders", [1, 2, 25])
def test_tiny_inputs_do_not_break_the_pipeline(tmp_path, orders):
    """Edge cases: a single order, and a day with a zero denominator."""
    _stats, report, output_dir = build(tmp_path, seed=5, orders=orders)
    assert report["unique_orders"] == orders
    assert (output_dir / "gold" / "daily_metrics.parquet").exists()
