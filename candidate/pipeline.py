"""Bronze -> Silver -> Gold event pipeline.

Design in one paragraph
-----------------------
This module is a thin, boring orchestrator. All business logic lives in versioned
SQL models under ``candidate/sql/``, executed in order against an in-process DuckDB
database, and all tunable rules live in ``candidate/config.py``. That split is
deliberate: the transformation logic stays declarative and reviewable without
reading any Python, and it is portable -- the same CTEs and window functions move to
Snowflake, BigQuery or Spark SQL unchanged when the volume outgrows a single node.

Why DuckDB
----------
The working set here is tens of thousands of rows. A distributed engine would add
cluster overhead, JVM start-up and operational surface area to a job that completes
in under a second, while making the output *less* deterministic. DuckDB reads JSONL
and writes Parquet natively, needs no infrastructure, and runs identically on a
laptop and in the container. See DECISIONS.md for where that choice stops holding.

Determinism
-----------
Required by the brief, and enforced structurally rather than hoped for:
  * the session timezone is pinned to UTC, so output does not depend on the host;
  * no wall-clock value is ever written into an artefact;
  * no business rule or tie-breaker reads file position -- every ordering is over
    data columns with a unique final tie-breaker.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb

from candidate import config, reporting

LOGGER = logging.getLogger("pipeline")

SQL_DIR = Path(__file__).parent / "sql"

# Executed in order. Each file is a layer; the numbering is the dependency graph.
SQL_MODELS: tuple[str, ...] = (
    "01_bronze.sql",
    "02_silver_validated.sql",
    "03_silver_dedup.sql",
    "04_gold_orders.sql",
    "05_gold_daily_metrics.sql",
)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # Pin the timezone so timestamp parsing and date bucketing are identical on
    # every machine. Without this, the same input could bucket into different days
    # depending on where the job happened to run.
    con.execute(f"SET TimeZone='{config.SESSION_TIMEZONE}'")
    # Preserve insertion order so Bronze's _source_row genuinely reflects file order
    # (used for audit only, never for business logic).
    con.execute("SET preserve_insertion_order=true")
    return con


def _register_reference_data(con: duckdb.DuckDBPyConnection) -> None:
    """Materialise the supported event vocabulary from config as a table.

    Modelling the vocabulary as data rather than as literals inside SQL gives the
    allowed types, their lifecycle order and their terminal flag exactly one source
    of truth. Adding an event type is a config change, not a SQL rewrite.
    """
    rows = [
        (
            event_type,
            config.LIFECYCLE_RANK[event_type],
            event_type in config.TERMINAL_EVENT_TYPES,
        )
        for event_type in config.ALLOWED_EVENT_TYPES
    ]
    con.execute(
        """
        CREATE OR REPLACE TABLE ref_event_types (
            event_type     VARCHAR PRIMARY KEY,
            lifecycle_rank INTEGER NOT NULL,
            is_terminal    BOOLEAN NOT NULL
        )
        """
    )
    con.executemany("INSERT INTO ref_event_types VALUES (?, ?, ?)", rows)


def _split_statements(sql: str) -> list[str]:
    """Split a model file into executable statements on top-level semicolons.

    A naive ``sql.split(";")`` is wrong here: the models are heavily commented and a
    semicolon (or an apostrophe) inside a comment or a string literal would corrupt
    the split. This walks the text tracking whether it is inside a line comment or a
    single-quoted literal, and only treats a semicolon as a terminator outside both.
    """
    statements: list[str] = []
    current: list[str] = []
    in_comment = False
    in_literal = False
    i = 0
    while i < len(sql):
        char = sql[i]

        if in_comment:
            current.append(char)
            if char == "\n":
                in_comment = False
            i += 1
            continue

        if in_literal:
            current.append(char)
            # '' is an escaped quote inside a SQL string literal.
            if char == "'":
                if i + 1 < len(sql) and sql[i + 1] == "'":
                    current.append(sql[i + 1])
                    i += 2
                    continue
                in_literal = False
            i += 1
            continue

        if char == "-" and sql.startswith("--", i):
            in_comment = True
            current.append(char)
            i += 1
            continue

        if char == "'":
            in_literal = True
            current.append(char)
            i += 1
            continue

        if char == ";":
            statements.append("".join(current))
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    statements.append("".join(current))
    return [s for s in statements if _is_executable(s)]


def _is_executable(statement: str) -> bool:
    """True if a fragment contains anything other than comments and whitespace."""
    body = "\n".join(
        line for line in statement.splitlines() if not line.strip().startswith("--")
    )
    return bool(body.strip())


def _params_for(statement: str, params: dict[str, Any]) -> dict[str, Any]:
    """Bind only the parameters a statement actually references.

    DuckDB rejects a named parameter that does not appear in the statement, so each
    statement receives exactly the subset it uses.
    """
    return {k: v for k, v in params.items() if f"${k}" in statement}


def _run_models(con: duckdb.DuckDBPyConnection, input_path: Path) -> None:
    params: dict[str, Any] = {
        # Bound as a parameter rather than interpolated: the path never becomes part
        # of the SQL text.
        "input_path": str(input_path),
        "lateness_threshold_seconds": config.LATENESS_THRESHOLD_SECONDS,
    }
    for model in SQL_MODELS:
        sql = (SQL_DIR / model).read_text(encoding="utf-8")
        LOGGER.debug("executing model %s", model)
        for statement in _split_statements(sql):
            bound = _params_for(statement, params)
            if bound:
                con.execute(statement, bound)
            else:
                con.execute(statement)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _write_parquet(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    output_dir: Path,
    relative_path: str,
    order_by: str,
) -> None:
    """Write a table to Parquet with an explicit, total ordering.

    The ORDER BY is not cosmetic. Two runs over the same input must produce
    byte-comparable files, and that only holds if row order is pinned by data rather
    than left to whatever the execution engine happens to do that day.
    """
    target = output_dir / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (SELECT * FROM {relation} ORDER BY {order_by})
        TO '{target.as_posix()}'
        (FORMAT PARQUET, COMPRESSION '{config.PARQUET_COMPRESSION}')
        """
    )
    LOGGER.debug("wrote %s", target)


# ---------------------------------------------------------------------------
# Metrics collection
# ---------------------------------------------------------------------------


def _collect_counts(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    def scalar(sql: str) -> Any:
        return con.execute(sql).fetchone()[0]

    raw_events = scalar("SELECT count(*) FROM bronze_events")
    rejected_events = scalar("SELECT count(*) FROM rejected_events")
    valid_events = scalar(
        "SELECT count(*) FROM silver_validated WHERE reject_reason IS NULL"
    )
    after_exact = scalar("SELECT count(*) FROM silver_exact_deduped")
    silver_events = scalar("SELECT count(*) FROM silver_events")

    # Lateness is measured on published Silver, i.e. on distinct real facts. Counting
    # it on Bronze would inflate the number with redelivered duplicates and make the
    # metric a function of transport noise rather than of pipeline freshness.
    late_events = scalar("SELECT count(*) FROM silver_events WHERE is_late")

    status_rows = dict(
        con.execute("SELECT final_status, count(*) FROM gold_orders GROUP BY 1").fetchall()
    )

    rejected_by_reason = dict(
        con.execute(
            """
            SELECT reject_reason, count(*) AS n
            FROM rejected_events
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
    )

    return {
        "raw_events": raw_events,
        "silver_events": silver_events,
        "rejected_events": rejected_events,
        "exact_duplicates_removed": valid_events - after_exact,
        "semantic_duplicates_removed": after_exact - silver_events,
        "duplicate_events_removed": valid_events - silver_events,
        "late_events": late_events,
        "late_event_rate": (late_events / silver_events) if silver_events else None,
        "unique_orders": scalar("SELECT count(*) FROM gold_orders"),
        "delivered_orders": status_rows.get("DELIVERED", 0),
        "cancelled_orders": status_rows.get("CANCELLED", 0),
        "open_orders": status_rows.get("OPEN", 0),
        "orders_missing_created_at": scalar(
            "SELECT count(*) FROM gold_orders WHERE created_at IS NULL"
        ),
        "rejected_by_reason": rejected_by_reason,
        "reporting_days": scalar("SELECT count(*) FROM gold_daily_metrics"),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run(input_path: str | Path, output_dir: str | Path) -> dict[str, Any]:
    """Execute the full Bronze -> Silver -> Gold pipeline.

    Returns the report dict so tests and callers can assert on the numbers without
    re-reading report.json from disk.
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    if not input_path.exists():
        raise FileNotFoundError(f"Input event stream not found: {input_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    con = _connect()
    try:
        _register_reference_data(con)
        _run_models(con, input_path)

        _write_parquet(
            con, "bronze_events", output_dir, config.BRONZE_EVENTS,
            order_by="_source_row",
        )
        _write_parquet(
            con, "silver_events", output_dir, config.SILVER_EVENTS,
            order_by="order_id, event_time, event_id",
        )
        _write_parquet(
            con, "rejected_events", output_dir, config.QUARANTINE_EVENTS,
            order_by="_source_row",
        )
        _write_parquet(
            con, "gold_orders", output_dir, config.GOLD_ORDERS,
            order_by="order_id",
        )
        _write_parquet(
            con, "gold_daily_metrics", output_dir, config.GOLD_DAILY_METRICS,
            order_by="date",
        )

        counts = _collect_counts(con)
        # Publish only if the layers balance. A pipeline that cannot account for
        # every input row has no business emitting metrics.
        reporting.assert_reconciles(counts)

        report = reporting.build_report(counts)
        reporting.write_report(report, output_dir)
        reporting.print_summary(report)
        return report
    finally:
        con.close()
