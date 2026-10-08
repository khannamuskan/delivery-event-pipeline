"""Report assembly and console summary.

Kept separate from the transformation logic so that "what we computed" and "how we
present it" can change independently.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from candidate import config


class ReconciliationError(RuntimeError):
    """Raised when the row-count identity across layers does not balance."""


def build_report(counts: Mapping[str, Any]) -> dict[str, Any]:
    """Assemble report.json.

    The required fields come first, followed by the extra diagnostics that make the
    numbers explainable. Deliberately contains no wall-clock timestamp or run id:
    the pipeline must produce identical output for identical input, and a now()
    field would break that for no analytical benefit.
    """
    report: dict[str, Any] = {
        # --- required by the brief -------------------------------------------
        "raw_events": counts["raw_events"],
        "silver_events": counts["silver_events"],
        "rejected_events": counts["rejected_events"],
        "duplicate_events_removed": counts["duplicate_events_removed"],
        "late_events": counts["late_events"],
        "unique_orders": counts["unique_orders"],
        "delivered_orders": counts["delivered_orders"],
        "cancelled_orders": counts["cancelled_orders"],
        "open_orders": counts["open_orders"],
        # --- additional data-quality detail ----------------------------------
        # The two duplicate classes are reported separately because they point at
        # different upstream problems: exact duplicates mean the transport is
        # redelivering, semantic duplicates mean a producer is retrying. Fixing
        # them requires talking to different teams.
        "exact_duplicates_removed": counts["exact_duplicates_removed"],
        "semantic_duplicates_removed": counts["semantic_duplicates_removed"],
        "rejected_by_reason": counts["rejected_by_reason"],
        "orders_missing_created_at": counts["orders_missing_created_at"],
        "late_event_rate": counts["late_event_rate"],
        "reporting_days": counts["reporting_days"],
    }
    return report


def assert_reconciles(counts: Mapping[str, Any]) -> None:
    """Fail loudly if events have gone missing between layers.

    Every raw row must end up in exactly one of three buckets: published to Silver,
    quarantined as invalid, or removed as a duplicate. If this identity does not
    hold, the pipeline has lost data somewhere and no downstream number can be
    trusted -- so we refuse to publish rather than emit a plausible-looking lie.
    """
    expected = (
        counts["silver_events"]
        + counts["rejected_events"]
        + counts["duplicate_events_removed"]
    )
    if expected != counts["raw_events"]:
        raise ReconciliationError(
            "Row counts do not reconcile: "
            f"raw_events={counts['raw_events']} but "
            f"silver({counts['silver_events']}) + rejected({counts['rejected_events']}) "
            f"+ duplicates({counts['duplicate_events_removed']}) = {expected}"
        )


def write_report(report: Mapping[str, Any], output_dir: Path) -> Path:
    path = output_dir / config.REPORT_JSON
    path.parent.mkdir(parents=True, exist_ok=True)
    # sort_keys=False keeps the required fields at the top where a reviewer looks
    # first; the content is deterministic either way.
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return path


def _n(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.4f}"
    return f"{value:,}"


def print_summary(report: Mapping[str, Any]) -> None:
    """Human-readable console summary."""
    width = 26

    def line(label: str, value: Any) -> None:
        print(f"{label:<{width}}{_n(value):>12}")

    print()
    print("PIPELINE COMPLETE")
    print("=================")
    print()
    print("Input")
    print("-----")
    line("Raw events:", report["raw_events"])
    line("Silver events:", report["silver_events"])
    line("Rejected events:", report["rejected_events"])
    line("Duplicate events removed:", report["duplicate_events_removed"])
    line("  exact duplicates:", report["exact_duplicates_removed"])
    line("  semantic duplicates:", report["semantic_duplicates_removed"])
    print()
    print("Orders")
    print("------")
    line("Unique orders:", report["unique_orders"])
    line("Delivered:", report["delivered_orders"])
    line("Cancelled:", report["cancelled_orders"])
    line("Open:", report["open_orders"])
    print()
    print("Data Quality")
    print("------------")
    line("Late events:", report["late_events"])
    line("Late event rate:", report["late_event_rate"])
    line("Orders w/o created_at:", report["orders_missing_created_at"])
    if report["rejected_by_reason"]:
        print()
        print("Rejections by reason")
        print("--------------------")
        for reason, count in report["rejected_by_reason"].items():
            line(f"  {reason}:", count)
    print()
    print("Output")
    print("------")
    for name in config.OUTPUT_MANIFEST:
        print(f"  {name}")
    print()
