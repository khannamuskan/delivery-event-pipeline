"""Central configuration for the event pipeline.

Every business rule that a reviewer might want to challenge lives here rather than
being buried inside SQL, so the pipeline's assumptions are inspectable in one place.
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# Business event vocabulary
# ---------------------------------------------------------------------------

# The only event types that are allowed into Silver. Anything else (including an
# unknown future type such as TELEPORTED) is quarantined rather than dropped, so a
# producer shipping a new event type is visible as a data-quality signal instead of
# silently vanishing.
ALLOWED_EVENT_TYPES: Final[tuple[str, ...]] = (
    "ORDER_CREATED",
    "RIDER_ALLOCATED",
    "RIDER_ACCEPTED",
    "STARTED_PICKUP",
    "DELIVERED",
    "CANCELLED",
)

# Lifecycle ordering used only as a deterministic tie-breaker when two events for the
# same order share an identical event_time. It encodes the natural order of the
# delivery funnel; it is never used to *infer* a missing event.
LIFECYCLE_RANK: Final[dict[str, int]] = {
    "ORDER_CREATED": 1,
    "RIDER_ALLOCATED": 2,
    "RIDER_ACCEPTED": 3,
    "STARTED_PICKUP": 4,
    "DELIVERED": 5,
    "CANCELLED": 6,
}

TERMINAL_EVENT_TYPES: Final[tuple[str, ...]] = ("DELIVERED", "CANCELLED")

# ---------------------------------------------------------------------------
# Data-quality thresholds
# ---------------------------------------------------------------------------

# An event is "late" when it reached us more than this long after it happened.
LATENESS_THRESHOLD_SECONDS: Final[int] = 5 * 60

# ---------------------------------------------------------------------------
# Deduplication keys
# ---------------------------------------------------------------------------

# Transport-level identity: the same event_id delivered more than once is an
# at-least-once delivery artefact.
EXACT_DUPLICATE_KEY: Final[tuple[str, ...]] = ("event_id",)

# Business-level identity: the same real-world fact re-emitted by the producer with a
# freshly minted event_id. Note that event_time and rider_id are part of the key, so
# genuinely distinct allocation attempts on the same order are NOT collapsed.
SEMANTIC_DUPLICATE_KEY: Final[tuple[str, ...]] = (
    "order_id",
    "event_type",
    "event_time",
    "rider_id",
    "supplier_id",
)

# ---------------------------------------------------------------------------
# Rejection reason codes (evaluated in this order; first failure wins)
# ---------------------------------------------------------------------------

REJECT_REASONS: Final[tuple[str, ...]] = (
    "UNPARSEABLE_JSON",
    "MISSING_EVENT_ID",
    "MISSING_ORDER_ID",
    "UNKNOWN_EVENT_TYPE",
    "INVALID_EVENT_TIME",
    "INVALID_INGESTED_AT",
)

# ---------------------------------------------------------------------------
# Output layout
# ---------------------------------------------------------------------------

BRONZE_EVENTS: Final[str] = "bronze/events.parquet"
SILVER_EVENTS: Final[str] = "silver/events.parquet"
QUARANTINE_EVENTS: Final[str] = "quarantine/rejected_events.parquet"
GOLD_ORDERS: Final[str] = "gold/orders.parquet"
GOLD_DAILY_METRICS: Final[str] = "gold/daily_metrics.parquet"
REPORT_JSON: Final[str] = "report.json"

# Files listed in the console summary, in the order they are produced.
OUTPUT_MANIFEST: Final[tuple[str, ...]] = (
    BRONZE_EVENTS,
    SILVER_EVENTS,
    QUARANTINE_EVENTS,
    GOLD_ORDERS,
    GOLD_DAILY_METRICS,
    REPORT_JSON,
)

# All timestamps are normalised to UTC. Pinning the session timezone keeps Parquet
# output byte-stable regardless of the host machine's locale.
SESSION_TIMEZONE: Final[str] = "UTC"

# Parquet codec. zstd is deterministic and well supported by every reader we care about.
PARQUET_COMPRESSION: Final[str] = "zstd"
