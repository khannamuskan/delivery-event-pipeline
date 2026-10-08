
from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

ALLOWED_EVENTS = {
    "ORDER_CREATED",
    "RIDER_ALLOCATED",
    "RIDER_ACCEPTED",
    "STARTED_PICKUP",
    "DELIVERED",
    "CANCELLED",
}

CITIES = ["Mumbai", "Delhi", "Bengaluru", "Pune", "Hyderabad"]
SUPPLIERS = [f"supplier_{i:02d}" for i in range(1, 9)]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _event(
    event_id: str,
    order_id: str,
    event_type: str,
    event_time: datetime,
    ingested_at: datetime,
    city: str | None,
    rider_id: str | None,
    supplier_id: str | None,
) -> Dict[str, Any]:
    return {
        "event_id": event_id,
        "order_id": order_id,
        "event_type": event_type,
        "event_time": _iso(event_time),
        "ingested_at": _iso(ingested_at),
        "city": city,
        "rider_id": rider_id,
        "supplier_id": supplier_id,
    }


def _business_key(e: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        e.get("order_id"),
        e.get("event_type"),
        e.get("event_time"),
        e.get("rider_id"),
        e.get("supplier_id"),
    )


def generate_dataset(
    seed: int,
    orders: int,
    output_path: str | Path,
    include_truth: bool = False,
) -> Dict[str, Any]:
    """
    Generate a deterministic event stream with realistic data-quality issues.

    The raw stream contains:
      - exact duplicates (same event_id)
      - semantic duplicates (same business event, different event_id)
      - out-of-order arrival
      - late-arriving records
      - structurally invalid records
      - unknown event types
      - missing intermediate lifecycle events
      - multiple rider allocation attempts

    If include_truth=True, canonical events and order truth are returned in-memory.
    The candidate starter never writes truth to disk.
    """
    rng = random.Random(seed)
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)

    canonical: List[Dict[str, Any]] = []
    emitted: List[Dict[str, Any]] = []
    truth_orders: Dict[str, Dict[str, Any]] = {}

    event_seq = 1

    def next_event_id(prefix: str = "evt") -> str:
        nonlocal event_seq
        eid = f"{prefix}_{event_seq:09d}"
        event_seq += 1
        return eid

    for idx in range(1, orders + 1):
        order_id = f"ord_{idx:07d}"
        city = rng.choice(CITIES)
        supplier_id = rng.choice(SUPPLIERS)
        created_at = start + timedelta(
            days=rng.randint(0, 6),
            seconds=rng.randint(0, 86399),
        )

        order_events: List[Dict[str, Any]] = []
        created = _event(
            next_event_id(),
            order_id,
            "ORDER_CREATED",
            created_at,
            created_at + timedelta(seconds=rng.randint(0, 20)),
            city,
            None,
            supplier_id,
        )
        order_events.append(created)

        allocation_attempts = rng.randint(1, 3)
        accepted = rng.random() < 0.80
        accepted_rider = None
        latest_supplier = supplier_id
        cursor = created_at

        for attempt in range(1, allocation_attempts + 1):
            cursor += timedelta(seconds=rng.randint(20, 160))
            rider_id = f"rider_{rng.randint(1, max(50, orders // 10)):06d}"
            latest_supplier = rng.choice(SUPPLIERS)

            alloc = _event(
                next_event_id(),
                order_id,
                "RIDER_ALLOCATED",
                cursor,
                cursor + timedelta(seconds=rng.randint(0, 25)),
                city,
                rider_id,
                latest_supplier,
            )
            order_events.append(alloc)

            # Only the final allocation attempt may be accepted.
            if accepted and attempt == allocation_attempts:
                cursor += timedelta(seconds=rng.randint(5, 90))
                accepted_rider = rider_id
                accept = _event(
                    next_event_id(),
                    order_id,
                    "RIDER_ACCEPTED",
                    cursor,
                    cursor + timedelta(seconds=rng.randint(0, 20)),
                    city,
                    rider_id,
                    latest_supplier,
                )
                order_events.append(accept)

        final_status = "OPEN"

        if accepted:
            outcome = rng.random()
            if outcome < 0.82:
                # Sometimes the pickup event is missing even though delivery exists.
                if rng.random() < 0.90:
                    cursor += timedelta(minutes=rng.randint(2, 15))
                    pickup = _event(
                        next_event_id(),
                        order_id,
                        "STARTED_PICKUP",
                        cursor,
                        cursor + timedelta(seconds=rng.randint(0, 20)),
                        city,
                        accepted_rider,
                        latest_supplier,
                    )
                    order_events.append(pickup)

                cursor += timedelta(minutes=rng.randint(8, 45))
                delivered = _event(
                    next_event_id(),
                    order_id,
                    "DELIVERED",
                    cursor,
                    cursor + timedelta(seconds=rng.randint(0, 30)),
                    city,
                    accepted_rider,
                    latest_supplier,
                )
                order_events.append(delivered)
                final_status = "DELIVERED"

            elif outcome < 0.94:
                cursor += timedelta(minutes=rng.randint(1, 20))
                cancelled = _event(
                    next_event_id(),
                    order_id,
                    "CANCELLED",
                    cursor,
                    cursor + timedelta(seconds=rng.randint(0, 20)),
                    city,
                    accepted_rider,
                    latest_supplier,
                )
                order_events.append(cancelled)
                final_status = "CANCELLED"
        else:
            if rng.random() < 0.72:
                cursor += timedelta(minutes=rng.randint(1, 15))
                cancelled = _event(
                    next_event_id(),
                    order_id,
                    "CANCELLED",
                    cursor,
                    cursor + timedelta(seconds=rng.randint(0, 20)),
                    city,
                    None,
                    latest_supplier,
                )
                order_events.append(cancelled)
                final_status = "CANCELLED"

        # Make some canonical events late-arriving.
        for e in order_events:
            if rng.random() < 0.06:
                event_dt = datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))
                late_by = timedelta(minutes=rng.randint(6, 180))
                e["ingested_at"] = _iso(event_dt + late_by)

        canonical.extend(order_events)

        truth_orders[order_id] = {
            "order_id": order_id,
            "created_at": created["event_time"],
            "first_allocated_at": min(
                e["event_time"] for e in order_events if e["event_type"] == "RIDER_ALLOCATED"
            ),
            "accepted_at": min(
                (e["event_time"] for e in order_events if e["event_type"] == "RIDER_ACCEPTED"),
                default=None,
            ),
            "started_pickup_at": min(
                (e["event_time"] for e in order_events if e["event_type"] == "STARTED_PICKUP"),
                default=None,
            ),
            "delivered_at": min(
                (e["event_time"] for e in order_events if e["event_type"] == "DELIVERED"),
                default=None,
            ),
            "cancelled_at": min(
                (e["event_time"] for e in order_events if e["event_type"] == "CANCELLED"),
                default=None,
            ),
            "final_status": final_status,
            "allocation_attempts": sum(
                1 for e in order_events if e["event_type"] == "RIDER_ALLOCATED"
            ),
            "unique_riders": len({
                e["rider_id"]
                for e in order_events
                if e["event_type"] == "RIDER_ALLOCATED" and e.get("rider_id")
            }),
            "city": city,
            "supplier_id": latest_supplier,
        }

    # Start with canonical events.
    emitted.extend(dict(e) for e in canonical)

    # Exact duplicates: same event_id and same business payload.
    exact_dupes = 0
    for e in canonical:
        if rng.random() < 0.035:
            emitted.append(dict(e))
            exact_dupes += 1

    # Semantic duplicates: same business event but different event_id.
    semantic_dupes = 0
    for e in canonical:
        if rng.random() < 0.020:
            dup = dict(e)
            dup["event_id"] = next_event_id("dup")
            emitted.append(dup)
            semantic_dupes += 1

    # Structurally invalid but syntactically valid JSON records.
    invalid_records: List[Dict[str, Any]] = []
    invalid_count = max(8, int(max(1, orders) * 0.01))
    for i in range(invalid_count):
        base_order = f"ord_{rng.randint(1, orders):07d}"
        kind = i % 5
        if kind == 0:
            bad = {
                "event_id": next_event_id("bad"),
                "order_id": None,
                "event_type": "RIDER_ACCEPTED",
                "event_time": _iso(start),
                "ingested_at": _iso(start),
                "city": rng.choice(CITIES),
                "rider_id": "rider_bad",
                "supplier_id": rng.choice(SUPPLIERS),
            }
        elif kind == 1:
            bad = {
                "event_id": next_event_id("bad"),
                "order_id": base_order,
                "event_type": "TELEPORTED",
                "event_time": _iso(start),
                "ingested_at": _iso(start),
                "city": rng.choice(CITIES),
                "rider_id": None,
                "supplier_id": rng.choice(SUPPLIERS),
            }
        elif kind == 2:
            bad = {
                "event_id": next_event_id("bad"),
                "order_id": base_order,
                "event_type": "DELIVERED",
                "event_time": "not-a-timestamp",
                "ingested_at": _iso(start),
                "city": rng.choice(CITIES),
                "rider_id": None,
                "supplier_id": rng.choice(SUPPLIERS),
            }
        elif kind == 3:
            bad = {
                "event_id": None,
                "order_id": base_order,
                "event_type": "ORDER_CREATED",
                "event_time": _iso(start),
                "ingested_at": _iso(start),
                "city": rng.choice(CITIES),
                "rider_id": None,
                "supplier_id": rng.choice(SUPPLIERS),
            }
        else:
            bad = {
                "event_id": next_event_id("bad"),
                "order_id": base_order,
                "event_type": None,
                "event_time": _iso(start),
                "ingested_at": _iso(start),
                "city": rng.choice(CITIES),
                "rider_id": None,
                "supplier_id": rng.choice(SUPPLIERS),
            }
        invalid_records.append(bad)

    emitted.extend(invalid_records)

    # Arrival order is intentionally unrelated to event_time.
    rng.shuffle(emitted)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for row in emitted:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")

    late_events = 0
    for e in canonical:
        et = datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))
        it = datetime.fromisoformat(e["ingested_at"].replace("Z", "+00:00"))
        if (it - et) > timedelta(minutes=5):
            late_events += 1

    summary = {
        "seed": seed,
        "orders": orders,
        "raw_events": len(emitted),
        "canonical_events": len(canonical),
        "exact_duplicates_injected": exact_dupes,
        "semantic_duplicates_injected": semantic_dupes,
        "invalid_records_injected": len(invalid_records),
        "late_canonical_events": late_events,
    }

    if include_truth:
        summary["canonical_rows"] = canonical
        summary["truth_orders"] = truth_orders

    return summary


if __name__ == "__main__":
    seed = int(os.getenv("SEED", "42"))
    orders = int(os.getenv("ORDERS", "1000"))
    output = os.getenv("EVENT_OUTPUT", "/app/data/events.jsonl")
    stats = generate_dataset(seed, orders, output, include_truth=False)
    print(json.dumps(stats, indent=2))
