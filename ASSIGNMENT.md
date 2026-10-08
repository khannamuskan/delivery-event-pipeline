# Assignment brief (provided)

> This is the original assessment brief exactly as it was provided, preserved
> verbatim so the requirements and the implementation can be read side by side.
> See [README.md](README.md) for how to run the pipeline and
> [DECISIONS.md](DECISIONS.md) for the reasoning behind each design choice.


## Goal

Build a small, production-minded data pipeline that turns a deliberately messy event stream into reliable Bronze, Silver and Gold datasets.

The assessment is intentionally small. We care more about correctness, modelling decisions and reasoning than framework size.

## Run

```bash
docker build -t data-engineer-assessment .
docker run --rm data-engineer-assessment
```

You may change the seed and size:

```bash
docker run --rm \
  -e SEED=42 \
  -e ORDERS=5000 \
  data-engineer-assessment
```

To inspect generated output files locally:

```bash
mkdir -p output
docker run --rm \
  -e SEED=42 \
  -e ORDERS=5000 \
  -v "$(pwd)/output:/app/output" \
  data-engineer-assessment
```

The evaluator will use additional hidden seeds.

## Input

The container generates `/app/data/events.jsonl`.

Each row is an event with fields similar to:

```json
{
  "event_id": "evt_000000123",
  "order_id": "ord_0000123",
  "event_type": "RIDER_ACCEPTED",
  "event_time": "2026-09-03T10:04:11Z",
  "ingested_at": "2026-09-03T10:04:18Z",
  "city": "Mumbai",
  "rider_id": "rider_000231",
  "supplier_id": "supplier_03"
}
```

Supported business event types:

- `ORDER_CREATED`
- `RIDER_ALLOCATED`
- `RIDER_ACCEPTED`
- `STARTED_PICKUP`
- `DELIVERED`
- `CANCELLED`

The stream deliberately contains real-world problems including duplicates, semantic duplicates, late records, out-of-order arrival, invalid rows, unknown event types, missing intermediate events and multiple allocation attempts.

## Your task

Implement the pipeline in `candidate/pipeline.py`. You may create any additional modules you need.

You may use Python, DuckDB, Polars, Pandas, PySpark, or another reasonable tool. If you add dependencies, update `requirements.txt`.

### 1. Bronze

Create:

`output/bronze/events.parquet`

Bronze should preserve the received events with minimal transformation.

### 2. Silver

Create:

`output/silver/events.parquet`

Silver must contain valid, canonical business events.

At minimum:

- validate required identifiers
- validate supported event types
- validate timestamps
- remove exact duplicates
- remove semantic duplicates
- preserve event-time and ingestion-time information
- make your deduplication decisions deterministic

Document how invalid records are handled.

### 3. Gold — Orders

Create:

`output/gold/orders.parquet`

Exactly one row per order.

Required columns:

- `order_id`
- `created_at`
- `first_allocated_at`
- `accepted_at`
- `started_pickup_at`
- `delivered_at`
- `cancelled_at`
- `final_status`
- `allocation_attempts`
- `unique_riders`
- `city`
- `supplier_id`

`final_status` should be one of:

- `DELIVERED`
- `CANCELLED`
- `OPEN`

The final status must be based on event-time semantics, not file order.

### 4. Gold — Daily Metrics

Create:

`output/gold/daily_metrics.parquet`

Required columns:

- `date`
- `orders_created`
- `orders_delivered`
- `orders_cancelled`
- `orders_allocated`
- `orders_accepted`
- `fulfilment_rate`
- `acceptance_rate`
- `avg_allocation_attempts`

Use the order creation date as the reporting date.

Definitions:

- `fulfilment_rate = delivered orders / created orders`
- `acceptance_rate = orders with at least one RIDER_ACCEPTED / orders with at least one RIDER_ALLOCATED`

If the denominator is zero, return `NULL` rather than inventing a value.

### 5. Report

Create:

`output/report.json`

Minimum fields:

```json
{
  "raw_events": 0,
  "silver_events": 0,
  "rejected_events": 0,
  "duplicate_events_removed": 0,
  "late_events": 0,
  "unique_orders": 0,
  "delivered_orders": 0,
  "cancelled_orders": 0,
  "open_orders": 0
}
```

Treat an event as late when:

`ingested_at - event_time > 5 minutes`

### 6. Console Output

Finish with a human-readable summary similar to:

```text
PIPELINE COMPLETE
=================

Input
-----
Raw events:               14,392
Silver events:            13,921
Rejected events:             117
Duplicate events removed:    354

Orders
------
Unique orders:             5,000
Delivered:                 4,021
Cancelled:                   611
Open:                        368

Data Quality
------------
Late events:                  312

Output
------
bronze/events.parquet
silver/events.parquet
gold/orders.parquet
gold/daily_metrics.parquet
report.json
```

The exact formatting does not matter. The values do.

## Important expectations

Your pipeline should:

- produce the same output when run twice with the same seed
- work with unseen seeds
- not rely on file ordering
- not hard-code expected metric values
- make data-quality decisions explicit
- remain understandable to another engineer

## What to submit

Submit the repository containing your implementation.

We should be able to run:

```bash
docker build -t data-engineer-assessment .
docker run --rm -e SEED=1234 -e ORDERS=5000 data-engineer-assessment
```

without any manual setup.

Include a short `DECISIONS.md` explaining:

1. why you chose your processing technology
2. your deduplication strategy
3. how you handle bad records
4. how you derive final order state
5. how you would evolve this design for 100M+ events/day
