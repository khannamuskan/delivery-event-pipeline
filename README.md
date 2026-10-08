# Senior Data Engineer Assessment — Event Pipeline

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

---

# Implementation notes

> Everything below this line was added by the candidate. The rationale for each
> decision is in [`DECISIONS.md`](DECISIONS.md).

## Approach in one paragraph

A Bronze → Silver → Gold medallion pipeline built on **DuckDB**, where all business
logic lives in numbered, reviewable SQL model files and `candidate/pipeline.py` is a
thin runner. Bronze lands every input line losslessly and judges nothing. Silver types
and validates each row, routes failures to a **quarantine** table with a reason code
instead of dropping them, and then removes duplicates in two stages using two different
keys — `event_id` for transport-level exact duplicates, and a wide business key for
producer-level semantic duplicates. Gold reconstructs the order lifecycle purely from
`event_time`, so late and out-of-order arrival need no special handling.

## Layout

```
candidate/
  pipeline.py                 # run(input_path, output_dir) -> report dict
  config.py                   # event vocabulary, lateness threshold, dedup keys, paths
  reporting.py                # report.json, reconciliation assertion, console summary
  sql/
    01_bronze.sql             # land raw lines + null-safe field extraction
    02_silver_validated.sql   # typing, validation, one reject_reason per bad row
    03_silver_dedup.sql       # quarantine, then exact + semantic deduplication
    04_gold_orders.sql        # one row per order, event-time semantics
    05_gold_daily_metrics.sql # daily aggregates, NULL-safe rates
tests/
  conftest.py                 # generates a stream and runs the real pipeline per test
  test_against_truth.py       # oracle test vs the generator's ground truth (5 seeds)
  test_contract.py            # output contract, grain, value domains, reconciliation
  test_determinism.py         # same seed twice -> identical data and row order
  test_interface.py           # provided
.github/workflows/ci.yml      # docker build + run + tests + determinism diff
```

The `Dockerfile`, `run.sh`, `schemas/` and the generator are unchanged. Outside
`candidate/` and `tests/` the only edits are: `pytest` added to `requirements.txt`,
`.venv/` and `.localrun/` added to `.dockerignore`, a two-line path fallback in
`main.py`, the CI workflow, and this section.

## Outputs

| Path | Grain | Notes |
| --- | --- | --- |
| `output/bronze/events.parquet` | one row per input line | lossless; raw payload + lineage retained |
| `output/silver/events.parquet` | one canonical business event | validated, deduplicated, `is_late` / `lateness_seconds` added |
| `output/quarantine/rejected_events.parquet` | one rejected row | **extra, not required**: reason code + original payload, so bad data is diagnosable and replayable |
| `output/gold/orders.parquet` | one row per order | all required columns, plus `event_count` |
| `output/gold/daily_metrics.parquet` | one row per creation date | all required columns |
| `output/report.json` | — | all required fields, plus duplicate split by class, rejections by reason, `orders_missing_created_at`, `late_event_rate` |

## Run

As specified in the brief — no manual setup required:

```bash
docker build -t data-engineer-assessment .
docker run --rm -e SEED=1234 -e ORDERS=5000 data-engineer-assessment
```

To run without Docker:

```bash
python -m venv .venv && . .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

`main.py` uses `/app` inside the container and falls back to the repository directory
outside it, so the same command works in both places. To drive the pipeline directly:

```python
from generator.generate import generate_dataset
from candidate.pipeline import run

generate_dataset(seed=42, orders=5000, output_path="data/events.jsonl")
run(input_path="data/events.jsonl", output_dir="output")
```

## Tests

```bash
pytest tests -q
```

or inside the image:

```bash
docker run --rm --entrypoint python data-engineer-assessment -m pytest tests -q
```

Each test generates a fresh stream and runs the **real** pipeline end to end; nothing
is mocked and no expected metric value is hard-coded.

### How correctness is actually established

The generator can be asked for the ground truth it simulated
(`generate_dataset(..., include_truth=True)`). The test suite uses that as an
**oracle** rather than asserting on numbers I looked at once:

- **`test_against_truth.py`** — for five seeds the implementation was never tuned
  against, every Gold order is compared field by field against the true outcome, and
  the Silver event *set* is compared for identity against the generator's canonical
  event set. That second assertion is the strongest available check on deduplication:
  if the dedup key were too wide, injected duplicates would survive; if it were too
  narrow, legitimate reallocation events would disappear. Both fail here.
- **`test_contract.py`** — files exist, required columns present, exactly one row per
  `order_id`, `final_status` in the allowed domain, rates within `[0, 1]` or `NULL`,
  status never contradicting its milestones, `unique_riders <= allocation_attempts`,
  Bronze lossless, quarantine rows always carrying a reason and payload, and the
  reconciliation invariant `raw = silver + rejected + duplicates`.
- **`test_determinism.py`** — the same seed run twice must produce identical content
  hashes *and* identical physical row order, and `report.json` must contain no
  wall-clock field. A control test asserts different seeds *do* differ, so the suite
  cannot pass by ignoring its input.

### Validation performed

- Full suite green across seeds 42, 7, 99, 1234, 2026.
- Edge cases: `ORDERS=1`, `2`, `25` (single-order and zero-denominator days).
- Scale check: 50,000 orders (~275k events) end to end in ~11 s on a laptop.
- On seed 42 / 1,000 orders the pipeline independently recovers the generator's own
  internal counts exactly: 5,253 canonical events, 182 exact duplicates, 101 semantic
  duplicates, 10 invalid records, 326 late events.
- **Image-parity check:** `main.py` and the full test suite were re-run in a clean
  virtual environment containing *only* what `requirements.txt` installs, to prove the
  image has no undeclared dependency.

### A note on Docker

The machine used for development is a managed Windows device without administrator
rights and without WSL2, so Docker could not be installed locally. Rather than ship an
unverified build, the container is verified in CI instead
(`.github/workflows/ci.yml`), which on every push does a real `docker build` and then:

- runs the exact `docker run` command from the brief (`SEED=1234 ORDERS=5000`);
- runs four further seeds the code has never been tuned against (7, 99, 2026, 31337);
- runs the full test suite **inside the image** — 31 passed;
- runs the same seed in two independent containers with separate mounted output
  directories and diffs the resulting `report.json`, which is identical.

All of the above is green. The `Dockerfile` and `run.sh` are unmodified from the
starter; the only dependency change is a pinned `pytest`, added so the suite can run
inside the image.

Locally, the same guarantees were checked a second way: `main.py` and all 31 tests were
re-run in a clean virtual environment containing *only* what `requirements.txt`
installs, proving the image has no undeclared dependency. The code has no OS-specific
behaviour — `pathlib` only, no shell calls, SQL models referenced by explicit name so a
case-sensitive filesystem is safe, and the Bronze reader trims stray `CR` so it is
agnostic to CRLF vs LF. `main.py` resolves `/app` inside the container and the
repository directory outside it, so one entrypoint serves both.
