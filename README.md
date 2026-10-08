# Delivery Event Pipeline

[![CI](https://github.com/khannamuskan/delivery-event-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/khannamuskan/delivery-event-pipeline/actions/workflows/ci.yml)

A Bronze, Silver and Gold pipeline that turns a deliberately messy delivery-order
event stream into reliable Parquet datasets and a reconciled JSON report.

The input stream contains everything a real one does: exact duplicates from
at-least-once delivery, semantic duplicates from producer retries, events that
arrive late and out of order, malformed records, unknown event types, orders
missing parts of their lifecycle, and orders allocated to several riders in turn.

Built on DuckDB. All business logic lives in numbered SQL model files;
`candidate/pipeline.py` is a thin runner around them.

- Run instructions: below
- Design reasoning: [DECISIONS.md](DECISIONS.md)
- Original brief: [ASSIGNMENT.md](ASSIGNMENT.md)

---

## Quickstart

### With Docker

```bash
docker build -t data-engineer-assessment .
docker run --rm -e SEED=1234 -e ORDERS=5000 data-engineer-assessment
```

No manual setup. `SEED` and `ORDERS` default to `42` and `1000`.

### Without Docker

```bash
python -m venv .venv
. .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

Requires Python 3.11 or newer. `main.py` writes to `/app` inside the container and
falls back to the repository directory outside it, so the same command works in
both places.

### Calling the pipeline directly

```python
from generator.generate import generate_dataset
from candidate.pipeline import run

generate_dataset(seed=42, orders=5000, output_path="data/events.jsonl")
report = run(input_path="data/events.jsonl", output_dir="output")
```

`run()` writes every output file and returns the report as a dict.

---

## What you get

| Path | Grain | Notes |
| --- | --- | --- |
| `output/bronze/events.parquet` | one row per input line | Lossless. Raw payload and lineage retained, including unparseable lines. |
| `output/silver/events.parquet` | one canonical business event | Validated and deduplicated. Adds `event_date`, `lateness_seconds`, `is_late`. |
| `output/quarantine/rejected_events.parquet` | one rejected row | Extra, not required by the brief. Reason code plus the original payload, so bad data is diagnosable and replayable. |
| `output/gold/orders.parquet` | one row per order | Full lifecycle, `final_status`, `allocation_attempts`, `unique_riders`, `event_count`. |
| `output/gold/daily_metrics.parquet` | one row per creation date | Volumes and NULL-safe rates. |
| `output/report.json` | one file | Counts for every layer, duplicates split by class, rejections by reason, lateness, and order outcomes. |

A console summary is printed at the end of every run.

---

## How it works

```
events.jsonl
     |
     v
[ 01_bronze ]          land every line losslessly, extract fields, judge nothing
     |
     v
[ 02_silver_validated ] type and validate, assign exactly one reject reason
     |
     +--> quarantine/rejected_events.parquet
     |
     v
[ 03_silver_dedup ]    stage A: exact duplicates   (event_id)
                       stage B: semantic duplicates (business key)
     |
     v
[ 04_gold_orders ]            [ 05_gold_daily_metrics ]
```

**Bronze lands, it does not judge.** Every input line is preserved, including
malformed JSON, so a bad record becomes a quarantined row rather than a failed job.

**Silver types, validates and deduplicates.** Validation assigns exactly one reason
code per bad row, evaluated structural first, then semantic, then temporal.
Deduplication runs in two stages with two different keys, because the stream
contains two unrelated defects:

| Stage | Defect | Key |
| --- | --- | --- |
| A | Transport redelivered the message (at-least-once) | `event_id` |
| B | Producer re-emitted the fact with a new ID (retry after ack timeout) | `(order_id, event_type, event_time, rider_id, supplier_id)` |

The stage B key is wide on purpose. A narrower key such as
`(order_id, event_type)` would collapse genuine rider reallocations, which are a
real and frequent business event, and would silently flatten every order to a
single allocation attempt. [DECISIONS.md](DECISIONS.md) has the measured impact.

**Gold reconstructs the order lifecycle from `event_time` only,** never from arrival
order or row position, so late and out-of-order events need no special handling.
Contradictions such as an order that is both delivered and cancelled resolve by a
stated rule rather than by whichever row happened to be last.

---

## Repository layout

```
candidate/
  pipeline.py                 run(input_path, output_dir) -> report dict
  config.py                   event vocabulary, lateness threshold, dedup keys, paths
  reporting.py                report.json, reconciliation check, console summary
  sql/
    01_bronze.sql             land raw lines, null-safe field extraction
    02_silver_validated.sql   typing, validation, one reject reason per bad row
    03_silver_dedup.sql       quarantine, then exact and semantic deduplication
    04_gold_orders.sql        one row per order, event-time semantics
    05_gold_daily_metrics.sql daily aggregates, NULL-safe rates
tests/
  conftest.py                 generates a stream and runs the real pipeline per test
  test_against_truth.py       compares output to the generator's ground truth, 5 seeds
  test_contract.py            output contract, grain, value domains, reconciliation
  test_determinism.py         same seed twice gives identical data and row order
  test_interface.py           provided
.github/workflows/ci.yml      docker build, run, tests, determinism diff
```

Business rules live in `config.py`, not scattered through the SQL. The event
vocabulary is registered as a reference table, so adding an event type does not mean
editing a model file.

The `Dockerfile`, `run.sh`, `schemas/` and the generator are unchanged. Outside
`candidate/` and `tests/` the only edits are a pinned `pytest` in
`requirements.txt`, ignore-file entries, a two-line path fallback in `main.py`, and
the CI workflow.

---

## Data quality

Decisions are explicit and visible in the output rather than buried in code.

- **Nothing is dropped silently.** Invalid rows go to quarantine with a reason code
  and their original payload, so they can be investigated and replayed once the
  upstream defect is fixed.
- **One reason per rejected row**, first failure wins, so rejection counts sum
  cleanly: `UNPARSEABLE_JSON`, `MISSING_EVENT_ID`, `MISSING_ORDER_ID`,
  `UNKNOWN_EVENT_TYPE`, `INVALID_EVENT_TIME`, `INVALID_INGESTED_AT`.
- **`NULL` is not `0`.** A rate with a zero denominator is `NULL`, because "no orders
  that day" and "0% delivered that day" mean different things. Orders missing
  `created_at` are excluded from daily metrics and reported separately as
  `orders_missing_created_at`.
- **All timestamps are UTC.** Inputs are parsed as timezone-aware and normalised, so
  consumers never need to know the session settings of the process that wrote the
  file.
- **Late events are measured, not discarded.** `lateness_seconds` and `is_late` are
  derived from the gap between event time and ingestion time, with the threshold set
  in `config.py`.
- **Reconciliation is enforced, not just reported.** Every run asserts
  `raw = silver + rejected + duplicates` and refuses to publish if it does not hold,
  rather than emitting a plausible-looking number.

---

## Tests

```bash
pytest tests -q
```

Inside the image:

```bash
docker run --rm --entrypoint python data-engineer-assessment -m pytest tests -q
```

31 tests. Each one generates a fresh stream and runs the real pipeline end to end.
Nothing is mocked and no expected metric value is hard-coded anywhere.

**Correctness is established against an oracle, not a snapshot.** The generator can
be asked for the ground truth it simulated (`include_truth=True`). The suite uses
that as the source of truth:

- `test_against_truth.py` compares every Gold order field by field against the true
  outcome, for five seeds the implementation was never tuned against. It also
  compares the Silver event set for identity against the generator's canonical set,
  which is the strongest available check on deduplication: too wide a key leaves
  injected duplicates alive, too narrow a key deletes real reallocation events, and
  both fail this assertion.
- `test_contract.py` asserts properties that must hold on any seed: one row per
  order, value domains, rates within `[0, 1]` or `NULL`, status never contradicting
  its milestones, `unique_riders <= allocation_attempts`, Bronze lossless, quarantine
  rows always carrying a reason and payload, and the reconciliation invariant. Also
  parametrised over 1, 2 and 25 orders, where empty days and single-row groups break
  naive aggregation.
- `test_determinism.py` asserts that the same seed produces identical content hashes
  and identical physical row order, and that `report.json` carries no wall-clock
  field. A control test asserts different seeds do differ, so the suite cannot pass
  by ignoring its input.

---

## Determinism and idempotency

Both properties are designed in, not hoped for.

**Deterministic**, meaning the same input always yields the same output:

- Session timezone pinned to UTC.
- No `now()`, `current_timestamp` or `random()` in any model.
- An explicit total `ORDER BY` on every Parquet write.
- Deduplication tie-breakers use data only (`ingested_at`, then payload hash, then
  `event_id`), never row number or file position.
- Bronze records the source file *name*, not its path, so output does not change
  when the input lives in a different directory.

**Idempotent**, meaning running it N times leaves the same state as running it once:

- Every model is `CREATE OR REPLACE TABLE`; every write is `COPY (...) TO 'file'`.
  Full overwrite, no append path anywhere.
- The DuckDB connection is in-memory and created per run, so no state survives to
  accumulate on.

Verified by running the pipeline three times into the same output directory: row
counts unchanged and the SHA-256 of every output file identical.

---

## Validation performed

- Full suite green on seeds 42, 7, 99, 1234 and 2026.
- Edge cases: `ORDERS=1`, `2` and `25`, covering single-order and zero-denominator
  days.
- Scale: 50,000 orders, roughly 275,000 events, end to end in about 11 seconds on a
  laptop.
- On seed 42 with 1,000 orders the pipeline independently recovers the generator's
  own internal counters exactly: 5,253 canonical events, 182 exact duplicates, 101
  semantic duplicates, 10 invalid records, 326 late events.
- Dependency check: `main.py` and the full suite were re-run in a clean virtual
  environment containing only what `requirements.txt` installs, confirming the image
  has no undeclared dependency.

---

## Running the container, and how it was verified

The machine used for development is a managed Windows device without administrator
rights and without WSL2, so Docker could not be installed locally. Rather than ship
an unverified build, the container is verified in CI
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)), which on every push does a
real `docker build` and then:

- runs the exact `docker run` command from the brief (`SEED=1234 ORDERS=5000`);
- runs four further seeds the code has never been tuned against (7, 99, 2026, 31337);
- runs the full test suite inside the image;
- runs the same seed in two independent containers with separate output directories
  and diffs the resulting `report.json`, which is identical.

All of the above is green on the badge at the top of this file.

Nothing in the code is OS-specific: `pathlib` throughout, no shell calls, and SQL
models referenced by explicit filename so a case-sensitive filesystem is safe. The
Bronze reader trims a stray carriage return if present, making it agnostic to CRLF
and LF line endings.
