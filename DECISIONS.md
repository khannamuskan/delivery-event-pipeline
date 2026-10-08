# Decisions

This document explains *why* the pipeline is built the way it is. Where a choice had
a credible alternative, the alternative and the reason it was rejected are recorded
too — the rejected options are usually more informative than the chosen one.

---

## Processing technology

**Chosen: DuckDB executing versioned SQL model files, orchestrated by a thin Python
layer.**

### The shape of the problem

The workload is a single-node, batch, set-oriented transformation: read a JSONL file,
apply row-level validation, deduplicate on two different keys, aggregate to an order
grain, aggregate again to a daily grain, write Parquet. Every step is naturally
expressible as a relational operation. There is no iterative computation, no
per-record external I/O, and no model training — nothing that actually wants an
imperative loop.

### Why DuckDB

1. **The logic is declarative, so it should be written declaratively.** Deduplicating
   on a composite key with a deterministic tie-break is a `row_number()` window.
   Reconstructing a lifecycle is a filtered aggregate. Writing those as SQL means the
   statement *is* the specification. A reviewer can read `min(event_time) FILTER
   (WHERE event_type = 'DELIVERED')` and verify the business rule in one line; the
   Pandas equivalent is a groupby-apply whose intent has to be reverse-engineered.

2. **SQL is the lingua franca of a data team.** This code has to be readable by
   analysts and analytics engineers, not just by whoever wrote it. The dedup rule
   being plain SQL means the person who later asks "why did my allocation event
   disappear?" can answer it themselves.

3. **It is a vectorised, larger-than-memory engine.** DuckDB streams its scans and
   spills to disk when a hash aggregate exceeds memory. The same code that runs on
   5,000 orders runs on tens of millions on one machine. Pandas would require the
   whole frame in RAM and would be the first thing to fall over on a larger stream.

4. **Zero operational surface.** It is an in-process library with no server, no JVM,
   no cluster, no config. The Docker image stays `python:3.12-slim` plus one wheel,
   the build is fast, and there is nothing to misconfigure between my machine and the
   evaluator's.

5. **Native Parquet writer.** `COPY ... TO ... (FORMAT PARQUET)` with explicit
   compression, no intermediate materialisation through Arrow or Pandas, and
   predictable types.

6. **It is already the project's only dependency.** `requirements.txt` shipped with
   `duckdb==1.4.1` pinned, which is a fairly direct hint about the expected approach.

### Alternatives considered

| Option | Why not |
| --- | --- |
| **Pandas** | Whole dataset in memory; no spill; silent dtype coercion (`NaN` for missing timestamps conflates "absent" with "invalid"); groupby-apply chains hide business rules in imperative code. |
| **Polars** | Genuinely excellent and a close second — lazy, vectorised, streaming. Rejected because the logic is relational and the expression API is still less reviewable by non-Python data people than SQL, and it would add a dependency to do what the pinned one already does. |
| **PySpark** | The right answer at 100M+ events/day (see *Scaling*), the wrong answer here. A JVM, a session startup cost measured in tens of seconds, and shuffle semantics to reason about — all to process a file that fits comfortably on one core. Reaching for a distributed engine at this volume is a judgement error, not a strength. |
| **Plain Python + `json` + dicts** | Maximum control, but every aggregate becomes hand-rolled state management, and correctness then depends on code review rather than on well-understood relational semantics. |

### How the code is organised

The Python layer deliberately contains almost no business logic:

```
candidate/
  pipeline.py   # orchestration: connect, register reference data, run models, write Parquet
  config.py     # the business vocabulary and tunable thresholds, in one place
  reporting.py  # report.json assembly, reconciliation assertion, console summary
  sql/
    01_bronze.sql            # land raw lines, extract fields, judge nothing
    02_silver_validated.sql  # type + validate, assign exactly one reject reason
    03_silver_dedup.sql      # quarantine, then two-stage deduplication
    04_gold_orders.sql       # one row per order
    05_gold_daily_metrics.sql
```

This is a deliberately "dbt-lite" structure. Every transformation is a numbered,
reviewable SQL file; `pipeline.py` is a runner. The consequence is that migrating
this to dbt + Snowflake/BigQuery/Databricks later is mostly a matter of moving the
files and adding `ref()` calls — the logic does not need rewriting. Thresholds that a
business owner might want to change (the 5-minute lateness window, the set of valid
event types, the dedup key columns) live in `config.py` rather than being scattered
through SQL as literals.

### Determinism, enforced structurally

The brief requires identical output across runs. Rather than hope for it, the design
removes the ways it is normally lost:

- The DuckDB session pins `TimeZone = 'UTC'`, so parsing never depends on the host
  locale.
- Timestamps are cast to naive UTC immediately (`TIMESTAMPTZ -> TIMESTAMP`), so no
  downstream operation can reintroduce an offset.
- No artifact contains a wall-clock value — no `load_timestamp`, no `generated_at` in
  `report.json`. Run metadata belongs in logs, not in a dataset that is supposed to be
  comparable across runs.
- Every `COPY ... TO ... PARQUET` has an explicit **total** `ORDER BY`, so physical
  row order is a function of the data, not of hash-table or thread scheduling.
- `_source_file` stores the file *name*, not the absolute path, so a run under
  `/app/data` and a run under a temp directory produce identical data.
- Every dedup tie-break is resolved using *data* columns, never line number.

`tests/test_determinism.py` runs the same seed twice and compares content hashes, and
separately compares physical row-order hashes, so a regression in any of the above
fails the build.

---

## Deduplication

### The central insight: two kinds of duplicate need two different keys

The stream contains two kinds of repetition with two different root causes, and
collapsing them with one rule gets the answer wrong.

**1. Exact duplicates — a transport problem.**
The same logical event delivered more than once (at-least-once delivery, consumer
replay after an offset commit failure, a retried HTTP POST). The payloads are
byte-identical, including `event_id`.

*Key:* `event_id`.
*Rule:* `row_number() OVER (PARTITION BY event_id ORDER BY ingested_at ASC,
md5(_raw_payload) ASC) = 1`.

`event_id` is the producer's idempotency key and it is the strongest signal
available — two rows carrying the same one are the same event by definition. The
tie-break on `ingested_at` keeps the first delivery; the hash of the raw payload is a
final deterministic fallback so that even two rows identical in every field resolve
the same way on every run.

**2. Semantic duplicates — a producer problem.**
The same real-world occurrence emitted twice with *different* `event_id`s: a service
retried after a timeout it had actually processed, or two instances of a producer both
published. `event_id` cannot see these at all.

*Key:* `(order_id, event_type, event_time, rider_id, supplier_id)`.
*Rule:* `row_number() OVER (PARTITION BY <that key> ORDER BY ingested_at ASC,
event_id ASC) = 1`.

The deduplication runs in that order — exact first, then semantic — because the
cheaper, higher-confidence rule should reduce the input to the more expensive,
judgement-based one.

### Why the business key is that wide — the trap in this dataset

The obvious-looking key is `(order_id, event_type)`: "an order is only created once,
only delivered once." **It is wrong, and it is the single most damaging mistake
available in this assignment.**

A delivery order can legitimately be allocated to a rider several times — the first
rider times out or declines, and the order is reallocated. Those are *multiple genuine
`RIDER_ALLOCATED` events for one order*, and they are the entire basis of the required
`allocation_attempts` and `unique_riders` columns. Deduplicating on
`(order_id, event_type)` would silently flatten every reallocation to one, pin
`allocation_attempts` at exactly 1 for every allocated order, and destroy
`avg_allocation_attempts` in the daily metrics — while the pipeline reported success.

Including `event_time`, `rider_id` and `supplier_id` in the key means two allocation
attempts to different riders, or to the same rider at different times, are correctly
preserved as distinct business facts; only an identical attempt at the identical
instant is treated as a repeat. `NULL`s in `rider_id`/`supplier_id` are coalesced to a
sentinel so that `NULL`-containing keys group rather than being compared as unknown.

### Why not just distinct the whole row

`SELECT DISTINCT *` catches exact duplicates and nothing else: semantic duplicates
differ by `event_id`, so they survive. It also quietly collapses any two rows that
happen to be identical for legitimate reasons, with no record that it happened.

### Why dedup counts are tracked separately

`report.json` reports `exact_duplicates_removed` and `semantic_duplicates_removed`
separately as well as the required combined total. They are different operational
signals: a spike in exact duplicates points at the message bus or consumer offsets; a
spike in semantic duplicates points at a specific producer's retry logic. Reporting
only the sum throws away the diagnosis.

### The reconciliation invariant

After every run the pipeline asserts:

```
raw_events = silver_events + rejected_events + duplicate_events_removed
```

Every row that entered is either published, quarantined, or explicitly removed as a
duplicate — nothing silently disappears. The assertion raises and fails the run if it
does not hold, so a future change that drops rows somewhere unexpected cannot ship
quietly.

---

## Invalid records

### Principle: quarantine, never discard; and never let one bad row fail the batch

Bad records are **routed, not dropped**. They are written to
`output/quarantine/rejected_events.parquet` with the original raw payload, the reason
code, and the source line number. Three reasons:

1. **A dropped row is an unanswerable question.** When someone asks "why is this order
   missing?", a quarantine table answers it in one query. A `WHERE` clause that
   filtered the row out leaves no evidence it ever existed.
2. **Rejections are a data-quality signal with an owner.** Grouped by reason code,
   they are exactly what a producer team needs to fix the problem at source. Rejection
   counts by reason are a monitorable metric; a rising `UNKNOWN_EVENT_TYPE` count is
   usually an unannounced producer deployment.
3. **They are replayable.** The payload is preserved verbatim, so once the upstream bug
   is fixed the quarantine can be reprocessed. Nothing is lost permanently.

### Where validation happens: after landing, not during

Bronze parses nothing and judges nothing. It lands one row per input line as raw text,
plus field extractions that are `NULL`-guarded by `json_valid()`. A row of corrupt JSON
becomes a Bronze row with `_is_valid_json = false` — **not** a parser exception that
kills the job.

This matters: `read_json_auto` would either fail on malformed lines or silently infer a
different schema depending on which values happen to appear in a given seed. Reading
the file as opaque lines and extracting fields explicitly makes the schema a *stated
contract* rather than something inferred per run, which is also why output is stable
across unseen seeds.

### Reason codes, evaluated first-failure-wins

Each rejected row gets exactly one reason, assigned in a fixed precedence order:

| Reason | Meaning |
| --- | --- |
| `UNPARSEABLE_JSON` | The line is not valid JSON. Nothing else can be evaluated. |
| `MISSING_EVENT_ID` | No `event_id`. Without it, idempotency is impossible. |
| `MISSING_ORDER_ID` | No `order_id`. The event cannot be attributed to an order. |
| `UNKNOWN_EVENT_TYPE` | Event type outside the supported vocabulary. |
| `INVALID_EVENT_TIME` | `event_time` absent or not parseable as a timestamp. |
| `INVALID_INGESTED_AT` | `ingested_at` absent or not parseable. |

One reason per row rather than a list, because the precedence order is causal: a row
that is not valid JSON does not also "have a missing order_id" in any useful sense. The
first failure is the actionable one. Empty strings are normalised to `NULL`
(`nullif(trim(x), '')`) so a whitespace-only identifier is treated as missing rather
than accepted as a valid-looking key.

### What is *not* a rejection

- **A missing `rider_id`, `supplier_id` or `city`** is expected for several event types
  and is simply `NULL`.
- **A missing lifecycle event** (an order with no `STARTED_PICKUP`) is a legitimate
  business outcome, not bad data. The milestone stays `NULL`.
- **An unknown event type is rejected rather than passed through**, because letting an
  unrecognised type into Silver means Gold is silently computed over a vocabulary
  nobody has validated. Quarantining it makes the schema drift visible.

### On `NULL` vs zero

Where a denominator is zero, the rate is `NULL`, not `0.0` (`nullif(denominator, 0)`).
`0.0` asserts "none of them were accepted"; `NULL` states "there is nothing to
measure". Writing zero here is how a dashboard ends up showing a dramatic, entirely
fictional drop in acceptance rate. Orders with no parseable `created_at` cannot be
attributed to a reporting day, so they are excluded from daily metrics and counted
explicitly as `orders_missing_created_at` in the report — excluded, but never hidden.

---

## Order-state reconstruction

### Everything is event-time semantics; file order is never consulted

The order of lines in the file is an artefact of how events were transported. Arrival
order is also not the truth — a late record describes something that happened earlier,
not something that happened later. So every value in `gold/orders.parquet` is derived
from `event_time`.

**Milestones** are `min(event_time) FILTER (WHERE event_type = '<TYPE>')`. The first
time something happened is a well-defined, order-independent fact. Because it is an
aggregate over the whole order, a record arriving two hours late lands in exactly the
right place with no special-case logic — out-of-order handling falls out of the model
rather than being bolted onto it.

**Missing milestones stay `NULL`.** `STARTED_PICKUP` is genuinely absent on some
delivered orders. It is not interpolated from the delivery time. `NULL` honestly means
"never observed"; a fabricated value looks like data and poisons every downstream
duration metric computed from it.

**`allocation_attempts`** is `count(*) FILTER (WHERE event_type = 'RIDER_ALLOCATED')`,
counted *after* deduplication so that a retried allocation message does not inflate an
operational KPI.

**`unique_riders`** is `count(DISTINCT rider_id)` restricted to `RIDER_ALLOCATED`
events, because that is the event type where rider assignment is actually decided;
downstream events merely echo the current rider.

**`city`** is a property of the order, so it is taken from `ORDER_CREATED` where
present, falling back to the earliest event carrying one.

**`supplier_id`** is treated as *slowly changing within the order*: it is reassigned on
each allocation attempt, so the correct answer is the **latest** value by event time
(`max_by(supplier_id, order_key)`), not the one the order was created with. Taking the
creation-time supplier would attribute a reallocated order to the wrong partner — a
mistake with commercial consequences.

**`final_status`** is decided by the terminal event with the latest `event_time`:

```sql
CASE
  WHEN delivered_at IS NULL AND cancelled_at IS NULL THEN 'OPEN'
  WHEN cancelled_at IS NULL                          THEN 'DELIVERED'
  WHEN delivered_at IS NULL                          THEN 'CANCELLED'
  WHEN delivered_at >= cancelled_at                  THEN 'DELIVERED'
  ELSE 'CANCELLED'
END
```

`OPEN` is the honest default for an order with no terminal event — it may still be in
flight, or its terminal event may not have arrived yet; either way the pipeline should
not guess. The both-terminals branch does not occur in this dataset, but a real stream
eventually produces it (a cancellation racing a delivery confirmation), so the rule is
written down rather than assumed away. A tie at an identical instant resolves to
`DELIVERED`, because a completed physical delivery is the stronger and
harder-to-reverse fact than a cancellation request.

### Deterministic attribute selection

Where a single value must be chosen from several candidate events (`city`,
`supplier_id`), the choice is ordered by the composite struct
`{event_time, lifecycle_rank, event_id}`. `lifecycle_rank` breaks ties between
different event types that share a timestamp using the natural lifecycle order, and
`event_id` — unique after deduplication — makes the ordering **total**. Without a total
order, two equally-ranked candidates could be returned in either order depending on
execution plan, and the output would stop being reproducible.

### How this was verified

The generator exposes `include_truth=True`, which returns the exact per-order outcome
it simulated. `tests/test_against_truth.py` uses that as an **oracle**: it runs the
real pipeline across five seeds the implementation was never tuned against and
compares all twelve Gold columns, order by order, against ground truth. It also
asserts that the Silver event set is *identical* — not merely equal in count — to the
generator's canonical event set, which is the strongest available statement that
deduplication is neither too aggressive (reallocations lost) nor too timid (injected
duplicates surviving).

This is why the brief's "do not hard-code expected metric values" rule is respected
while still having real assertions: nothing is compared to a number I wrote down, it is
compared to independently derived truth.

Alongside it, `tests/test_contract.py` asserts the output contract (files, columns, one
row per order, value domains, status/milestone consistency, count reconciliation) and
`tests/test_determinism.py` asserts stable data and row order across repeated runs.

---

## Scaling

### What actually changes at 100M+ events/day

100M events/day is roughly 1,200 events/second average and realistically 5–10k/second
at peak — a few hundred GB/day. The transformation logic above stays correct; what
breaks is the *execution model*. Specifically: the single-file read, the global window
functions over the entire history, and the full-rebuild-every-run pattern.

### 1. Ingestion — a log, partitioned by `order_id`

Kafka (or Kinesis/Pub-Sub) with **`order_id` as the partition key**. That one choice
does a lot of work: all events for an order land on the same partition and are
therefore processed by the same consumer with the same local state, which makes both
deduplication and lifecycle reconstruction local operations instead of distributed
shuffles. Producers publish Avro/Protobuf against a **schema registry** with
compatibility enforcement, so `UNKNOWN_EVENT_TYPE` and malformed payloads are rejected
at the edge rather than discovered three layers downstream.

### 2. Bronze — an open table format, not loose Parquet files

Land raw events to object storage as **Iceberg** (or Delta) partitioned by
`event_date`, ideally with an hour sub-partition at this volume. The open table format
buys the things loose Parquet cannot do: atomic commits, schema evolution, snapshot
isolation for readers, time travel for debugging a bad batch, and — critically —
**`MERGE INTO` for idempotent restatement of a single partition**. Compaction of small
files becomes a scheduled maintenance job, because at 1,200 events/second the
small-file problem arrives quickly.

### 3. Deduplication — bounded keyed state, not a global window

`row_number() OVER (PARTITION BY event_id)` across all history is `O(all data)` and
becomes untenable. Replace it with:

- **Streaming:** Flink or Spark Structured Streaming keyed by `order_id`, holding a
  `MapState` of seen `event_id`s and business keys with a **TTL** sized to the
  realistic duplicate window (say 24–48 hours, derived from observed
  `ingested_at - event_time` percentiles). Watermarks drive state eviction so memory
  is bounded.
- **Batch:** dedup *within partition* against a compact `(event_id, event_date)` index
  table covering only the recent window, instead of scanning history.

A probabilistic pre-filter (Bloom/HyperLogLog) in front of the exact check cheaply
removes the overwhelming majority of non-duplicates before the expensive lookup.

### 4. Late data — side outputs and targeted restatement

A watermark with an allowed-lateness bound (say 15 minutes) covers the common case.
Events later than that go to a **side output** rather than being dropped, and a
scheduled job replays them with `MERGE INTO` against the specific affected `event_date`
partitions. Because the Gold aggregates are deterministic functions of the Silver
partition, restating a day is a safe, repeatable operation rather than a risky manual
fix. The lateness threshold itself should be derived from the observed distribution and
alerted on when it shifts — it is a property of the upstream system, not a constant.

### 5. Silver and Gold — incremental, not full rebuild

- `silver_events` becomes an append + merge into an Iceberg table partitioned by
  `event_date`, clustered/sorted by `order_id` so order reconstruction reads contiguous
  data.
- `gold_orders` becomes **incremental on the set of `order_id`s touched since the last
  high-water mark**, not a full re-aggregation. Open orders are reconsidered until they
  reach a terminal state plus a grace period, then frozen.
- `daily_metrics` is recomputed only for dates whose partitions changed.
- Long-lived orders are the thing to watch: an order that never terminates keeps state
  alive forever. It needs an explicit expiry policy and a "stuck orders" alert — which
  is a genuine operational signal, not just a technical cleanup.

### 6. Compute

Spark or Flink on Kubernetes (EMR/Databricks/Dataproc) with autoscaling, or
Snowflake/BigQuery if the organisation is warehouse-centric. The SQL models here would
migrate largely intact — the dbt-lite file layout is deliberately one `ref()` away from
being a dbt project. Importantly, **DuckDB does not necessarily disappear**: it remains
an excellent engine for per-partition tasks, local development, and CI, where spinning
up a cluster to test a transformation is pure overhead.

### 7. Orchestration, testing and observability

- **Airflow or Dagster** for scheduling, retries, backfills and dependency management.
  Dagster's asset model is a particularly good fit for a medallion layout with
  partition-level lineage.
- **dbt tests / Great Expectations** running as a gate: uniqueness of `order_id` in
  Gold, referential integrity between layers, non-negative counters, rates within
  `[0, 1]`, and the same reconciliation invariant this pipeline already asserts.
- **Monitoring on data, not just on jobs**: rejection rate by reason code, duplicate
  rate split by exact vs semantic, late-event percentiles, freshness/lag per partition,
  and distribution drift on `allocation_attempts`. A green DAG with a quietly doubled
  rejection rate is a failure nobody gets paged for — those thresholds should page.
- **Data contracts with producer teams**, versioned in the schema registry, so schema
  changes are negotiated rather than discovered.

### 8. Cost and layout

Partition by `event_date`, sort/cluster by `order_id`, compact to ~128–512 MB files,
and keep Zstd compression. Most of the cost at this volume is scan volume and small
files; both are layout problems, not compute problems. Tier old Bronze partitions to
cold storage with a retention policy, and keep Silver as the replay source of record.

### What would *not* change

The semantics. Event-time milestones, two-key deduplication, quarantine-don't-drop,
`NULL` over fabricated zeros, and deterministic tie-breaking are correctness
properties, not scale properties. They would be implemented with different machinery
and should produce the same answers — which is exactly why they are specified as SQL
and verified against an oracle rather than embedded in one engine's API.
