-- =============================================================================
-- 03_silver_dedup.sql  --  Quarantine split + two-stage deduplication
-- =============================================================================
-- Duplicates in this stream are two *different* failure modes and they need two
-- different keys. Collapsing them into one "DISTINCT" would be wrong in both
-- directions -- it would either under-remove or destroy real business facts.
--
--   Stage A -- exact duplicates (transport artefact)
--       The same event_id delivered more than once, because the transport layer
--       guarantees at-least-once, not exactly-once. Identity = event_id.
--
--   Stage B -- semantic duplicates (producer artefact)
--       The same real-world fact re-emitted with a freshly minted event_id, e.g. a
--       producer retrying after an ack timeout. Identity = the business key
--       (order_id, event_type, event_time, rider_id, supplier_id).
--
-- THE TRAP, and why the business key is this wide:
--   A single order can legitimately have several RIDER_ALLOCATED events -- that is
--   the reallocation flow, and allocation_attempts in Gold counts exactly those.
--   Because event_time and rider_id are part of the key, genuine reallocations are
--   preserved while true re-emissions collapse. A narrower key such as
--   (order_id, event_type) would silently flatten every order to one attempt and
--   quietly corrupt the headline operational metric.
--
-- DETERMINISM:
--   Both stages keep the row with the EARLIEST ingested_at -- the moment we first
--   learned the fact -- and break ties on data only (payload hash, then event_id).
--   No tie-breaker uses _source_row or file position. That is deliberate: the brief
--   requires that output not depend on file ordering, and a row-number tie-break
--   would violate that silently while still looking deterministic on one machine.
--   Any tie that survives the payload hash is between byte-identical rows, so the
--   choice is immaterial by definition.
-- =============================================================================

-- --- Quarantine (dead letter) ------------------------------------------------
-- Rejected rows are preserved with their reason code AND their original payload, so
-- they can be investigated and replayed once the upstream defect is fixed. Dropping
-- bad data silently is how you end up unable to explain a number six months later.
CREATE OR REPLACE TABLE rejected_events AS
SELECT
    _source_row,
    _source_file,
    reject_reason,
    event_id,
    order_id,
    event_type,
    event_time_raw,
    ingested_at_raw,
    city,
    rider_id,
    supplier_id,
    _raw_payload
FROM silver_validated
WHERE reject_reason IS NOT NULL
ORDER BY _source_row;


-- --- Stage A: exact duplicates ----------------------------------------------
CREATE OR REPLACE TABLE silver_exact_deduped AS
SELECT * EXCLUDE (_dedup_rank)
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at ASC, md5(_raw_payload) ASC
        ) AS _dedup_rank
    FROM silver_validated
    WHERE reject_reason IS NULL
)
WHERE _dedup_rank = 1;


-- --- Stage B: semantic duplicates -------------------------------------------
-- coalesce() on the nullable key parts makes NULL-matching explicit rather than
-- leaning on engine-specific NULL grouping semantics. The sentinel is a control
-- character that cannot appear in a real identifier.
CREATE OR REPLACE TABLE silver_business_deduped AS
SELECT * EXCLUDE (_dedup_rank)
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY
                order_id,
                event_type,
                event_time,
                coalesce(rider_id, chr(1)),
                coalesce(supplier_id, chr(1))
            ORDER BY ingested_at ASC, event_id ASC
        ) AS _dedup_rank
    FROM silver_exact_deduped
)
WHERE _dedup_rank = 1;


-- --- Published Silver --------------------------------------------------------
-- Valid, canonical, deduplicated business events. Both event time and ingestion
-- time are preserved: they answer different questions ("when did it happen" vs
-- "when did we find out"), and the gap between them is itself a quality signal.
CREATE OR REPLACE TABLE silver_events AS
SELECT
    event_id,
    order_id,
    event_type,
    event_time,
    ingested_at,
    cast(event_time AS DATE) AS event_date,
    city,
    rider_id,
    supplier_id,
    date_diff('second', event_time, ingested_at) AS lateness_seconds,
    date_diff('second', event_time, ingested_at) > $lateness_threshold_seconds AS is_late,
    _source_row,
    _source_file
FROM silver_business_deduped
ORDER BY order_id, event_time, event_id;
