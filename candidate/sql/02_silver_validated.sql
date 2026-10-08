-- =============================================================================
-- 02_silver_validated.sql  --  Typing + validation (no rows removed yet)
-- =============================================================================
-- Every Bronze row is typed and then assigned a reject_reason, or NULL if it is a
-- valid business event. Nothing is deleted here: this table is the full audit trail
-- of "what we decided about every row and why". The split into Silver vs quarantine
-- happens in 03.
--
-- Rules are evaluated top-down and the FIRST failure wins, so a row always has
-- exactly one reason code and the classification is stable across runs. The order
-- goes from structural (can we even identify this row?) to semantic (is it an event
-- we understand?) to temporal (can we place it in time?).
--
-- try_cast is used for every timestamp so a malformed value yields NULL and is
-- routed to quarantine, instead of raising and killing the whole batch. One bad row
-- must never cost us the other 14,000.
--
-- TIME ZONE CONTRACT: strings are parsed as TIMESTAMPTZ -- which correctly handles
-- both the "...Z" form and any explicit offset a future producer might emit -- and
-- are then normalised to a naive UTC TIMESTAMP. Everything downstream, and every
-- published Parquet file, is therefore unambiguously UTC. This is deliberate:
-- timestamp-with-timezone round-trips inconsistently between Parquet readers, and
-- "all timestamps are UTC" is a contract every consumer can rely on without having
-- to know our session settings.
--
-- The supported event vocabulary is NOT hard-coded here: it comes from the
-- ref_event_types table, which is registered from candidate/config.py. One source of
-- truth, and adding an event type never means editing SQL.
-- =============================================================================

CREATE OR REPLACE TABLE silver_validated AS
WITH typed AS (
    SELECT
        _source_row,
        _source_file,
        _raw_payload,
        _is_valid_json,
        -- Treat blank/whitespace-only identifiers as missing. A value of "" is an
        -- absent identifier wearing a disguise.
        nullif(trim(event_id), '')    AS event_id,
        nullif(trim(order_id), '')    AS order_id,
        nullif(trim(event_type), '')  AS event_type,
        nullif(trim(city), '')        AS city,
        nullif(trim(rider_id), '')    AS rider_id,
        nullif(trim(supplier_id), '') AS supplier_id,
        -- Keep the original strings for the quarantine file: a rejected row is
        -- useless for debugging if we throw away the value that caused the reject.
        event_time   AS event_time_raw,
        ingested_at  AS ingested_at_raw,
        try_cast(event_time  AS TIMESTAMP WITH TIME ZONE)::TIMESTAMP AS event_time,
        try_cast(ingested_at AS TIMESTAMP WITH TIME ZONE)::TIMESTAMP AS ingested_at
    FROM bronze_events
)
SELECT
    typed.*,
    CASE
        WHEN NOT _is_valid_json                 THEN 'UNPARSEABLE_JSON'
        WHEN event_id IS NULL                   THEN 'MISSING_EVENT_ID'
        WHEN order_id IS NULL                   THEN 'MISSING_ORDER_ID'
        WHEN ref.event_type IS NULL             THEN 'UNKNOWN_EVENT_TYPE'
        WHEN typed.event_time IS NULL           THEN 'INVALID_EVENT_TIME'
        WHEN typed.ingested_at IS NULL          THEN 'INVALID_INGESTED_AT'
    END AS reject_reason
FROM typed
LEFT JOIN ref_event_types AS ref
       ON ref.event_type = typed.event_type;
