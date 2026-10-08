-- =============================================================================
-- 01_bronze.sql  --  Raw landing layer
-- =============================================================================
-- Contract: Bronze preserves what arrived. It does not validate, deduplicate,
-- reorder or reject anything. Every input line survives here, so every later
-- decision can be audited and replayed without re-reading the source system.
--
-- Two deliberate choices:
--
--  1. The file is read as one VARCHAR column of raw text, not with read_json_auto.
--     Type inference over a deliberately dirty stream is a correctness hazard: a
--     value like "not-a-timestamp", or an unseen seed with a different null mix,
--     would silently change the Bronze schema from run to run. Bronze must have a
--     stable schema regardless of payload quality, so parsing happens *after*
--     landing -- with json_extract_string -- where a failure becomes a quarantined
--     row rather than a failed job.
--     The chr(1)/chr(2)/chr(3) delimiter, quote and escape characters are control
--     characters that json.dumps always escapes, so they can never occur inside the
--     payload. This makes read_csv a faithful line reader that still streams,
--     rather than loading the entire file into memory.
--
--  2. Lineage columns are deterministic (_source_file, _source_row) and there is no
--     wall-clock load timestamp. The brief requires identical output across runs of
--     the same seed; a now() column would quietly break that guarantee.
--     _source_row exists so a specific line can be audited back to the source file.
--     It is never used in a business rule, because the pipeline must not depend on
--     file ordering.
--     _source_file stores the file *name* rather than the absolute path on purpose:
--     the path changes with the mount point (/app/data in Docker, a temp directory
--     under test, something else on a laptop), which would make otherwise identical
--     runs produce different data. The name is the part that actually identifies the
--     batch; the location is an environment detail and does not belong in the data.
-- =============================================================================

CREATE OR REPLACE TABLE bronze_events AS
WITH raw_lines AS (
    SELECT
        parse_filename(filename, false) AS _source_file,
        -- Trim stray carriage returns so the reader is agnostic to CRLF vs LF.
        trim(_raw_payload, chr(13)) AS _raw_payload
    FROM read_csv(
        $input_path,
        columns = {'_raw_payload': 'VARCHAR'},
        delim = chr(1),
        quote = chr(2),
        escape = chr(3),
        header = false,
        auto_detect = false,
        filename = true
    )
),
numbered AS (
    SELECT
        _source_file,
        _raw_payload,
        row_number() OVER () AS _source_row
    FROM raw_lines
    WHERE length(trim(_raw_payload)) > 0
)
SELECT
    _source_row,
    _source_file,
    _raw_payload,
    json_valid(_raw_payload) AS _is_valid_json,
    -- Extracted as text only: no casting, no coercion, no judgement.
    CASE WHEN json_valid(_raw_payload)
         THEN json_extract_string(_raw_payload, '$.event_id')    END AS event_id,
    CASE WHEN json_valid(_raw_payload)
         THEN json_extract_string(_raw_payload, '$.order_id')    END AS order_id,
    CASE WHEN json_valid(_raw_payload)
         THEN json_extract_string(_raw_payload, '$.event_type')  END AS event_type,
    CASE WHEN json_valid(_raw_payload)
         THEN json_extract_string(_raw_payload, '$.event_time')  END AS event_time,
    CASE WHEN json_valid(_raw_payload)
         THEN json_extract_string(_raw_payload, '$.ingested_at') END AS ingested_at,
    CASE WHEN json_valid(_raw_payload)
         THEN json_extract_string(_raw_payload, '$.city')        END AS city,
    CASE WHEN json_valid(_raw_payload)
         THEN json_extract_string(_raw_payload, '$.rider_id')    END AS rider_id,
    CASE WHEN json_valid(_raw_payload)
         THEN json_extract_string(_raw_payload, '$.supplier_id') END AS supplier_id
FROM numbered
ORDER BY _source_row;
