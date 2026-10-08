-- =============================================================================
-- 04_gold_orders.sql  --  One row per order
-- =============================================================================
-- This is the order lifecycle reconstructed from an unordered event stream.
--
-- EVENT TIME, NOT ARRIVAL ORDER:
--   Every milestone is min(event_time) FILTER (...), so the answer is a function of
--   *when things happened*, never of when they arrived or where they sat in the
--   file. There is no "last row wins" anywhere in this model, which is why
--   out-of-order and late-arriving records are handled correctly for free rather
--   than needing special-case logic.
--
-- MISSING MILESTONES STAY NULL:
--   STARTED_PICKUP is genuinely absent on a slice of delivered orders. We do not
--   interpolate it from the delivery time. NULL honestly means "never observed";
--   a fabricated value would look like data and silently poison every downstream
--   duration metric.
--
-- DETERMINISM:
--   Where an attribute has to be picked from a single event (city, supplier_id) the
--   pick is ordered by the struct (event_time, lifecycle_rank, event_id). event_id
--   is unique after dedup, so the ordering is total and the result is reproducible
--   even when two events share a timestamp.
-- =============================================================================

CREATE OR REPLACE TABLE gold_orders AS
WITH events AS (
    SELECT
        s.*,
        ref.lifecycle_rank,
        -- Total ordering key for an order's events. Structs compare field by field.
        {'t': s.event_time, 'r': ref.lifecycle_rank, 'e': s.event_id} AS order_key
    FROM silver_events AS s
    JOIN ref_event_types AS ref USING (event_type)
),
aggregated AS (
    SELECT
        order_id,

        -- Lifecycle milestones: first observed occurrence of each, by event time.
        min(event_time) FILTER (WHERE event_type = 'ORDER_CREATED')   AS created_at,
        min(event_time) FILTER (WHERE event_type = 'RIDER_ALLOCATED') AS first_allocated_at,
        min(event_time) FILTER (WHERE event_type = 'RIDER_ACCEPTED')  AS accepted_at,
        min(event_time) FILTER (WHERE event_type = 'STARTED_PICKUP')  AS started_pickup_at,
        min(event_time) FILTER (WHERE event_type = 'DELIVERED')       AS delivered_at,
        min(event_time) FILTER (WHERE event_type = 'CANCELLED')       AS cancelled_at,

        -- Operational counters. These are counted AFTER dedup, so a retried
        -- allocation event does not inflate the attempt count.
        count(*) FILTER (WHERE event_type = 'RIDER_ALLOCATED') AS allocation_attempts,
        count(DISTINCT rider_id) FILTER (
            WHERE event_type = 'RIDER_ALLOCATED' AND rider_id IS NOT NULL
        ) AS unique_riders,

        -- City is a property of the order, so prefer the value stated at creation
        -- and fall back to the earliest event that carried one.
        coalesce(
            min_by(city, order_key) FILTER (WHERE event_type = 'ORDER_CREATED' AND city IS NOT NULL),
            min_by(city, order_key) FILTER (WHERE city IS NOT NULL)
        ) AS city,

        -- Supplier is slowly changing WITHIN an order: it is reassigned on each
        -- allocation attempt. We therefore report the latest known value by event
        -- time (last-write-wins), not the one the order was created with.
        max_by(supplier_id, order_key) FILTER (WHERE supplier_id IS NOT NULL) AS supplier_id,

        count(*) AS event_count
    FROM events
    GROUP BY order_id
)
SELECT
    order_id,
    created_at,
    first_allocated_at,
    accepted_at,
    started_pickup_at,
    delivered_at,
    cancelled_at,
    -- Final state is decided by the terminal event with the LATEST event time.
    -- In this dataset an order never both delivers and cancels, but a real stream
    -- eventually will (e.g. a cancellation racing a delivery confirmation), so the
    -- rule is written explicitly rather than assumed away. The tie-break favours
    -- DELIVERED: a completed physical delivery is the stronger, harder-to-reverse
    -- fact than a cancellation request recorded at the same instant.
    CASE
        WHEN delivered_at IS NULL AND cancelled_at IS NULL THEN 'OPEN'
        WHEN cancelled_at IS NULL                          THEN 'DELIVERED'
        WHEN delivered_at IS NULL                          THEN 'CANCELLED'
        WHEN delivered_at >= cancelled_at                  THEN 'DELIVERED'
        ELSE 'CANCELLED'
    END AS final_status,
    allocation_attempts,
    unique_riders,
    city,
    supplier_id,
    event_count
FROM aggregated
ORDER BY order_id;
