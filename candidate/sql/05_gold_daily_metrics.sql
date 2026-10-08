-- =============================================================================
-- 05_gold_daily_metrics.sql  --  Daily operational KPIs
-- =============================================================================
-- GRAIN AND ATTRIBUTION:
--   The reporting date is the order's CREATION date, as specified. This is cohort
--   attribution, not activity attribution: an order created on Sep-1 and delivered
--   on Sep-2 counts entirely towards Sep-1. That is the right choice for funnel
--   rates -- it keeps numerator and denominator inside the same cohort, so
--   fulfilment_rate actually means "of the orders we took that day, what share did
--   we deliver". It does mean the most recent day under-reports until its orders
--   finish, which is a known and expected property of cohort metrics.
--
-- ZERO DENOMINATORS:
--   nullif() makes every rate NULL rather than 0 or an error when nothing qualifies.
--   NULL and 0 are different claims: NULL means "undefined, we had nothing to
--   measure", 0 means "we tried and failed every time". Conflating them is how an
--   empty day shows up on a dashboard as a catastrophic outage.
--
-- ORDERS WITHOUT A CREATION DATE:
--   An order whose ORDER_CREATED event never arrived or was quarantined cannot be
--   attributed to any day, so it is excluded from this grain. It is NOT dropped
--   quietly: the count is surfaced as orders_missing_created_at in report.json.
--   Silent row loss is the one unforgivable failure in a pipeline; counted row loss
--   is just a data-quality metric.
-- =============================================================================

CREATE OR REPLACE TABLE gold_daily_metrics AS
WITH attributable_orders AS (
    SELECT *
    FROM gold_orders
    WHERE created_at IS NOT NULL
)
SELECT
    cast(created_at AS DATE) AS date,

    count(*)                                                       AS orders_created,
    count(*) FILTER (WHERE final_status = 'DELIVERED')             AS orders_delivered,
    count(*) FILTER (WHERE final_status = 'CANCELLED')             AS orders_cancelled,
    count(*) FILTER (WHERE first_allocated_at IS NOT NULL)         AS orders_allocated,
    count(*) FILTER (WHERE accepted_at IS NOT NULL)                AS orders_accepted,

    cast(count(*) FILTER (WHERE final_status = 'DELIVERED') AS DOUBLE)
        / nullif(count(*), 0)                                      AS fulfilment_rate,

    cast(count(*) FILTER (WHERE accepted_at IS NOT NULL) AS DOUBLE)
        / nullif(count(*) FILTER (WHERE first_allocated_at IS NOT NULL), 0)
                                                                   AS acceptance_rate,

    cast(avg(allocation_attempts) AS DOUBLE)                       AS avg_allocation_attempts
FROM attributable_orders
GROUP BY 1
ORDER BY 1;
