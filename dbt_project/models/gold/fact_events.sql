-- ============================================================
-- GOLD: FACT_EVENTS — Event Engagement Dimensional Model
-- Layer : Gold (business-ready star schema)
-- Author: Ram Karne | Enterprise Data Architect
-- Notes : Powers executive dashboards and ML audience scoring
--         Implements the pattern that drove $1.5M revenue lift
--         at MJH Life Sciences via personalized targeting
-- ============================================================

{{
  config(
    materialized = 'table',
    schema       = 'gold',
    tags         = ['gold', 'events', 'revenue', 'ml_ready'],
    comment      = 'Event registrations joined with golden contact records and brand dimensions'
  )
}}

WITH events AS (
  SELECT * FROM {{ ref('silver_events') }}
),

contacts AS (
  SELECT * FROM {{ ref('silver_contacts') }}
),

brands AS (
  SELECT * FROM {{ ref('silver_brands') }}
),

-- ── Engagement scoring for AI targeting ───────────────────
-- This pattern drove 30% engagement lift at MJH Life Sciences
engagement_features AS (
  SELECT
    e.event_id,
    e.contact_key,
    e.brand_id,
    e.event_date,
    e.event_type,
    e.registration_source,
    e.attended_flag,
    e.revenue_amount,

    -- Recency: days since last event (lower = more engaged)
    DATEDIFF('day',
      LAG(e.event_date) OVER (PARTITION BY e.contact_key ORDER BY e.event_date),
      e.event_date
    )                                                           AS days_since_last_event,

    -- Frequency: total events attended in last 12 months
    COUNT(*) OVER (
      PARTITION BY e.contact_key
      ORDER BY e.event_date
      RANGE BETWEEN INTERVAL '365 days' PRECEDING AND CURRENT ROW
    )                                                           AS events_last_12m,

    -- Monetary: revenue in last 12 months
    SUM(e.revenue_amount) OVER (
      PARTITION BY e.contact_key
      ORDER BY e.event_date
      RANGE BETWEEN INTERVAL '365 days' PRECEDING AND CURRENT ROW
    )                                                           AS revenue_last_12m

  FROM events e
),

-- ── RFM segmentation ──────────────────────────────────────
rfm_segments AS (
  SELECT
    *,
    CASE
      WHEN events_last_12m >= 5 AND revenue_last_12m >= 1000 THEN 'CHAMPION'
      WHEN events_last_12m >= 3 AND revenue_last_12m >= 500  THEN 'LOYAL'
      WHEN days_since_last_event <= 30                        THEN 'RECENT'
      WHEN events_last_12m >= 2                               THEN 'POTENTIAL'
      ELSE                                                         'AT_RISK'
    END                                                         AS rfm_segment
  FROM engagement_features
)

-- ── Final fact table join ──────────────────────────────────
SELECT
  -- Surrogate key
  {{ dbt_utils.generate_surrogate_key(['r.event_id', 'r.contact_key']) }}
                                                                AS fact_key,

  -- Event dimensions
  r.event_id,
  r.event_date,
  r.event_type,
  r.registration_source,
  r.attended_flag,
  r.revenue_amount,

  -- Contact dimensions (from MDM golden record)
  c.contact_key,
  c.mdm_golden_id,
  c.first_name,
  c.last_name,
  c.email,
  c.company_name,
  c.job_title,
  c.dq_score                                                    AS contact_dq_score,

  -- Brand dimension
  r.brand_id,
  b.brand_name,
  b.brand_vertical,
  b.brand_region,

  -- ML features for audience targeting model
  r.days_since_last_event,
  r.events_last_12m,
  r.revenue_last_12m,
  r.rfm_segment,

  -- Audit
  CURRENT_TIMESTAMP()                                           AS gold_processed_at

FROM rfm_segments r
LEFT JOIN contacts c ON r.contact_key = c.contact_key
LEFT JOIN brands   b ON r.brand_id    = b.brand_id
