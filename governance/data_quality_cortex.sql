-- ============================================================
-- SNOWFLAKE CORTEX AI — Data Quality & Anomaly Detection
-- Author : Ram Karne | Enterprise Data Architect
-- Purpose: AI-driven data classification, anomaly detection,
--          and intelligent record enrichment using Cortex
-- Pattern: Deployed at ServiceNow for Dynamics 365 → SOM migration
-- ============================================================

-- ── 1. AI-DRIVEN ANOMALY DETECTION ────────────────────────
-- Detect anomalous revenue values using Cortex ML
-- Flags records that are statistical outliers for steward review

CREATE OR REPLACE TABLE LAKEHOUSE.GOVERNANCE.ANOMALY_FLAGS AS
WITH revenue_stats AS (
  SELECT
    brand_id,
    AVG(revenue_amount)    AS avg_revenue,
    STDDEV(revenue_amount) AS std_revenue
  FROM LAKEHOUSE.GOLD.FACT_EVENTS
  WHERE revenue_amount > 0
  GROUP BY brand_id
)
SELECT
  f.fact_key,
  f.brand_id,
  f.revenue_amount,
  s.avg_revenue,
  s.std_revenue,
  -- Z-score: values > 3 std dev flagged as anomalies
  ABS(f.revenue_amount - s.avg_revenue) / NULLIF(s.std_revenue, 0) AS z_score,
  CASE
    WHEN ABS(f.revenue_amount - s.avg_revenue) / NULLIF(s.std_revenue, 0) > 3
    THEN TRUE ELSE FALSE
  END AS is_anomaly,
  CURRENT_TIMESTAMP() AS detected_at
FROM LAKEHOUSE.GOLD.FACT_EVENTS f
JOIN revenue_stats s ON f.brand_id = s.brand_id;


-- ── 2. CORTEX AI RECORD CLASSIFICATION ───────────────────
-- Use Snowflake Cortex to classify job titles into
-- standardized personas for audience targeting

CREATE OR REPLACE TABLE LAKEHOUSE.SILVER.CONTACT_PERSONAS AS
SELECT
  contact_key,
  job_title,
  -- Cortex LLM: classify raw job title into standard persona
  SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    CONCAT(
      'Classify this job title into exactly one of these personas: ',
      'CLINICIAN, RESEARCHER, ADMINISTRATOR, EXECUTIVE, SALES, MARKETING, ENGINEERING, OTHER. ',
      'Job title: "', COALESCE(job_title, 'unknown'), '". ',
      'Return ONLY the persona label, nothing else.'
    )
  )                                                             AS ai_persona,
  CURRENT_TIMESTAMP()                                           AS classified_at
FROM LAKEHOUSE.SILVER.CONTACTS
WHERE job_title IS NOT NULL;


-- ── 3. CORTEX SENTIMENT — Event feedback scoring ─────────
-- Score open-text event feedback for satisfaction analysis

CREATE OR REPLACE TABLE LAKEHOUSE.SILVER.EVENT_FEEDBACK_SCORED AS
SELECT
  feedback_id,
  event_id,
  contact_key,
  feedback_text,
  -- Cortex sentiment: returns score between -1 (negative) and 1 (positive)
  SNOWFLAKE.CORTEX.SENTIMENT(feedback_text)                     AS sentiment_score,
  CASE
    WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) >  0.3 THEN 'POSITIVE'
    WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) < -0.3 THEN 'NEGATIVE'
    ELSE 'NEUTRAL'
  END                                                           AS sentiment_label,
  CURRENT_TIMESTAMP()                                           AS scored_at
FROM LAKEHOUSE.BRONZE.EVENT_FEEDBACK
WHERE feedback_text IS NOT NULL
  AND LENGTH(TRIM(feedback_text)) > 10;


-- ── 4. CORTEX AI — DATA QUALITY NARRATIVE ────────────────
-- Generate human-readable DQ issue summaries for stewards

CREATE OR REPLACE PROCEDURE LAKEHOUSE.GOVERNANCE.GENERATE_DQ_NARRATIVE(
  p_model_name   STRING,
  p_dq_score_avg FLOAT,
  p_null_pct     FLOAT,
  p_anomaly_cnt  NUMBER
)
RETURNS STRING
LANGUAGE SQL
AS $$
  LET narrative STRING;
  SET narrative = (
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
      'snowflake-arctic',
      CONCAT(
        'Write a 2-sentence data quality summary for a data steward. ',
        'Model: ', :p_model_name, '. ',
        'Average DQ score: ', :p_dq_score_avg, '/100. ',
        'Null rate: ', :p_null_pct, '%. ',
        'Anomaly count: ', :p_anomaly_cnt, '. ',
        'Be specific and actionable. Plain text, no markdown.'
      )
    )
  );
  RETURN narrative;
$$;


-- ── 5. INTELLIGENT DUPLICATE DETECTION ───────────────────
-- Use Cortex embeddings to find fuzzy-match duplicates
-- beyond exact email matching

CREATE OR REPLACE TABLE LAKEHOUSE.GOVERNANCE.POTENTIAL_DUPLICATES AS
WITH contact_embeddings AS (
  SELECT
    contact_key,
    email,
    first_name || ' ' || last_name || ' ' || COALESCE(company_name, '')
                                                              AS identity_string,
    -- Cortex: generate text embeddings for semantic similarity
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
      'snowflake-arctic-embed-m',
      first_name || ' ' || last_name || ' ' || COALESCE(company_name, '')
    )                                                         AS identity_vector
  FROM LAKEHOUSE.SILVER.CONTACTS
),
similarity_scores AS (
  SELECT
    a.contact_key                                             AS contact_key_a,
    b.contact_key                                             AS contact_key_b,
    a.email                                                   AS email_a,
    b.email                                                   AS email_b,
    a.identity_string                                         AS name_a,
    b.identity_string                                         AS name_b,
    VECTOR_COSINE_SIMILARITY(a.identity_vector, b.identity_vector)
                                                              AS similarity_score
  FROM contact_embeddings a
  JOIN contact_embeddings b
    ON a.contact_key < b.contact_key   -- avoid self-joins and duplicates
   AND a.email != b.email              -- exclude already-caught exact matches
)
SELECT *
FROM similarity_scores
WHERE similarity_score > 0.92          -- high-confidence fuzzy duplicates
ORDER BY similarity_score DESC;
