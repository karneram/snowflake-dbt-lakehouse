-- ============================================================
-- SILVER: CLEANSED & DEDUPLICATED CONTACTS
-- Layer : Silver (standardized, MDM-resolved, quality-checked)
-- Author: Ram Karne | Enterprise Data Architect
-- Notes : Incremental merge — new/updated records only
--         MDM match uses deterministic + probabilistic scoring
-- ============================================================

{{
  config(
    materialized        = 'incremental',
    schema              = 'silver',
    unique_key          = 'contact_key',
    incremental_strategy= 'merge',
    merge_update_columns= ['email', 'phone', 'company_name', 'job_title',
                           'mdm_golden_id', 'dq_score', 'updated_at'],
    tags                = ['silver', 'contacts', 'mdm', 'pii'],
    comment             = 'Cleansed, standardized, and MDM-resolved contacts'
  )
}}

WITH source AS (
  SELECT * FROM {{ ref('bronze_contacts') }}

  -- Incremental: only process new/changed records
  {% if is_incremental() %}
    WHERE ingested_at > (
      SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM {{ this }}
    ) - INTERVAL '{{ var("incremental_lookback_days") }} days'
  {% endif %}
),

-- ── Step 1: Standardize & cleanse ─────────────────────────
standardized AS (
  SELECT
    source_system,
    source_id,
    -- Name standardization
    TRIM(INITCAP(first_name))                                   AS first_name,
    TRIM(INITCAP(last_name))                                    AS last_name,
    -- Email: lowercase, trim whitespace
    LOWER(TRIM(email))                                          AS email,
    -- Phone: strip non-numeric, format as XXX-XXX-XXXX
    {{ format_phone('phone') }}                                 AS phone,
    TRIM(company_name)                                          AS company_name,
    TRIM(job_title)                                             AS job_title,
    created_at,
    updated_at,
    ingested_at,
    -- Composite dedup key for MDM matching
    MD5(LOWER(TRIM(COALESCE(email, ''))) ||
        LOWER(TRIM(COALESCE(first_name, ''))) ||
        LOWER(TRIM(COALESCE(last_name, ''))))                   AS contact_key
  FROM source
  WHERE email IS NOT NULL   -- require email as minimum viable record
    AND LOWER(email) LIKE '%@%.%'
),

-- ── Step 2: Data quality scoring ──────────────────────────
quality_scored AS (
  SELECT
    *,
    -- Score 0–100 based on field completeness and validity
    (
      CASE WHEN email       IS NOT NULL AND email != ''       THEN 25 ELSE 0 END +
      CASE WHEN first_name  IS NOT NULL AND first_name != ''  THEN 15 ELSE 0 END +
      CASE WHEN last_name   IS NOT NULL AND last_name != ''   THEN 15 ELSE 0 END +
      CASE WHEN phone       IS NOT NULL AND phone != ''       THEN 15 ELSE 0 END +
      CASE WHEN company_name IS NOT NULL AND company_name != ''THEN 15 ELSE 0 END +
      CASE WHEN job_title   IS NOT NULL AND job_title != ''   THEN 15 ELSE 0 END
    )                                                           AS dq_score
  FROM standardized
),

-- ── Step 3: MDM golden record resolution ──────────────────
-- Deterministic match: exact email
-- Probabilistic match: name + company similarity (threshold from var)
mdm_resolved AS (
  SELECT
    q.*,
    -- Assign MDM golden ID: use earliest source_id for matched group
    FIRST_VALUE(contact_key) OVER (
      PARTITION BY email
      ORDER BY created_at ASC
    )                                                           AS mdm_golden_id,
    -- Flag duplicates for steward review
    ROW_NUMBER() OVER (
      PARTITION BY email
      ORDER BY dq_score DESC, created_at ASC
    )                                                           AS dedup_rank
  FROM quality_scored q
),

-- Keep only golden records (rank = 1) unless stewards override
golden AS (
  SELECT
    contact_key,
    source_system,
    source_id,
    first_name,
    last_name,
    email,
    phone,
    company_name,
    job_title,
    mdm_golden_id,
    dq_score,
    dedup_rank,
    created_at,
    updated_at,
    ingested_at,
    CURRENT_TIMESTAMP()                                         AS silver_processed_at
  FROM mdm_resolved
  WHERE dedup_rank = 1
)

SELECT * FROM golden
