-- ============================================================
-- DBT MACROS — Reusable Data Quality & Utility Functions
-- Author : Ram Karne | Enterprise Data Architect
-- ============================================================


-- ── Phone number formatter ────────────────────────────────
-- Strips non-numeric characters and formats as XXX-XXX-XXXX
{% macro format_phone(column_name) %}
  CASE
    WHEN {{ column_name }} IS NULL THEN NULL
    WHEN LENGTH(REGEXP_REPLACE({{ column_name }}, '[^0-9]', '')) = 10
      THEN REGEXP_REPLACE(
             REGEXP_REPLACE({{ column_name }}, '[^0-9]', ''),
             '([0-9]{3})([0-9]{3})([0-9]{4})',
             '\\1-\\2-\\3'
           )
    WHEN LENGTH(REGEXP_REPLACE({{ column_name }}, '[^0-9]', '')) = 11
      THEN REGEXP_REPLACE(
             RIGHT(REGEXP_REPLACE({{ column_name }}, '[^0-9]', ''), 10),
             '([0-9]{3})([0-9]{3})([0-9]{4})',
             '\\1-\\2-\\3'
           )
    ELSE {{ column_name }}
  END
{% endmacro %}


-- ── Null rate assertion ───────────────────────────────────
-- Fails if null % in a column exceeds threshold
{% macro assert_null_rate(model, column_name, threshold_pct=5) %}
  SELECT
    '{{ model }}.{{ column_name }}'               AS check_name,
    COUNT(*)                                       AS total_rows,
    SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END)
                                                   AS null_count,
    ROUND(
      100.0 * SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END) / COUNT(*),
      2
    )                                              AS null_pct,
    {{ threshold_pct }}                            AS threshold_pct,
    CASE
      WHEN ROUND(
        100.0 * SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2
      ) > {{ threshold_pct }}
      THEN 'FAIL' ELSE 'PASS'
    END                                            AS result
  FROM {{ ref(model) }}
{% endmacro %}


-- ── Freshness check ───────────────────────────────────────
-- Warns if most recent record is older than expected
{% macro assert_freshness(model, timestamp_column, warn_hours=24, error_hours=48) %}
  SELECT
    '{{ model }}.{{ timestamp_column }}'          AS check_name,
    MAX({{ timestamp_column }})                   AS max_timestamp,
    DATEDIFF('hour', MAX({{ timestamp_column }}), CURRENT_TIMESTAMP()) AS hours_stale,
    CASE
      WHEN DATEDIFF('hour', MAX({{ timestamp_column }}), CURRENT_TIMESTAMP())
           > {{ error_hours }} THEN 'ERROR'
      WHEN DATEDIFF('hour', MAX({{ timestamp_column }}), CURRENT_TIMESTAMP())
           > {{ warn_hours }}  THEN 'WARN'
      ELSE 'PASS'
    END                                           AS result
  FROM {{ ref(model) }}
{% endmacro %}


-- ── Email validator ───────────────────────────────────────
{% macro is_valid_email(column_name) %}
  (
    {{ column_name }} IS NOT NULL
    AND {{ column_name }} LIKE '%@%.%'
    AND {{ column_name }} NOT LIKE '% %'
    AND LENGTH({{ column_name }}) >= 6
  )
{% endmacro %}


-- ── DQ summary macro ─────────────────────────────────────
-- Generates a data quality summary for any model
{% macro dq_summary(model) %}
  SELECT
    '{{ model }}'                                  AS model_name,
    COUNT(*)                                       AS total_rows,
    AVG(dq_score)                                  AS avg_dq_score,
    MIN(dq_score)                                  AS min_dq_score,
    SUM(CASE WHEN dq_score < 50 THEN 1 ELSE 0 END) AS low_quality_rows,
    SUM(CASE WHEN dq_score >= 80 THEN 1 ELSE 0 END) AS high_quality_rows,
    CURRENT_TIMESTAMP()                            AS checked_at
  FROM {{ ref(model) }}
{% endmacro %}
