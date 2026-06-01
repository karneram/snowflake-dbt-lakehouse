-- ============================================================
-- ROW-LEVEL SECURITY & DYNAMIC DATA MASKING
-- Author : Ram Karne | Enterprise Data Architect
-- Purpose: Protect PII and enforce row-level access policies
-- ============================================================

-- ── DYNAMIC DATA MASKING — PII fields ───────────────────────

-- Email masking: analysts see j***@***.com, architects see full
CREATE MASKING POLICY IF NOT EXISTS LAKEHOUSE.GOVERNANCE.MASK_EMAIL
  AS (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('ROLE_DATA_ARCHITECT', 'ROLE_MDM_STEWARD')
        THEN val
      WHEN val IS NULL
        THEN NULL
      ELSE
        REGEXP_REPLACE(
          SPLIT_PART(val, '@', 1), '.', '*', 2) || '@' ||
          REGEXP_REPLACE(SPLIT_PART(val, '@', 2), '[^.]', '*')
    END;

-- Phone masking: show only last 4 digits to consumers
CREATE MASKING POLICY IF NOT EXISTS LAKEHOUSE.GOVERNANCE.MASK_PHONE
  AS (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('ROLE_DATA_ARCHITECT', 'ROLE_MDM_STEWARD')
        THEN val
      ELSE '***-***-' || RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 4)
    END;

-- SSN: fully masked for all except architect
CREATE MASKING POLICY IF NOT EXISTS LAKEHOUSE.GOVERNANCE.MASK_SSN
  AS (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() = 'ROLE_DATA_ARCHITECT' THEN val
      ELSE '***-**-****'
    END;

-- Apply masking policies to Silver contact table
ALTER TABLE LAKEHOUSE.SILVER.CONTACTS
  MODIFY COLUMN email   SET MASKING POLICY LAKEHOUSE.GOVERNANCE.MASK_EMAIL;
ALTER TABLE LAKEHOUSE.SILVER.CONTACTS
  MODIFY COLUMN phone   SET MASKING POLICY LAKEHOUSE.GOVERNANCE.MASK_PHONE;

-- ── ROW-LEVEL SECURITY — Brand / region isolation ────────────
-- Analysts only see data for their assigned brand(s)

-- Mapping table: user → allowed brands
CREATE TABLE IF NOT EXISTS LAKEHOUSE.GOVERNANCE.USER_BRAND_ACCESS (
  user_email   STRING    NOT NULL,
  brand_id     STRING    NOT NULL,
  granted_by   STRING,
  granted_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (user_email, brand_id)
);

-- Row access policy: filter rows based on brand ownership
CREATE ROW ACCESS POLICY IF NOT EXISTS LAKEHOUSE.GOVERNANCE.RAP_BRAND_ISOLATION
  AS (brand_id STRING) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ROLE_DATA_ARCHITECT', 'ROLE_DATA_ENGINEER')
    OR EXISTS (
      SELECT 1
      FROM   LAKEHOUSE.GOVERNANCE.USER_BRAND_ACCESS
      WHERE  user_email = CURRENT_USER()
        AND  brand_id   = brand_id
    );

-- Apply to Gold events table
ALTER TABLE LAKEHOUSE.GOLD.FACT_EVENTS
  ADD ROW ACCESS POLICY LAKEHOUSE.GOVERNANCE.RAP_BRAND_ISOLATION ON (brand_id);

-- ── AUDIT LOGGING ─────────────────────────────────────────────
-- Track all access to sensitive tables

CREATE TABLE IF NOT EXISTS LAKEHOUSE.GOVERNANCE.AUDIT_LOG (
  log_id        STRING    DEFAULT UUID_STRING(),
  event_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  user_name     STRING    DEFAULT CURRENT_USER(),
  role_name     STRING    DEFAULT CURRENT_ROLE(),
  query_id      STRING    DEFAULT CURRENT_STATEMENT(),
  table_name    STRING,
  action        STRING,
  row_count     NUMBER,
  PRIMARY KEY   (log_id)
);

-- Example: log procedure for sensitive table access
CREATE OR REPLACE PROCEDURE LAKEHOUSE.GOVERNANCE.LOG_TABLE_ACCESS(
  p_table_name STRING,
  p_action     STRING,
  p_row_count  NUMBER
)
RETURNS STRING
LANGUAGE SQL
AS $$
  INSERT INTO LAKEHOUSE.GOVERNANCE.AUDIT_LOG
    (table_name, action, row_count)
  VALUES
    (:p_table_name, :p_action, :p_row_count);
  RETURN 'Logged';
$$;
