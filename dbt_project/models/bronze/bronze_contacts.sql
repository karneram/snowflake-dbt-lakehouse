-- ============================================================
-- BRONZE: RAW CONTACTS
-- Layer : Bronze (raw ingestion — no transformations)
-- Source: Fivetran → CRM, Marketing, Events sources
-- Author: Ram Karne | Enterprise Data Architect
-- ============================================================

{{
  config(
    materialized = 'table',
    schema       = 'bronze',
    tags         = ['bronze', 'contacts', 'pii'],
    comment      = 'Raw contact records from all source systems. No dedup, no cleansing.'
  )
}}

WITH crm_contacts AS (
  SELECT
    'CRM'                             AS source_system,
    id                                AS source_id,
    first_name,
    last_name,
    email,
    phone,
    company_name,
    job_title,
    created_at,
    updated_at,
    _fivetran_synced                  AS ingested_at
  FROM {{ source('crm', 'contacts') }}
  WHERE _fivetran_deleted = FALSE
),

marketing_contacts AS (
  SELECT
    'MARKETING'                       AS source_system,
    contact_id                        AS source_id,
    fname                             AS first_name,
    lname                             AS last_name,
    email_address                     AS email,
    mobile                            AS phone,
    organization                      AS company_name,
    title                             AS job_title,
    created_date                      AS created_at,
    last_modified                     AS updated_at,
    _fivetran_synced                  AS ingested_at
  FROM {{ source('marketing', 'subscribers') }}
  WHERE unsubscribed = FALSE
),

event_contacts AS (
  SELECT
    'EVENTS'                          AS source_system,
    registration_id                   AS source_id,
    registrant_first_name             AS first_name,
    registrant_last_name              AS last_name,
    registrant_email                  AS email,
    registrant_phone                  AS phone,
    registrant_company                AS company_name,
    registrant_title                  AS job_title,
    registration_date                 AS created_at,
    registration_date                 AS updated_at,
    CURRENT_TIMESTAMP()               AS ingested_at
  FROM {{ source('events', 'registrations') }}
)

SELECT * FROM crm_contacts
UNION ALL
SELECT * FROM marketing_contacts
UNION ALL
SELECT * FROM event_contacts
