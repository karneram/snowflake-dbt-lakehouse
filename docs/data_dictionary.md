# Enterprise Data Dictionary
**Author**: Ram Karne · Enterprise Data Architect  
**Last updated**: 2025  
**Scope**: Lakehouse Bronze → Silver → Gold layers

---

## Table of Contents
- [Contacts](#contacts)
- [Events](#events)
- [Brands](#brands)
- [Data Quality Rules](#data-quality-rules)
- [MDM Golden Record Logic](#mdm-golden-record-logic)

---

## Contacts

### `SILVER.CONTACTS`
Master contact entity — MDM-resolved golden records from CRM, Marketing, and Events.

| Column | Type | Description | PII | DQ Rule |
|--------|------|-------------|-----|---------|
| `contact_key` | STRING | MD5 surrogate key: email + first_name + last_name | No | Not null, unique |
| `source_system` | STRING | Origin: CRM, MARKETING, EVENTS | No | Not null |
| `source_id` | STRING | Primary key from source system | No | Not null |
| `first_name` | STRING | Given name — INITCAP standardized | Yes | Not null |
| `last_name` | STRING | Family name — INITCAP standardized | Yes | Not null |
| `email` | STRING | Lowercase, trimmed, validated format | Yes | Not null, valid format, unique |
| `phone` | STRING | Formatted XXX-XXX-XXXX | Yes | Format validated |
| `company_name` | STRING | Employer / organization name | No | |
| `job_title` | STRING | Role title (raw) | No | |
| `mdm_golden_id` | STRING | Golden record key — earliest contact_key for email group | No | Not null |
| `dq_score` | NUMBER | 0–100 completeness score | No | Range 0–100 |
| `dedup_rank` | NUMBER | 1 = golden record; >1 = duplicate candidate | No | |
| `created_at` | TIMESTAMP | First seen in source | No | Not null |
| `updated_at` | TIMESTAMP | Last modified in source | No | |
| `silver_processed_at` | TIMESTAMP | When this Silver record was created | No | Not null |

---

## Events

### `GOLD.FACT_EVENTS`
Event registration and attendance fact table. Powers revenue reporting and ML audience scoring.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| `fact_key` | STRING | Surrogate key: event_id + contact_key | |
| `event_id` | STRING | Unique event identifier | |
| `event_date` | DATE | Date of the event | |
| `event_type` | STRING | CONFERENCE, WEBINAR, WORKSHOP, ROUNDTABLE | |
| `registration_source` | STRING | EMAIL, ORGANIC, PAID, REFERRAL | |
| `attended_flag` | BOOLEAN | True if contact actually attended | |
| `revenue_amount` | NUMBER(12,2) | Revenue attributed to this registration | |
| `contact_key` | STRING | FK → SILVER.CONTACTS.contact_key | |
| `brand_id` | STRING | FK → SILVER.BRANDS.brand_id | |
| `days_since_last_event` | NUMBER | Recency feature for ML targeting | |
| `events_last_12m` | NUMBER | Frequency feature for ML targeting | |
| `revenue_last_12m` | NUMBER(12,2) | Monetary feature for ML targeting | |
| `rfm_segment` | STRING | CHAMPION, LOYAL, RECENT, POTENTIAL, AT_RISK | |

---

## Brands

### `SILVER.BRANDS`
Brand master — 63 brands consolidated from MDM (Reltio).

| Column | Type | Description |
|--------|------|-------------|
| `brand_id` | STRING | Unique brand identifier from MDM |
| `brand_name` | STRING | Full brand name (e.g., "Oncology Times") |
| `brand_vertical` | STRING | ONCOLOGY, CARDIOLOGY, PRIMARY_CARE, etc. |
| `brand_region` | STRING | US, EU, APAC |
| `is_active` | BOOLEAN | Currently publishing / active |
| `launch_date` | DATE | Brand launch date |

---

## Data Quality Rules

| Rule ID | Table | Column | Rule | Threshold | Action |
|---------|-------|--------|------|-----------|--------|
| DQ-001 | SILVER.CONTACTS | email | Valid email format | 100% | Block |
| DQ-002 | SILVER.CONTACTS | email | Unique per golden record | 100% | Deduplicate |
| DQ-003 | SILVER.CONTACTS | dq_score | Avg score ≥ 75 | 75/100 | Alert steward |
| DQ-004 | GOLD.FACT_EVENTS | revenue_amount | No negative values | 100% | Quarantine |
| DQ-005 | GOLD.FACT_EVENTS | revenue_amount | Z-score ≤ 3 std dev | 99.7% | Flag anomaly |
| DQ-006 | SILVER.CONTACTS | first_name | Not null | 95% | Warn |
| DQ-007 | BRONZE.* | ingested_at | Freshness < 24 hours | 100% | Alert |

---

## MDM Golden Record Logic

Golden record selection follows a two-pass approach:

**Pass 1 — Deterministic matching** (exact email)
- Records sharing the same lowercase email are grouped
- Earliest `created_at` record within the group is selected as golden
- All others are flagged `dedup_rank > 1` for steward review

**Pass 2 — Probabilistic matching** (Cortex vector similarity)
- Cortex embeddings generated for `first_name + last_name + company_name`
- Cosine similarity > 0.92 flagged as potential duplicate even if emails differ
- Steward review required before merge

**MDM Confidence tiers**

| Tier | Method | Confidence | Action |
|------|--------|------------|--------|
| EXACT | Email match | 100% | Auto-merge |
| HIGH | Name + company similarity ≥ 0.95 | ~98% | Auto-merge with audit log |
| MEDIUM | Name + company similarity 0.92–0.95 | ~90% | Steward review queue |
| LOW | Name similarity only < 0.92 | <85% | Reject — keep separate |
