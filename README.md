# 🏗️ Enterprise Lakehouse Architecture — Snowflake + DBT + Airflow

**By Ram Karne** · Enterprise Data Architect · [linkedin.com/in/ram-karne](https://linkedin.com/in/ram-karne)

---

## Overview

A production-ready reference architecture for a **cloud-native data lakehouse** using Snowflake, DBT, and Airflow. This project demonstrates the medallion architecture pattern (Bronze → Silver → Gold), AI-driven data quality with Snowflake Cortex, MDM consolidation, and enterprise data governance — patterns applied across real engagements at MJH Life Sciences, 24 Hour Fitness, and ServiceNow.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                │
│  CRM · ERP · Events · Marketing · Product · 3rd-party APIs     │
└───────────────────────┬─────────────────────────────────────────┘
                        │  Fivetran / Airflow ingestion
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Raw)                             │
│  • Raw ingestion, no transformation                             │
│  • Full history retained                                        │
│  • Partitioned by ingestion date                                │
└───────────────────────┬─────────────────────────────────────────┘
                        │  DBT models
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Cleansed)                        │
│  • Deduplication & standardization                              │
│  • AI-driven quality checks (Snowflake Cortex)                 │
│  • MDM identity resolution                                      │
└───────────────────────┬─────────────────────────────────────────┘
                        │  DBT models
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (Business-Ready)                    │
│  • Dimensional models (star schema)                             │
│  • Aggregated metrics for BI & ML                               │
│  • Row-level security applied                                   │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
            Power BI · Tableau · Snowflake Cortex AI
```

---

## Project Structure

```
snowflake-dbt-lakehouse/
├── snowflake_setup/
│   ├── 01_warehouse_setup.sql       # Warehouses, databases, roles, RBAC
│   └── 02_row_level_security.sql    # Row-level security policies
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── bronze/                  # Raw ingestion models
│   │   ├── silver/                  # Cleansed & validated models
│   │   └── gold/                    # Business-ready dimensional models
│   ├── macros/
│   │   └── data_quality.sql         # Reusable DQ macros
│   └── tests/
│       └── custom_tests.sql         # Custom data quality tests
├── governance/
│   └── data_quality_cortex.sql      # Snowflake Cortex AI quality checks
├── pipelines/
│   └── lakehouse_dag.py             # Airflow DAG for orchestration
└── docs/
    └── data_dictionary.md           # Enterprise data dictionary
```

---

## Key Patterns Demonstrated

| Pattern | Implementation |
|---|---|
| Medallion architecture | Bronze / Silver / Gold DBT model layers |
| AI data quality | Snowflake Cortex anomaly detection & classification |
| MDM identity resolution | Deterministic + probabilistic matching in Silver |
| RBAC & row-level security | Role-based access + dynamic data masking |
| Incremental loading | DBT incremental models with merge strategy |
| Data lineage | DBT docs + column-level lineage |
| Pipeline orchestration | Airflow DAG with retry logic and alerting |
| Data dictionary | Standardized metadata across all domains |

---

## Tech Stack

- **Warehouse**: Snowflake (multi-cluster, auto-scaling)
- **Transformation**: DBT Core
- **Orchestration**: Apache Airflow
- **AI / Quality**: Snowflake Cortex
- **Ingestion**: Fivetran
- **BI**: Power BI, Tableau
- **Governance**: Atlan, Informatica Axon
- **CI/CD**: GitHub Actions + DBT Cloud

---

## Real-World Impact

This architecture pattern — adapted from production implementations — delivered:
- **$2M** annual savings at 24 Hour Fitness through cloud modernization
- **$1.5M** revenue increase at MJH Life Sciences via AI-driven targeting
- **35%** improvement in data quality compliance across 63 brand domains
- **Zero** revenue disruption during Dynamics 365 → SOM migration at ServiceNow

---

## Getting Started

```bash
# Clone the repo
git clone https://github.com/karneram/snowflake-dbt-lakehouse.git
cd snowflake-dbt-lakehouse

# Set up DBT profile (edit with your Snowflake credentials)
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml

# Install DBT
pip install dbt-snowflake

# Run the full pipeline
dbt deps
dbt run --select bronze
dbt run --select silver
dbt run --select gold
dbt test
```

---

*Ram Karne · Enterprise Data Architect · ram_mohan17@yahoo.com · Carlsbad, CA*
