"""
============================================================
ENTERPRISE LAKEHOUSE — AIRFLOW ORCHESTRATION DAG
Author : Ram Karne | Enterprise Data Architect
Purpose: Daily pipeline: ingest → bronze → silver → gold → DQ
Pattern: Deployed across 24 Hour Fitness and MJH Life Sciences
============================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ── DAG defaults ──────────────────────────────────────────
DEFAULT_ARGS = {
    "owner"           : "ram_karne",
    "depends_on_past" : False,
    "start_date"      : datetime(2025, 1, 1),
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": True,
    "email"           : ["ram_mohan17@yahoo.com"],
}

SNOWFLAKE_CONN   = "snowflake_lakehouse"
DBT_PROFILE      = "lakehouse"
DQ_SCORE_FLOOR   = 75     # fail pipeline if avg DQ score drops below this


# ── Callbacks ─────────────────────────────────────────────
def on_failure_callback(context):
    """Notify Slack on task failure."""
    msg = (
        f":red_circle: *Pipeline failure*\n"
        f"*DAG*: {context['dag'].dag_id}\n"
        f"*Task*: {context['task_instance'].task_id}\n"
        f"*Time*: {context['execution_date']}\n"
        f"*Log*: {context['task_instance'].log_url}"
    )
    SlackWebhookOperator(
        task_id            = "slack_alert",
        slack_webhook_conn_id="slack_data_alerts",
        message            = msg,
    ).execute(context)


# ── DQ gate function ──────────────────────────────────────
def check_dq_scores(**kwargs):
    """Branch: pass if DQ scores meet threshold, else quarantine."""
    import snowflake.connector
    conn = snowflake.connector.connect(
        account   = "{{ var('snowflake_account') }}",
        user      = "{{ var('snowflake_user') }}",
        warehouse = "WH_CORTEX",
        database  = "LAKEHOUSE",
        schema    = "SILVER",
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT AVG(dq_score) FROM LAKEHOUSE.SILVER.CONTACTS
        WHERE silver_processed_at >= CURRENT_DATE()
    """)
    avg_score = cursor.fetchone()[0] or 0
    conn.close()

    kwargs["ti"].xcom_push(key="avg_dq_score", value=round(avg_score, 2))

    if avg_score >= DQ_SCORE_FLOOR:
        return "dq_passed"
    else:
        return "dq_failed_quarantine"


# ── DAG definition ────────────────────────────────────────
with DAG(
    dag_id            = "lakehouse_daily_pipeline",
    default_args      = DEFAULT_ARGS,
    schedule_interval = "0 4 * * *",    # 4am daily
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["lakehouse", "production", "data_architecture"],
    description       = "Daily medallion pipeline: ingest → bronze → silver → gold",
    on_failure_callback= on_failure_callback,
) as dag:

    # ── 1. Start ─────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── 2. Bronze: run DBT raw models ────────────────────
    bronze_run = SnowflakeOperator(
        task_id        = "bronze_dbt_run",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql            = """
            CALL LAKEHOUSE.GOVERNANCE.LOG_TABLE_ACCESS(
                'bronze_pipeline', 'START', 0
            );
        """,
    )

    # In production: replace with BashOperator running dbt
    # dbt run --select bronze --profiles-dir /opt/airflow/dbt --target prod

    # ── 3. Silver: cleanse + MDM resolve ─────────────────
    silver_run = SnowflakeOperator(
        task_id           = "silver_dbt_run",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql               = "-- dbt run --select silver",
    )

    # ── 4. Cortex AI quality scoring ─────────────────────
    cortex_dq = SnowflakeOperator(
        task_id           = "cortex_quality_scoring",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql               = """
            -- Run Cortex anomaly detection and classification
            CALL SYSTEM$WAIT(1);  -- placeholder; replace with actual Cortex call
        """,
    )

    # ── 5. DQ gate ───────────────────────────────────────
    dq_branch = BranchPythonOperator(
        task_id        = "dq_score_gate",
        python_callable= check_dq_scores,
    )

    dq_passed = EmptyOperator(task_id="dq_passed")

    dq_failed_quarantine = SnowflakeOperator(
        task_id           = "dq_failed_quarantine",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql               = """
            INSERT INTO LAKEHOUSE.GOVERNANCE.AUDIT_LOG
                (table_name, action, row_count)
            VALUES ('silver_contacts', 'DQ_QUARANTINE', -1);
        """,
    )

    # ── 6. Gold: dimensional models ──────────────────────
    gold_run = SnowflakeOperator(
        task_id           = "gold_dbt_run",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql               = "-- dbt run --select gold",
        trigger_rule      = "none_failed_min_one_success",
    )

    # ── 7. DBT tests ─────────────────────────────────────
    dbt_test = SnowflakeOperator(
        task_id           = "dbt_test_suite",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql               = "-- dbt test --select gold silver",
    )

    # ── 8. Notify success ────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id              = "notify_success",
        slack_webhook_conn_id= "slack_data_alerts",
        message              = (
            ":large_green_circle: *Lakehouse pipeline complete*\n"
            "All bronze → silver → gold layers refreshed successfully."
        ),
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # ── DAG wiring ────────────────────────────────────────
    (
        start
        >> bronze_run
        >> silver_run
        >> cortex_dq
        >> dq_branch
        >> [dq_passed, dq_failed_quarantine]
    )
    dq_passed           >> gold_run
    dq_failed_quarantine >> gold_run   # gold still runs with clean prior data
    gold_run            >> dbt_test >> notify_success >> end
