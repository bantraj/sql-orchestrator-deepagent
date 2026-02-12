"""
Oracle Historical Data → BigQuery Migration DAG
================================================
Strategy  : Hourly incremental pull using a timestamp watermark
Start Date: 2025-01-01 00:00:00 UTC
Engine    : GCP Cloud Composer (Airflow 2.x) + Dataproc Spark
Author    : Your Team
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# ─────────────────────────────────────────────
# 1.  GLOBAL CONFIGURATION
#     Store secrets in Airflow Variables or
#     GCP Secret Manager — never hard-code them
# ─────────────────────────────────────────────
PROJECT_ID          = Variable.get("gcp_project_id",          default_var="my-gcp-project")
REGION              = Variable.get("gcp_region",               default_var="us-central1")
GCS_BUCKET          = Variable.get("gcs_bucket",               default_var="my-migration-bucket")
BQ_DATASET          = Variable.get("bq_dataset",               default_var="oracle_historical")
BQ_TABLE            = Variable.get("bq_table",                 default_var="transactions")
BQ_STAGING_DATASET  = Variable.get("bq_staging_dataset",       default_var="oracle_staging")

# Oracle connection (managed via Airflow Connection: oracle_default)
ORACLE_CONN_ID      = "oracle_default"          # host/port/sid/user/pass set in Airflow UI
ORACLE_SCHEMA       = Variable.get("oracle_schema",            default_var="FINANCE")
ORACLE_TABLE        = Variable.get("oracle_table",             default_var="TRANSACTIONS")
ORACLE_TS_COLUMN    = Variable.get("oracle_timestamp_column",  default_var="CREATED_AT")

# Dataproc cluster config
DATAPROC_CLUSTER    = Variable.get("dataproc_cluster_name",    default_var="oracle-migration-cluster")
SPARK_JOB_FILE      = f"gs://{GCS_BUCKET}/spark-jobs/oracle_to_bq_spark.py"

# GCS staging path (Spark writes Parquet here before BQ load)
GCS_STAGING_PREFIX  = f"gs://{GCS_BUCKET}/staging/oracle/{BQ_TABLE}"

# Watermark table in BigQuery — tracks last successfully loaded hour
WATERMARK_DATASET   = Variable.get("watermark_dataset",        default_var="pipeline_metadata")
WATERMARK_TABLE     = Variable.get("watermark_table",          default_var="oracle_watermarks")

# Historical backfill start — do NOT process anything before this
HISTORICAL_START    = datetime(2025, 1, 1, 0, 0, 0)

# ─────────────────────────────────────────────
# 2.  DATAPROC CLUSTER DEFINITION
# ─────────────────────────────────────────────
DATAPROC_CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 200},
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.sql.shuffle.partitions": "200",
            "spark:spark.executor.memory": "4g",
            "spark:spark.driver.memory": "4g",
        },
        # Oracle JDBC driver must be staged in GCS first
        "optional_components": ["JUPYTER"],
    },
    "initialization_actions": [
        {
            "executable_file": f"gs://{GCS_BUCKET}/init-scripts/install_oracle_jdbc.sh",
            "execution_timeout": "300s",
        }
    ],
    "gce_cluster_config": {
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform"
        ],
    },
}

# ─────────────────────────────────────────────
# 3.  DEFAULT DAG ARGUMENTS
# ─────────────────────────────────────────────
default_args = {
    "owner"            : "data-engineering",
    "depends_on_past"  : True,          # each run must succeed before the next
    "email"            : ["data-alerts@yourcompany.com"],
    "email_on_failure" : True,
    "email_on_retry"   : False,
    "retries"          : 3,
    "retry_delay"      : timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay"  : timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

# ─────────────────────────────────────────────
# 4.  PYTHON CALLABLES
# ─────────────────────────────────────────────

def validate_window(**context):
    """
    Enforce the historical start boundary.
    Airflow passes logical (execution) date as data_interval_start for Airflow 2.x.
    Each DAG run processes exactly ONE hour: [window_start, window_end).
    """
    window_start: datetime = context["data_interval_start"]
    window_end:   datetime = context["data_interval_end"]

    # Block anything before the historical start
    if window_start < HISTORICAL_START:
        raise ValueError(
            f"Window {window_start} is before the allowed historical start "
            f"{HISTORICAL_START}. Skipping."
        )

    # Push window boundaries to XCom so downstream tasks can use them
    context["ti"].xcom_push(key="window_start", value=window_start.strftime("%Y-%m-%d %H:%M:%S"))
    context["ti"].xcom_push(key="window_end",   value=window_end.strftime("%Y-%m-%d %H:%M:%S"))
    context["ti"].xcom_push(
        key="gcs_output_path",
        value=f"{GCS_STAGING_PREFIX}/{window_start.strftime('%Y/%m/%d/%H')}/"
    )

    print(f"Processing window: [{window_start}  →  {window_end})")


def check_watermark(**context):
    """
    Query the BQ watermark table.
    If this window has already been loaded successfully, skip to avoid duplicates.
    Returns branch name.
    """
    window_start = context["ti"].xcom_pull(task_ids="validate_window", key="window_start")

    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    query = f"""
        SELECT COUNT(1) AS cnt
        FROM `{PROJECT_ID}.{WATERMARK_DATASET}.{WATERMARK_TABLE}`
        WHERE table_name   = '{ORACLE_SCHEMA}.{ORACLE_TABLE}'
          AND window_start = TIMESTAMP('{window_start}')
          AND status       = 'SUCCESS'
    """
    result = bq_hook.get_first(sql=query)
    count  = result[0] if result else 0

    if count > 0:
        print(f"Window {window_start} already loaded. Skipping.")
        return "skip_already_loaded"
    return "create_dataproc_cluster"


def build_spark_args(**context):
    """Build the PySpark job argument list and push to XCom."""
    ti           = context["ti"]
    window_start = ti.xcom_pull(task_ids="validate_window", key="window_start")
    window_end   = ti.xcom_pull(task_ids="validate_window", key="window_end")
    gcs_path     = ti.xcom_pull(task_ids="validate_window", key="gcs_output_path")

    # Airflow Connection string for the Spark job
    # Spark reads connection details from these args (no plain-text secrets)
    spark_args = [
        "--oracle-url",        "{{ conn.oracle_default.host }}",
        "--oracle-port",       "{{ conn.oracle_default.port }}",
        "--oracle-service",    "{{ conn.oracle_default.schema }}",
        "--oracle-user",       "{{ conn.oracle_default.login }}",
        "--oracle-password-secret", f"projects/{PROJECT_ID}/secrets/oracle-db-password/versions/latest",
        "--oracle-schema",     ORACLE_SCHEMA,
        "--oracle-table",      ORACLE_TABLE,
        "--timestamp-column",  ORACLE_TS_COLUMN,
        "--window-start",      window_start,
        "--window-end",        window_end,
        "--gcs-output-path",   gcs_path,
        "--partition-column",  ORACLE_TS_COLUMN,
        "--num-partitions",    "8",
        "--fetch-size",        "10000",
    ]
    ti.xcom_push(key="spark_args", value=spark_args)
    ti.xcom_push(key="gcs_output_path_for_bq", value=gcs_path)


def upsert_watermark(status: str, **context):
    """Write/update the watermark record in BigQuery."""
    ti           = context["ti"]
    window_start = ti.xcom_pull(task_ids="validate_window", key="window_start")
    window_end   = ti.xcom_pull(task_ids="validate_window", key="window_end")
    run_id       = context["run_id"]

    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    merge_sql = f"""
        MERGE `{PROJECT_ID}.{WATERMARK_DATASET}.{WATERMARK_TABLE}` T
        USING (
            SELECT
                '{ORACLE_SCHEMA}.{ORACLE_TABLE}'  AS table_name,
                TIMESTAMP('{window_start}')        AS window_start,
                TIMESTAMP('{window_end}')          AS window_end,
                '{status}'                         AS status,
                '{run_id}'                         AS airflow_run_id,
                CURRENT_TIMESTAMP()                AS updated_at
        ) S
        ON  T.table_name   = S.table_name
        AND T.window_start = S.window_start
        WHEN MATCHED THEN
            UPDATE SET
                status          = S.status,
                airflow_run_id  = S.airflow_run_id,
                updated_at      = S.updated_at
        WHEN NOT MATCHED THEN
            INSERT (table_name, window_start, window_end, status, airflow_run_id, updated_at)
            VALUES (S.table_name, S.window_start, S.window_end, S.status, S.airflow_run_id, S.updated_at)
    """
    bq_hook.run_query(sql=merge_sql, use_legacy_sql=False)
    print(f"Watermark upserted: {window_start} → {status}")


# ─────────────────────────────────────────────
# 5.  DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id             = "oracle_to_bigquery_hourly_migration",
    description        = "Hourly incremental Oracle → BigQuery historical migration via Dataproc Spark",
    default_args       = default_args,
    start_date         = HISTORICAL_START,
    schedule_interval  = "@hourly",         # runs every hour; backfills from start_date
    catchup            = True,              # TRUE = backfill all hours from 2025-01-01
    max_active_runs    = 3,                 # allow up to 3 parallel hour-windows
    tags               = ["oracle", "bigquery", "migration", "dataproc", "spark"],
    doc_md             = __doc__,
) as dag:

    # ── 5.1  Validate time window ──────────────────────────────────────
    task_validate = PythonOperator(
        task_id         = "validate_window",
        python_callable = validate_window,
    )

    # ── 5.2  Check if already loaded (idempotency guard) ───────────────
    task_check_watermark = BranchPythonOperator(
        task_id         = "check_watermark",
        python_callable = check_watermark,
    )

    task_skip = EmptyOperator(task_id="skip_already_loaded")

    # ── 5.3  Mark window as IN_PROGRESS ────────────────────────────────
    task_mark_in_progress = PythonOperator(
        task_id         = "mark_in_progress",
        python_callable = upsert_watermark,
        op_kwargs       = {"status": "IN_PROGRESS"},
    )

    # ── 5.4  Build Spark arguments ─────────────────────────────────────
    task_build_spark_args = PythonOperator(
        task_id         = "build_spark_args",
        python_callable = build_spark_args,
    )

    # ── 5.5  Create Dataproc cluster (ephemeral per run) ───────────────
    task_create_cluster = DataprocCreateClusterOperator(
        task_id          = "create_dataproc_cluster",
        project_id       = PROJECT_ID,
        cluster_config   = DATAPROC_CLUSTER_CONFIG,
        region           = REGION,
        cluster_name     = f"{DATAPROC_CLUSTER}-{{{{ ts_nodash }}}}",
        # Use ts_nodash so parallel runs get unique clusters
        gcp_conn_id      = "google_cloud_default",
    )

    # ── 5.6  Submit Spark job ──────────────────────────────────────────
    task_spark_job = DataprocSubmitJobOperator(
        task_id    = "submit_spark_job",
        project_id = PROJECT_ID,
        region     = REGION,
        job        = {
            "reference"  : {"project_id": PROJECT_ID},
            "placement"  : {"cluster_name": f"{DATAPROC_CLUSTER}-{{{{ ts_nodash }}}}"},
            "pyspark_job": {
                "main_python_file_uri": SPARK_JOB_FILE,
                "args": "{{ ti.xcom_pull(task_ids='build_spark_args', key='spark_args') }}",
                "jar_file_uris": [
                    f"gs://{GCS_BUCKET}/jars/ojdbc8.jar",           # Oracle JDBC 8 driver
                    f"gs://{GCS_BUCKET}/jars/spark-bigquery-with-dependencies.jar",
                ],
                "properties": {
                    "spark.sql.shuffle.partitions"    : "200",
                    "spark.executor.instances"        : "4",
                    "spark.executor.cores"            : "2",
                    "spark.executor.memory"           : "4g",
                    "spark.driver.memory"             : "4g",
                    "spark.jars.packages"             : "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
                },
            },
        },
        gcp_conn_id = "google_cloud_default",
    )

    # ── 5.7  Delete Dataproc cluster ──────────────────────────────────
    task_delete_cluster = DataprocDeleteClusterOperator(
        task_id      = "delete_dataproc_cluster",
        project_id   = PROJECT_ID,
        cluster_name = f"{DATAPROC_CLUSTER}-{{{{ ts_nodash }}}}",
        region       = REGION,
        gcp_conn_id  = "google_cloud_default",
        trigger_rule = TriggerRule.ALL_DONE,   # delete even if Spark job fails
    )

    # ── 5.8  Load Parquet from GCS into BigQuery (MERGE / append) ─────
    task_bq_load = BigQueryInsertJobOperator(
        task_id        = "load_to_bigquery",
        project_id     = PROJECT_ID,
        gcp_conn_id    = "google_cloud_default",
        configuration  = {
            "load": {
                "sourceUris"            : ["{{ ti.xcom_pull(task_ids='build_spark_args', key='gcs_output_path_for_bq') }}*.parquet"],
                "destinationTable"      : {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_STAGING_DATASET,
                    "tableId"  : f"{BQ_TABLE}_{{{{ ds_nodash }}}}_{{{{ macros.ds_format(ts, '%Y%m%d%H%M%S', '%H') }}}}",
                },
                "sourceFormat"          : "PARQUET",
                "writeDisposition"      : "WRITE_TRUNCATE",   # staging table is always fresh
                "autodetect"            : True,
                "createDisposition"     : "CREATE_IF_NEEDED",
            }
        },
    )

    # ── 5.9  MERGE staging → final table (upsert / dedup) ─────────────
    task_bq_merge = BigQueryInsertJobOperator(
        task_id       = "merge_to_final_table",
        project_id    = PROJECT_ID,
        gcp_conn_id   = "google_cloud_default",
        configuration = {
            "query": {
                "query": f"""
                    MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` T
                    USING (
                        SELECT * FROM `{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_TABLE}_{{}}`
                    ) S
                    ON T.TRANSACTION_ID = S.TRANSACTION_ID     -- << adjust PK >>
                    WHEN MATCHED THEN
                        UPDATE SET
                            T.CREATED_AT  = S.CREATED_AT,
                            T.UPDATED_AT  = S.UPDATED_AT,
                            T.STATUS      = S.STATUS
                            -- add remaining columns here
                    WHEN NOT MATCHED THEN
                        INSERT ROW
                """,
                "useLegacySql"   : False,
                "writeDisposition": "WRITE_APPEND",
            }
        },
    )

    # ── 5.10 Quality check: row count in BQ > 0 ──────────────────────
    task_bq_quality_check = BigQueryCheckOperator(
        task_id     = "bq_quality_check",
        sql         = f"""
            SELECT COUNT(1) > 0
            FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE {ORACLE_TS_COLUMN} >= TIMESTAMP('{{{{ ti.xcom_pull(task_ids="validate_window", key="window_start") }}}}')
              AND {ORACLE_TS_COLUMN} <  TIMESTAMP('{{{{ ti.xcom_pull(task_ids="validate_window", key="window_end") }}}}')
        """,
        use_legacy_sql = False,
        gcp_conn_id    = "google_cloud_default",
    )

    # ── 5.11 Mark window SUCCESS ──────────────────────────────────────
    task_mark_success = PythonOperator(
        task_id         = "mark_success",
        python_callable = upsert_watermark,
        op_kwargs       = {"status": "SUCCESS"},
    )

    # ── 5.12 Mark window FAILED ───────────────────────────────────────
    task_mark_failed = PythonOperator(
        task_id         = "mark_failed",
        python_callable = upsert_watermark,
        op_kwargs       = {"status": "FAILED"},
        trigger_rule    = TriggerRule.ONE_FAILED,
    )

    # ── 5.13 Final join (for branching path) ─────────────────────────
    task_end = EmptyOperator(
        task_id      = "end",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ─────────────────────────────────────────
    # 6.  TASK DEPENDENCIES
    # ─────────────────────────────────────────
    (
        task_validate
        >> task_check_watermark
        >> [task_mark_in_progress, task_skip]
    )

    (
        task_mark_in_progress
        >> task_build_spark_args
        >> task_create_cluster
        >> task_spark_job
        >> task_delete_cluster
        >> task_bq_load
        >> task_bq_merge
        >> task_bq_quality_check
        >> task_mark_success
        >> task_end
    )

    # Failure path
    task_spark_job    >> task_mark_failed
    task_bq_load      >> task_mark_failed
    task_bq_merge     >> task_mark_failed
    task_mark_failed  >> task_end
    task_skip         >> task_end
