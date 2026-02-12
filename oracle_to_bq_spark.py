"""
oracle_to_bq_spark.py
=====================
PySpark job submitted by the Airflow DAG.
Reads one hourly window from Oracle via JDBC, writes Parquet to GCS.

GCS path  → Airflow DAG then loads it into BigQuery.
Cluster   : GCP Dataproc 2.1 (Spark 3.3)
JDBC      : Oracle ojdbc8.jar  (staged in GCS, mounted via --jar_file_uris)
"""

import argparse
import sys
from datetime import datetime

from google.cloud import secretmanager
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


# ─────────────────────────────────────────
# 1.  ARGUMENT PARSING
# ─────────────────────────────────────────
def parse_args(argv):
    parser = argparse.ArgumentParser(description="Oracle → GCS Spark extractor")

    # Oracle connection
    parser.add_argument("--oracle-url",             required=True,  help="Oracle DB host")
    parser.add_argument("--oracle-port",            required=True,  help="Oracle DB port (default 1521)")
    parser.add_argument("--oracle-service",         required=True,  help="Oracle service name or SID")
    parser.add_argument("--oracle-user",            required=True,  help="Oracle user")
    parser.add_argument("--oracle-password-secret", required=True,  help="GCP Secret Manager resource path")

    # Source
    parser.add_argument("--oracle-schema",     required=True,  help="Oracle schema name")
    parser.add_argument("--oracle-table",      required=True,  help="Oracle table name")
    parser.add_argument("--timestamp-column",  required=True,  help="Timestamp column used for windowing")
    parser.add_argument("--window-start",      required=True,  help="Window start (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--window-end",        required=True,  help="Window end   (YYYY-MM-DD HH:MM:SS)")

    # Output
    parser.add_argument("--gcs-output-path",   required=True,  help="GCS path for Parquet output")

    # Parallelism
    parser.add_argument("--partition-column",  default=None,   help="Column used for JDBC partitioning")
    parser.add_argument("--num-partitions",    type=int, default=8,  help="Number of JDBC read partitions")
    parser.add_argument("--fetch-size",        type=int, default=10000, help="JDBC fetchSize")

    return parser.parse_args(argv)


# ─────────────────────────────────────────
# 2.  FETCH SECRET FROM GCP SECRET MANAGER
# ─────────────────────────────────────────
def get_secret(secret_resource_path: str) -> str:
    client  = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": secret_resource_path})
    return response.payload.data.decode("UTF-8").strip()


# ─────────────────────────────────────────
# 3.  BUILD ORACLE JDBC URL
# ─────────────────────────────────────────
def build_jdbc_url(host: str, port: str, service: str) -> str:
    return f"jdbc:oracle:thin:@//{host}:{port}/{service}"


# ─────────────────────────────────────────
# 4.  COMPUTE PARTITION BOUNDS
#     Spark JDBC parallel reads need numeric
#     lower/upper bounds when partitioning.
#     We convert timestamps to epoch seconds.
# ─────────────────────────────────────────
def timestamp_to_epoch(ts_str: str) -> int:
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp())


# ─────────────────────────────────────────
# 5.  MAIN
# ─────────────────────────────────────────
def main(argv=None):
    args = parse_args(argv or sys.argv[1:])

    print(f"[Spark] Window: [{args.window_start}  →  {args.window_end})")
    print(f"[Spark] Source: {args.oracle_schema}.{args.oracle_table}")
    print(f"[Spark] Output: {args.gcs_output_path}")

    # Fetch Oracle password from Secret Manager
    oracle_password = get_secret(args.oracle_password_secret)

    jdbc_url = build_jdbc_url(args.oracle_url, args.oracle_port, args.oracle_service)

    # ── Spark session ────────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName(f"OracleToBQ_{args.oracle_table}_{args.window_start.replace(' ', 'T')}")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.writeLegacyFormat", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Build the SQL predicate for the hour window ──────────────────
    #    Using TO_TIMESTAMP for Oracle compatibility
    predicate_sql = (
        f"SELECT * FROM {args.oracle_schema}.{args.oracle_table} "
        f"WHERE {args.timestamp_column} >= "
        f"TO_TIMESTAMP('{args.window_start}', 'YYYY-MM-DD HH24:MI:SS') "
        f"AND {args.timestamp_column} < "
        f"TO_TIMESTAMP('{args.window_end}',   'YYYY-MM-DD HH24:MI:SS')"
    )

    # ── JDBC read options ────────────────────────────────────────────
    jdbc_options = {
        "url"          : jdbc_url,
        "dbtable"      : f"({predicate_sql}) ORACLE_SUBQUERY",
        "user"         : args.oracle_user,
        "password"     : oracle_password,
        "driver"       : "oracle.jdbc.driver.OracleDriver",
        "fetchsize"    : str(args.fetch_size),
        "sessionInitStatement": "BEGIN DBMS_APPLICATION_INFO.SET_MODULE('Spark Migration', 'READ'); END;",
    }

    # Enable parallel reads if a partition column is specified
    if args.partition_column:
        lower_bound = timestamp_to_epoch(args.window_start)
        upper_bound = timestamp_to_epoch(args.window_end)

        jdbc_options.update({
            "partitionColumn": f"EXTRACT(EPOCH FROM {args.partition_column})",  # virtual expr
            "lowerBound"     : str(lower_bound),
            "upperBound"     : str(upper_bound),
            "numPartitions"  : str(args.num_partitions),
        })

    print(f"[Spark] Reading with JDBC options (password redacted): "
          f"{  {k: v for k, v in jdbc_options.items() if k != 'password'} }")

    # ── Read from Oracle ─────────────────────────────────────────────
    df = spark.read.format("jdbc").options(**jdbc_options).load()

    record_count = df.count()
    print(f"[Spark] Records fetched: {record_count}")

    if record_count == 0:
        print("[Spark] No records found for this window. Writing empty marker.")
        # Write an empty Parquet so the BQ load doesn't fail
        spark.createDataFrame([], df.schema).write.mode("overwrite").parquet(args.gcs_output_path)
        spark.stop()
        return

    # ── Data quality / casting ───────────────────────────────────────
    # Ensure the timestamp column is properly typed
    df = df.withColumn(args.timestamp_column, F.col(args.timestamp_column).cast(TimestampType()))

    # Add pipeline metadata columns
    df = df.withColumn("_pipeline_window_start", F.lit(args.window_start))
    df = df.withColumn("_pipeline_window_end",   F.lit(args.window_end))
    df = df.withColumn("_pipeline_loaded_at",    F.current_timestamp())

    # ── Write Parquet to GCS ─────────────────────────────────────────
    (
        df.write
        .mode("overwrite")
        .partitionBy(args.timestamp_column[:10])  # date-level GCS sub-partition
        .parquet(args.gcs_output_path)
    )

    print(f"[Spark] Successfully written {record_count} records to {args.gcs_output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
