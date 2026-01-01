from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# --------------------------------------------------
# DEFAULT ARGS
# --------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
with DAG(
    dag_id="silver_dim_lead_360",
    default_args=default_args,
    description="RAW + NESSIE → SILVER DIM LEAD 360 (Iceberg)",
    schedule_interval=None,  # manual trigger (recommended for now)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "nessie", "iceberg", "crm"],
) as dag:

    run_dim_lead_360 = SparkSubmitOperator(
        task_id="dim_lead_360",
        application="/opt/airflow/spark_jobs/dim360.py",
        conn_id="spark_local",

        # 🔥 REQUIRED PACKAGES (THIS FIXES S3A ERROR)
        packages=",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2",
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12:0.77.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.772"
        ]),

        conf={
            "spark.master": "local[*]",

            # Iceberg
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

            # Nessie + Silver catalog
            "spark.sql.catalog.silver": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.silver.catalog-impl":
                "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.silver.uri": "http://nessie:19120/api/v2",
            "spark.sql.catalog.silver.ref": "main",
            "spark.sql.catalog.silver.warehouse":
                "s3a://promotionengine-search/TCG_SILVER",

            # 🔥 S3A CONFIG (MOST IMPORTANT)
            "spark.hadoop.fs.s3a.impl":
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",

            "spark.sql.session.timeZone": "Asia/Kolkata",
        },

        executor_memory="6g",
        driver_memory="4g",
        verbose=True
    )

    run_dim_lead_360
