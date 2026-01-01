from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# --------------------------------------------------
# DAG CONFIG
# --------------------------------------------------
DAG_ID = "silver_lead_flat_dag_v2"
SPARK_APP = "/opt/airflow/spark_jobs/silver_yukio_uat_lead_flat.py"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="S3 Bronze → Nessie Silver Lead Flatten (Iceberg + Nessie v1)",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["spark", "iceberg", "nessie", "silver"],
) as dag:

    silver_lead_flat = SparkSubmitOperator(
        task_id="silver_yukio_uat_lead_flat",
        application="/opt/airflow/spark_jobs/silver_yukio_uat_lead_flat.py",
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

    silver_lead_flat
