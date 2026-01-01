from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_lead_communication_logs_exploded",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["spark", "iceberg", "nessie", "silver"],
) as dag:

   explode_lead_communication_logs = SparkSubmitOperator(
    task_id="explode_lead_communication_logs",
    application="/opt/airflow/spark_jobs/silver_lead_communication_logs_exploded.py",

    conn_id="spark_local",  # ✅ THIS IS THE KEY

    packages=",".join([
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2",
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12:0.77.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.772"
    ]),

    conf={
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

        "spark.sql.catalog.silver": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.silver.catalog-impl":
            "org.apache.iceberg.nessie.NessieCatalog",
        "spark.sql.catalog.silver.uri": "http://nessie:19120/api/v2",
        "spark.sql.catalog.silver.ref": "main",
        "spark.sql.catalog.silver.warehouse":
            "s3a://promotionengine-search/TCG_SILVER",

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




