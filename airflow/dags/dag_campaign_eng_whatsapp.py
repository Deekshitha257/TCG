from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="silver_campaign_engagement_whatsapp",
    default_args=default_args,
    description="Build WhatsApp campaign engagement metrics (Silver layer)",
    schedule_interval=None,   
    start_date=days_ago(1),
    catchup=False,
    tags=["silver", "whatsapp", "campaign", "iceberg"],
) as dag:

    run_silver_campaign_engagement_whatsapp = SparkSubmitOperator(
        task_id="campaign_engagement_whatsapp",
        application="/opt/airflow/spark_jobs/silver_campaign_engagement_whatsapp.py",
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

    run_silver_campaign_engagement_whatsapp
