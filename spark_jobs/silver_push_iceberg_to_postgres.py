import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col
from pyspark.sql.types import StructType, ArrayType, MapType

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("iceberg_to_postgres")

# --------------------------------------------------
# Spark / Iceberg / Nessie Config
# --------------------------------------------------
NESSIE_URI = "http://nessie:19120/api/v2"
SILVER_WAREHOUSE = "s3a://promotionengine-search/TCG_SILVER"

# --------------------------------------------------
# PostgreSQL Config
# --------------------------------------------------
PG_HOST = "postgres"
PG_PORT = "5432"
PG_DB = "sirrus_ai"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# --------------------------------------------------
# Tables to push (Iceberg → Postgres)
# --------------------------------------------------
TABLES = {
    "silver.sirrus_ai_silver.campaign_response_data": "campaign_response_data",
    "silver.sirrus_ai_silver.uat_campaign_engagement_sms": "uat_campaign_engagement_sms",
    "silver.sirrus_ai_silver.uat_campaign_engagement_whatsapp": "uat_campaign_engagement_whatsapp",
    "silver.sirrus_ai_silver.uat_dim_lead_360": "uat_dim_lead_360",
    "silver.crm.lead_flat": "lead_flat",
    "silver.crm.yukio_uat_lead_communication_logs_exploded": "lead_communication_logs_exploded"
}

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
def create_spark():
    return (
        SparkSession.builder
        .appName("ICEBERG_TO_POSTGRES")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.silver.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )
        .config("spark.sql.catalog.silver.uri", NESSIE_URI)
        .config("spark.sql.catalog.silver.ref", "main")
        .config("spark.sql.catalog.silver.warehouse", SILVER_WAREHOUSE)
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .getOrCreate()
    )

# --------------------------------------------------
# 🔧 FIX: Convert unsupported Spark types to JSON
# --------------------------------------------------
def prepare_for_postgres(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            df = df.withColumn(field.name, to_json(col(field.name)))
    return df

# --------------------------------------------------
# Main Logic
# --------------------------------------------------
def main():
    spark = create_spark()
    logger.info("🚀 Starting Iceberg → PostgreSQL sync")

    for iceberg_table, pg_table in TABLES.items():
        logger.info(f"📥 Reading Iceberg table: {iceberg_table}")

        df = spark.read.table(iceberg_table)
        row_count = df.count()

        logger.info(f"📊 {iceberg_table} rows = {row_count}")

        # 🔧 FIX APPLIED HERE (only change)
        df_pg = prepare_for_postgres(df)

        logger.info(f"📤 Writing to PostgreSQL table: {pg_table}")

        (
            df_pg.write
            .jdbc(
                url=PG_URL,
                table=pg_table,
                mode="overwrite",   # full refresh
                properties=PG_PROPS
            )
        )

        logger.info(f"✅ Successfully written: {pg_table}")

    logger.info("🎉 All tables pushed to PostgreSQL successfully")
    spark.stop()


if __name__ == "__main__":
    main()
