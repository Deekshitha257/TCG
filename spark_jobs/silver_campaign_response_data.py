import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, lower, when,
    from_utc_timestamp, to_date,
    get_json_object, lit
)
from pyspark.sql.types import StringType
from pyspark.sql.functions import sum as spark_sum


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("silver_campaign_response_data")

SMS_BRONZE_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/"
    "campaign-consumer-uat/"
    "sms_webhook_logs_cebc69be-534f-4e53-bd29-4d1114eeeaa9/data/"
)

WHATSAPP_BRONZE_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/"
    "campaign-consumer-uat/"
    "whatsapp_webhook_logs_f044cd78-3342-4bf4-bed6-00e1562f0b5a/data/"
)

EMAIL_BRONZE_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/"
    "campaign-consumer-uat/"
    "email_webhook_logs_210d1615-0c76-485f-96e5-013c6b653f08/data/"
)

SILVER_WAREHOUSE = "s3a://promotionengine-search/TCG_SILVER"
NESSIE_URI = "http://nessie:19120/api/v2"

LEAD_DIM_TABLE = "silver.sirrus_ai_silver.uat_dim_lead_360"
TARGET_TABLE = "silver.sirrus_ai_silver.campaign_response_data"


def create_spark():
    return (
        SparkSession.builder
        .appName("SILVER_CAMPAIGN_RESPONSE_DATA")
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
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .getOrCreate()
    )

def ensure_column(df, col_name, data_type=StringType()):
    if col_name not in df.columns:
        df = df.withColumn(col_name, lit(None).cast(data_type))
    return df


def main():
    spark = create_spark()
    logger.info("🚀 Building SILVER campaign_response_data")

    # Read bronze
    sms_df = spark.read.parquet(SMS_BRONZE_PATH)
    whatsapp_df = spark.read.parquet(WHATSAPP_BRONZE_PATH)
    email_df = spark.read.parquet(EMAIL_BRONZE_PATH)

    # Ensure schema consistency
    sms_df = ensure_column(sms_df, "profileId")
    whatsapp_df = ensure_column(whatsapp_df, "profileId")
    email_df = ensure_column(email_df, "profileId")
    email_df = ensure_column(email_df, "projectId")   # ✅ FIX

    sms_events = (
        sms_df
        .withColumn(
            "event_date",
            to_date(from_utc_timestamp(col("updatedAt").cast("timestamp"), "Asia/Kolkata"))
        )
                .withColumn(
            "canonical_event",
            when(
                upper(col("status")).isin(
                    "SENT","SUBMITD","DELIVRD","DLT HEADER FILTER","DLT TEMPLATE FILTER",
                    "DLT VL FAILURE","EXP-ABS-SUB","EXP-NW-FAIL","EXPIRED",
                    "FAILED","INVALID REQUEST"
                ),
                "sent"
            )
            .when(upper(col("status")).isin("FAILED","UNDELIV"), "failed")
            .when(lower(col("status")) == "click", "click")
        )

        .select(
            "event_date",
            col("organisationId").alias("orgId"),
            "projectId",
            "profileId",
            "canonical_event"
        )
        .withColumn("channel", lit("sms"))
    )

    whatsapp_events = (
        whatsapp_df
        .withColumn(
            "event_date",
            to_date(from_utc_timestamp(col("updatedAt").cast("timestamp"), "Asia/Kolkata"))
        )
        .withColumn("projectId", get_json_object(col("payload"), "$.projectId"))
        .withColumn(
            "canonical_event",
            when(upper(col("status")).isin("QUEUED","SENT"), "sent")
            .when(lower(col("status")).isin("delivered","deliv"), "delivered")
            .when(lower(col("status")).isin("read","seen","message_read"), "read")
            .when(lower(col("status")).isin("click","url_click"), "click")
            .when(lower(col("status")) == "failed", "failed")
            .when(lower(col("status")).isin("reply","replied"), "reply")
        )
        .select(
            "event_date",
            col("organisationId").alias("orgId"),
            "projectId",
            "profileId",
            "canonical_event"
        )
        .withColumn("channel", lit("whatsapp"))
    )

    email_events = (
        email_df
        .withColumn(
            "event_date",
            to_date(from_utc_timestamp(col("createdAt").cast("timestamp"), "Asia/Kolkata"))
        )
        .withColumn(
            "canonical_event",
            when(upper(col("status")).isin("PROCESSED","DROPPED","BOUNCED","REJECTED","SENT"), "sent")
            .when(lower(col("status")) == "delivered", "delivered")
            .when(lower(col("status")).isin("open","opened"), "open")
            .when(lower(col("status")).isin("click","clicked"), "click")
            .when(lower(col("status")) == "spam", "spam")
            .when(lower(col("status")).isin("unsubscribe","unsubscribed"), "unsubscribe")
        )
        .select(
            "event_date",
            col("organisationId").alias("orgId"),
            "projectId",
            "profileId",
            "canonical_event"
        )
        .withColumn("channel", lit("email"))
    )

    all_events = sms_events.unionByName(whatsapp_events).unionByName(email_events)

    agg = (
        all_events
        .groupBy("event_date", "orgId", "projectId", "profileId")
        .pivot("channel", ["email", "whatsapp", "sms"])
        .agg(
            spark_sum(when(col("canonical_event") == "sent", 1).otherwise(0)).alias("SENT"),
            spark_sum(when(col("canonical_event") == "delivered", 1).otherwise(0)).alias("DELIVERED"),
            spark_sum(when(col("canonical_event") == "open", 1).otherwise(0)).alias("OPENED"),
            spark_sum(when(col("canonical_event") == "click", 1).otherwise(0)).alias("CLICKED"),
            spark_sum(when(col("canonical_event") == "failed", 1).otherwise(0)).alias("FAILED"),
            spark_sum(when(col("canonical_event") == "reply", 1).otherwise(0)).alias("REPLIED"),
            spark_sum(when(col("canonical_event") == "spam", 1).otherwise(0)).alias("SPAM"),
            spark_sum(when(col("canonical_event") == "unsubscribe", 1).otherwise(0)).alias("UNSUBSCRIBED")
        )
    )


    lead_df = spark.read.table(LEAD_DIM_TABLE)

    final_df = (
    agg.alias("a")
    .join(
        lead_df.alias("l"),
        col("a.profileId") == col("l.lead_flat_id"),
        "inner"
    )
    .select(
        col("l.leadId"),
        col("l.lead_flat_id"),
        col("a.event_date"),
        col("a.orgId"),          
        col("a.projectId"),

        col("a.email_SENT").alias("EMAIL_SENT"),
        col("a.email_DELIVERED").alias("EMAIL_DELIVERED"),
        col("a.email_OPENED").alias("EMAIL_OPENED"),
        col("a.email_CLICKED").alias("EMAIL_CLICKED"),
        col("a.email_SPAM").alias("EMAIL_SPAM"),
        col("a.email_UNSUBSCRIBED").alias("EMAIL_UNSUBSCRIBED"),

        col("a.whatsapp_SENT").alias("WHATSAPP_SENT"),
        col("a.whatsapp_DELIVERED").alias("WHATSAPP_DELIVERED"),
        col("a.whatsapp_OPENED").alias("WHATSAPP_READ"),
        col("a.whatsapp_CLICKED").alias("WHATSAPP_CLICKED"),
        col("a.whatsapp_FAILED").alias("WHATSAPP_FAILED"),
        col("a.whatsapp_REPLIED").alias("WHATSAPP_REPLIED"),

        col("a.sms_SENT").alias("SMS_SENT"),
        col("a.sms_DELIVERED").alias("SMS_DELIVERED"),
        col("a.sms_FAILED").alias("SMS_FAILED"),
        col("a.sms_CLICKED").alias("SMS_CLICKED")
    )
)


    final_df.writeTo(TARGET_TABLE).createOrReplace()
    logger.info("✅ campaign_response_data table created successfully")

    spark.stop()

if __name__ == "__main__":
    main()
