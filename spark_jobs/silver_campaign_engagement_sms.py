import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, get_json_object,
    from_utc_timestamp, to_date,
    when, min as spark_min, max as spark_max,
    countDistinct, coalesce, lit
)
from pyspark.sql.types import DoubleType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("silver_campaign_engagement_sms")


NESSIE_URI = "http://nessie:19120/api/v2"
SILVER_WAREHOUSE = "s3a://promotionengine-search/TCG_SILVER"

SMS_BRONZE_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/"
    "campaign-consumer-uat/"
    "sms_logs_e3133a3c-4934-4845-b272-bff94a276f67/data/"
)

CAMPAIGNS_BRONZE_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/"
    "marchestrator/"
    "campaigns_ae3a609c-9494-4e95-83e1-8be8c16336c6/data/"
)

LEAD_360_TABLE = "silver.sirrus_ai_silver.uat_dim_lead_360"
TARGET_TABLE = "silver.sirrus_ai_silver.uat_campaign_engagement_sms"


def create_spark():
    return (
        SparkSession.builder
        .appName("silver_campaign_engagement_sms")
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


def main():
    spark = create_spark()
    logger.info("🚀 Building SILVER campaign_engagement_sms")

    sms_logs = spark.read.parquet(SMS_BRONZE_PATH)
    campaigns = spark.read.parquet(CAMPAIGNS_BRONZE_PATH)
    lead_360 = spark.table(LEAD_360_TABLE)

  
    sms_raw = (
        sms_logs.alias("s")
        .join(
            campaigns.alias("yc"),
            col("s.campaignId") == col("yc._id"),
            "left"
        )
        .join(
            lead_360.alias("l"),
            regexp_replace(col("s.to").cast("string"), "[^0-9]", "") ==
            col("l.normalized_whatsapp_number"),
            "left"
        )
        .where(col("s.campaignId").isNotNull())
        .select(
            col("l.leadId"),
            col("s.campaignId").alias("campaignid"),
            col("yc.name").alias("campaignname"),
            col("yc.channel").alias("campaignchannel"),

            lower(
                when(
                    get_json_object(col("s.providerResponse"), "$.status.code").isNotNull(),
                    get_json_object(col("s.providerResponse"), "$.status.code")
                )
                .when(col("s.type").isNotNull(), col("s.type"))
                .when(
                    get_json_object(col("s.payload"), "$.status").isNotNull(),
                    get_json_object(col("s.payload"), "$.status")
                )
                .otherwise(col("s._id"))
            ).alias("raw_status"),

            when(
                get_json_object(col("s.providerResponse"), "$.status.code") == "200",
                "delivered"
            ).when(
                lower(get_json_object(col("s.providerResponse"), "$.status.desc"))
                .like("%accept%"),
                "delivered"
            ).when(
                lower(col("s.type")).isin("delivered", "delivery"),
                "delivered"
            ).otherwise("other").alias("canonical_event"),

            from_utc_timestamp(
                col("s.createdAt").cast("timestamp"),
                "Asia/Kolkata"
            ).alias("event_ts_ist"),

            coalesce(col("l.site_visit_scheduled"), lit(False))
            .alias("site_visit_scheduled"),

            col("l.siteVisit_date"),

            when(
                (col("l.bookingId").isNotNull()) &
                (lower(col("l.sirrus_lead_status")) == "booked"),
                1
            ).otherwise(0).alias("booked_flag"),

            when(
                (
                    regexp_replace(
                        coalesce(col("l.callDuration"), lit("0")),
                        "[^0-9.]",
                        ""
                    ).cast(DoubleType()) > 0
                )
                | lower(coalesce(col("l.callStatus"), lit(""))).isin(
                    "connected", "answered", "completed", "in-progress"
                ),
                1
            ).otherwise(0).alias("calls_connected_flag")
        )
    )

  
    sms_events_dedup = (
        sms_raw
        .where(col("leadId").isNotNull())
        .groupBy(
            "leadId",
            "campaignid",
            "campaignname",
            "campaignchannel",
            "canonical_event"
        )
        .agg(
            spark_min("event_ts_ist").alias("first_event_ts_ist"),
            spark_max("site_visit_scheduled").alias("site_visit_scheduled"),
            spark_max("siteVisit_date").alias("siteVisit_date"),
            spark_max("booked_flag").alias("booked_flag"),
            spark_max("calls_connected_flag").alias("calls_connected_flag")
        )
    )

   
    lead_flags = (
        sms_events_dedup
        .groupBy(
            "campaignid",
            "campaignname",
            "campaignchannel",
            "leadId"
        )
        .agg(
            spark_min("first_event_ts_ist").alias("first_event_ts_ist"),
            to_date(spark_min("first_event_ts_ist")).alias("event_date"),

            spark_max(
                when(col("canonical_event") == "delivered", 1).otherwise(0)
            ).alias("has_sms_delivered"),

            spark_max("calls_connected_flag").alias("calls_connected_flag"),

            spark_max(
                when(col("site_visit_scheduled") == True, 1).otherwise(0)
            ).alias("site_visit_scheduled_flag"),

            spark_max(
                when(col("siteVisit_date").isNotNull(), 1).otherwise(0)
            ).alias("site_visit_delivered_flag"),

            spark_max("booked_flag").alias("booked_flag")
        )
    )

    final_df = (
        lead_flags
        .groupBy(
            "campaignid",
            "campaignname",
            "campaignchannel",
            "event_date"
        )
        .agg(
            countDistinct("leadId").alias("total_unique_leads"),

            countDistinct(
                when(col("has_sms_delivered") == 1, col("leadId"))
            ).alias("sms_delivered_unique"),

            countDistinct(
                when(col("calls_connected_flag") == 1, col("leadId"))
            ).alias("calls_connected_unique"),

            countDistinct(
                when(col("site_visit_scheduled_flag") == 1, col("leadId"))
            ).alias("site_visit_scheduled_any_unique"),

            countDistinct(
                when(col("site_visit_delivered_flag") == 1, col("leadId"))
            ).alias("site_visit_delivered_any_unique"),

            countDistinct(
                when(col("booked_flag") == 1, col("leadId"))
            ).alias("booked_any_unique")
        )
    )

    final_df.writeTo(TARGET_TABLE).createOrReplace()
    logger.info("✅ campaign_engagement_sms table created successfully")

    spark.stop()


if __name__ == "__main__":
    main()
