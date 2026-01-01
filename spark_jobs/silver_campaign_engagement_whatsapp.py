import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, get_json_object,
    from_unixtime, from_utc_timestamp, to_date,
    when, min as spark_min, max as spark_max,
    countDistinct
)
from pyspark.sql.types import LongType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("silver_campaign_engagement_whatsapp")


NESSIE_URI = "http://nessie:19120/api/v2"
SILVER_WAREHOUSE = "s3a://promotionengine-search/TCG_SILVER"

WHATSAPP_BRONZE_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/"
    "campaign-consumer-uat/"
    "whatsapp_webhook_logs_f044cd78-3342-4bf4-bed6-00e1562f0b5a/data/"
)

CAMPAIGNS_BRONZE_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/"
    "marchestrator/"
    "campaigns_ae3a609c-9494-4e95-83e1-8be8c16336c6/data/"
)

LEAD_360_TABLE = "silver.sirrus_ai_silver.uat_dim_lead_360"

TARGET_TABLE = "silver.sirrus_ai_silver.uat_campaign_engagement_whatsapp"


def create_spark():
    return (
        SparkSession.builder
        .appName("SILVER_CAMPAIGN_ENGAGEMENT_WHATSAPP")
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
    logger.info("🚀 Building SILVER campaign_engagement_whatsapp")

    whatsapp_logs = spark.read.parquet(WHATSAPP_BRONZE_PATH)
    campaigns = spark.read.parquet(CAMPAIGNS_BRONZE_PATH)

    lead_360 = spark.table(LEAD_360_TABLE)

    whatsapp_raw = (
        whatsapp_logs.alias("y")
        .join(
            campaigns.alias("yc"),
            (col("y.campaignId") == col("yc._id")) &
            (col("y.organisationId") == col("yc.organisationId")),
            "left"
        )
        .join(
            lead_360.alias("l"),
            (col("l.profile_whatsAppNumber") ==
             regexp_replace(
                 get_json_object(col("y.payload"), "$.recipient.to"),
                 "[^0-9]", ""
             )) &
            (col("l.orgId") == col("y.organisationId")),
            "left"
        )
        .where(col("y.campaignId").isNotNull())
        .select(
            col("l.leadId"),
            col("y.campaignId").alias("campaignid"),
            col("yc.name").alias("campaignname"),
            col("yc.channel").alias("campaignchannel"),
            col("y.organisationId").alias("orgId"),

            when(lower(col("y.status")).isin("read", "seen", "message_read"), "read")
            .when(lower(col("y.status")).isin("click", "url_click"), "click")
            .when(lower(col("y.status")).isin("delivered", "deliv"), "delivered")
            .otherwise(lower(col("y.status")))
            .alias("canonical_event"),

            from_utc_timestamp(
                from_unixtime(
                    get_json_object(
                        col("y.payload"), "$.events.timestamp"
                    ).cast(LongType()) / 1000
                ).cast("timestamp"),
                "Asia/Kolkata"
            ).alias("event_ts_ist"),

            when(col("l.site_visit_scheduled").isNull(), False)
            .otherwise(col("l.site_visit_scheduled"))
            .alias("site_visit_scheduled"),

            col("l.siteVisit_date"),

            when(
                (col("l.bookingId").isNotNull()) &
                (lower(col("l.sirrus_lead_status")) == "booked"),
                1
            ).otherwise(0).alias("booked_flag")
        )
    )

    events_dedup = (
        whatsapp_raw
        .where(col("leadId").isNotNull())
        .groupBy(
            "leadId",
            "campaignid",
            "campaignname",
            "campaignchannel",
            "orgId",
            "canonical_event"
        )
        .agg(
            spark_min("event_ts_ist").alias("first_event_ts_ist"),
            spark_max("site_visit_scheduled").alias("site_visit_scheduled"),
            spark_max("siteVisit_date").alias("siteVisit_date"),
            spark_max("booked_flag").alias("booked_flag")
        )
    )

  
    lead_flags = (
        events_dedup
        .groupBy(
            "campaignid",
            "campaignname",
            "campaignchannel",
            "orgId",
            "leadId"
        )
        .agg(
            spark_min("first_event_ts_ist").alias("first_event_ts_ist"),
            to_date(spark_min("first_event_ts_ist")).alias("event_date"),

            spark_max(
                when(col("canonical_event") == "read", 1).otherwise(0)
            ).alias("has_whatsapp_read"),

            spark_max(
                when(col("canonical_event") == "click", 1).otherwise(0)
            ).alias("has_whatsapp_click"),

            spark_max(
                when(col("canonical_event") == "delivered", 1).otherwise(0)
            ).alias("has_whatsapp_delivered"),

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
            "orgId",
            "event_date"
        )
        .agg(
            countDistinct("leadId").alias("total_unique_leads"),

            countDistinct(
                when(col("has_whatsapp_read") == 1, col("leadId"))
            ).alias("whatsapp_read_unique"),

            countDistinct(
                when(col("has_whatsapp_click") == 1, col("leadId"))
            ).alias("whatsapp_click_unique"),

            countDistinct(
                when(
                    (col("has_whatsapp_read") == 1) &
                    (col("has_whatsapp_click") == 1),
                    col("leadId")
                )
            ).alias("whatsapp_both_unique"),

            countDistinct(
                when(
                    (col("has_whatsapp_read") == 1) &
                    (col("has_whatsapp_click") == 0),
                    col("leadId")
                )
            ).alias("whatsapp_read_only_unique"),

            countDistinct(
                when(
                    (col("has_whatsapp_click") == 1) &
                    (col("has_whatsapp_read") == 0),
                    col("leadId")
                )
            ).alias("whatsapp_click_only_unique"),

            countDistinct(
                when(col("has_whatsapp_delivered") == 1, col("leadId"))
            ).alias("whatsapp_delivered_unique"),

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

    logger.info("✅ campaign_engagement_whatsapp table created successfully")
    spark.stop()


if __name__ == "__main__":
    main()
