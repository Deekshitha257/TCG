import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, lit


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("silver_lead_flat")

BRONZE_S3_PATH = (
    "s3a://promotionengine-search/"
    "TCG_BRONZE/sso/"
    "lead_faf9e2a3-cc0d-418f-9f7c-bd9fb209924a/data/"
)

SILVER_WAREHOUSE = "s3a://promotionengine-search/TCG_SILVER"
NESSIE_URI = "http://nessie:19120/api/v2"

TARGET_TABLE = "nessie.crm.lead_flat"


def create_spark():
    return (
        SparkSession.builder
        .appName("SILVER_LEAD_FLAT")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", SILVER_WAREHOUSE)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .getOrCreate()
    )


def profile_schema():
    return StructType([
        StructField("fullName", StringType()),
        StructField("email", StringType()),
        StructField("age", StringType()),
        StructField("gender", StringType()),
        StructField("currentLocation", StringType()),
        StructField("alternateNumber", StringType()),
        StructField("whatsAppNumber", StringType()),
        StructField("sourceOfLead", StringType()),
        StructField("subSourceOfLead", StringType()),
        StructField("referralSource", StringType()),
        StructField("agentName", StringType()),
        StructField("additionalInfo", StringType()),
    ])

def property_interest_schema():
    return StructType([
        StructField("budgetRange", StringType()),
        StructField("carpetAreaSqFtRange", StringType()),
        StructField("preferredFloor", StringType()),
        StructField("preferredUnitType", StringType()),
        StructField("purposeOfProperty", StringType()),
        StructField("propertyStatus", StringType()),
        StructField("otherPreferences", StringType()),
    ])

def conversion_propensity_schema():
    return StructType([
        StructField("description", StringType()),
        StructField("probability", StringType()),
        StructField("interactionNumber", StringType()),
        StructField("leadIntent", StringType()),
    ])

def site_visit_schema():
    return StructType([
        StructField("date", StringType()),
        StructField("time", StringType()),
        StructField("schedule", StringType()),
        StructField("modeOfVisit", StringType()),
    ])


def safe_col_struct(df, col_name, schema_fn):
    return (
        from_json(col(col_name), schema_fn())
        if col_name in df.columns
        else from_json(lit(None), schema_fn())
    )

def safe_col_scalar(df, col_name):
    return (
        col(col_name).cast(StringType())
        if col_name in df.columns
        else lit(None).cast(StringType())
    )


def main():
    logger.info("🚀 Starting SILVER Lead Flatten Job (S3 → Nessie)")
    spark = create_spark()

    df = spark.read.parquet(BRONZE_S3_PATH)

    df_parsed = (
        df
        .withColumn("profile_struct", safe_col_struct(df, "profile", profile_schema))
        .withColumn("prop_struct", safe_col_struct(df, "propertyInterest", property_interest_schema))
        .withColumn("conv_struct", safe_col_struct(df, "conversionPropensity", conversion_propensity_schema))
        .withColumn("sv_struct", safe_col_struct(df, "siteVisit", site_visit_schema))
    )

    final_df = df_parsed.select(
        # Root
        safe_col_scalar(df, "_id").alias("_id"),
        safe_col_scalar(df, "orgId").alias("orgId"),
        safe_col_scalar(df, "leadId").alias("leadId"),
        safe_col_scalar(df, "isActive").alias("isActive"),
        safe_col_scalar(df, "createdAt").alias("createdAt"),
        safe_col_scalar(df, "createdBy").alias("createdBy"),
        safe_col_scalar(df, "creatorId").alias("creatorId"),
        safe_col_scalar(df, "projectId").alias("projectId"),
        safe_col_scalar(df, "updatedAt").alias("updatedAt"),
        safe_col_scalar(df, "updatedBy").alias("updatedBy"),
        safe_col_scalar(df, "assigneeId").alias("assigneeId"),
        safe_col_scalar(df, "leadStatus").alias("leadStatus"),
        safe_col_scalar(df, "leadStatusChangeReason").alias("leadStatusChangeReason"),
        safe_col_scalar(df, "background").alias("background"),
        safe_col_scalar(df, "followUpDate").alias("followUpDate"),
        safe_col_scalar(df, "followUpTime").alias("followUpTime"),
        safe_col_scalar(df, "nextFollowUp").alias("nextFollowUp"),
        safe_col_scalar(df, "callHyperLink").alias("callHyperLink"),
        safe_col_scalar(df, "chatHyperLink").alias("chatHyperLink"),
        safe_col_scalar(df, "latestComment").alias("latestComment"),
        safe_col_scalar(df, "nextSiteVisit").alias("nextSiteVisit"),
        safe_col_scalar(df, "stageChangedAt").alias("stageChangedAt"),
        safe_col_scalar(df, "totalUnreadMsgs").alias("totalUnreadMsgs"),
        safe_col_scalar(df, "totalMissedCalls").alias("totalMissedCalls"),
        safe_col_scalar(df, "assignmentHistory").alias("assignmentHistory"),
        safe_col_scalar(df, "dispositionCallSid").alias("dispositionCallSid"),
        safe_col_scalar(df, "leadCreationSource").alias("leadCreationSource"),
        safe_col_scalar(df, "siteVisitDoneCount").alias("siteVisitDoneCount"),

        # Original JSON
        safe_col_scalar(df, "siteVisit").alias("original_siteVisit_json"),
        safe_col_scalar(df, "propertyInterest").alias("propertyInterest"),
        safe_col_scalar(df, "conversionPropensity").alias("conversionPropensity"),
        safe_col_scalar(df, "profile").alias("profile"),

        # Profile
        col("profile_struct.fullName").alias("profile_fullName"),
        col("profile_struct.email").alias("profile_email"),
        col("profile_struct.age").alias("profile_age"),
        col("profile_struct.gender").alias("profile_gender"),
        col("profile_struct.currentLocation").alias("profile_currentLocation"),
        col("profile_struct.alternateNumber").alias("profile_alternateNumber"),
        col("profile_struct.whatsAppNumber").alias("profile_whatsAppNumber"),
        col("profile_struct.sourceOfLead").alias("profile_sourceOfLead"),
        col("profile_struct.subSourceOfLead").alias("profile_subSourceOfLead"),
        col("profile_struct.referralSource").alias("profile_referralSource"),
        col("profile_struct.agentName").alias("profile_agentName"),

        # Property Interest
        col("prop_struct.budgetRange").alias("propertyInterest_budgetRange"),
        col("prop_struct.carpetAreaSqFtRange").alias("propertyInterest_carpetAreaSqFtRange"),
        col("prop_struct.preferredFloor").alias("propertyInterest_preferredFloor"),
        col("prop_struct.preferredUnitType").alias("propertyInterest_preferredUnitType"),
        col("prop_struct.purposeOfProperty").alias("propertyInterest_purposeOfProperty"),
        col("prop_struct.propertyStatus").alias("propertyInterest_propertyStatus"),
        col("prop_struct.otherPreferences").alias("propertyInterest_otherPreferences"),

        # Conversion
        col("conv_struct.probability").alias("conversionPropensity_probability"),
        col("conv_struct.leadIntent").alias("conversionPropensity_leadIntent"),

        # Site Visit
        col("sv_struct.date").alias("siteVisit_date"),
        col("sv_struct.time").alias("siteVisit_time"),
        col("sv_struct.schedule").alias("siteVisit_schedule"),
        col("sv_struct.modeOfVisit").alias("siteVisit_modeOfVisit"),
    )

    # 4️⃣ WRITE
    final_df.writeTo(TARGET_TABLE).createOrReplace()
    logger.info("✅ SILVER Lead Flat Job Completed")
    spark.stop()

if __name__ == "__main__":
    main()
