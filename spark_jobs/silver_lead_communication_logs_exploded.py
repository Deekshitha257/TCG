

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, lit, coalesce

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("silver_lead_comm_exploded")


BRONZE_S3_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/sso/"
    "lead_communication_logs_4bab84bc-c358-403e-8f7b-7585d558cae0/data/"
)

SILVER_WAREHOUSE = "s3a://promotionengine-search/TCG_SILVER"
NESSIE_URI = "http://nessie:19120/api/v2"
TARGET_TABLE = "silver.crm.yukio_uat_lead_communication_logs_exploded"


def create_spark():
    return (
        SparkSession.builder
        .appName("SILVER_LEAD_COMMUNICATION_LOGS_EXPLODED")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
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


def payload_schema():
    return StructType([
        StructField("__v", StringType()),
        StructField("_id", StringType()),

        StructField("isActive", StringType()),
        StructField("createdAt", StringType()),
        StructField("createdBy", StringType()),
        StructField("updatedAt", StringType()),
        StructField("updatedBy", StringType()),
        StructField("projectId", StringType()),
        StructField("assigneeId", StringType()),
        StructField("prevAssigneeId", StringType()),
        StructField("nextSiteVisit", StringType(), True),
        StructField("leadStatus", StringType()),
        StructField("prevStatus", StringType()),
        StructField("leadStatusChangeReason", StringType()),
        StructField("logEventType", StringType()),
        StructField("status", StringType()),
        StructField("subStatus", StringType()),
        StructField("viewed", BooleanType()),
        StructField("remarks", StringType()),
        StructField("nextFollowUp", StringType()),

        StructField("assignmentHistory", ArrayType(StringType())),

        StructField("agentId", StringType()),
        StructField("agentName", StringType()),
        StructField("agentPhone", StringType()),

        StructField("chatHyperLink", StringType()),
        StructField("latestComment", StringType()),

        StructField("background", StructType([
            StructField("annualIncome", StringType()),
            StructField("assignedTo", StringType()),
            StructField("currentLocation", StringType()),
            StructField("fundingIsLoaned", StringType()),
            StructField("occupation", StringType()),
            StructField("possessionDate", StructType([
                StructField("month", StringType()),
                StructField("year", StringType())
            ])),
            StructField("qualification", StringType()),
            StructField("workLocation", StringType())
        ])),

        StructField("bookingId", StringType()),
        StructField("buildingId", StringType()),
        StructField("buildingName", StringType()),
        StructField("cancelledReason", StringType()),
        StructField("coOwnerDetails", StringType()),

      StructField("conversionPropensity", StructType([
            StructField("breakdown", StructType([
                StructField("ability", StringType(), True),
                StructField("intent", StringType(), True),
                StructField("perception", StringType(), True),
                StructField("readiness", StringType(), True)
            ]), True),
            StructField("description", StringType(), True),
            StructField("interactionNumber", StringType(), True),
            StructField("probability", StringType(), True)
        ]), True),

        StructField("pricingDetails", StructType([
            StructField("agreementValue", StringType()),
            StructField("modeOfPayment", StringType()),
            StructField("tokenAmount", StringType())
        ])),

        StructField("profile", StructType([
            StructField("fullName", StringType()),
            StructField("email", StringType()),
            StructField("gender", StringType()),
            StructField("age", StringType()),
            StructField("alternateNumber", StringType()),
            StructField("countryCode", StringType()),
            StructField("referralSource", StringType()),
            StructField("sourceOfLead", StringType()),
            StructField("subSourceOfLead", StringType()),
            StructField("whatsappNumber", StringType())
        ])),

        StructField("propertyInterest", StructType([
            StructField("purposeOfProperty", StringType()),
            StructField("budgetRange", StringType()),
            StructField("carpetAreaSqFtRange", StringType()),
            StructField("otherPreferences", StringType()),
            StructField("preferredFloor", StringType()),
            StructField("preferredUnitType", StringType()),
            StructField("propertyStatus", StringType())
        ])),

        StructField("siteVisit", StructType([
            StructField("date", StringType()),
            StructField("time", StringType()),
            StructField("schedule", StringType()),
            StructField("modeOfVisit", StringType())
        ])),

        StructField("siteVisitDate", StringType()),
        StructField("siteVisitTime", StringType()),
        StructField("siteVisitDoneCount", StringType()),
        StructField("startDateTime", StringType()),
        StructField("endDateTime", StringType()),

        StructField("totalMissedCalls", StringType()),
        StructField("totalUnreadMsgs", StringType()),
        StructField("stageChangedAt", StringType()),
        StructField("unitId", StringType()),
        StructField("unitNumber", StringType()),
        StructField("unitType", StringType()),

        StructField("callDuration", StringType()),
        StructField("visitDate", StringType()),
        StructField("visitOutcome", StringType()),
        StructField("qualificationReason", StringType()),
        StructField("qualifiedBy", StringType()),
        StructField("callStatus", StringType()),
        StructField("comment", StringType()),
        StructField("meetingNotes", StringType()),
        StructField("followUpRequired", StringType()),

        StructField("Direction", StringType()),
        StructField("CallFrom", StringType()),
        StructField("DialWhomNumber", StringType()),
        StructField("CallTo", StringType()),
        StructField("DialCallStatus", StringType()),
        StructField("CallType", StringType()),
        StructField("CallSid", StringType()),
        StructField("Legs", ArrayType(
            StructType([
                StructField("onCallDuration", StringType()),
                StructField("status", StringType())
            ])
        )),
        StructField("RecordingUrl", StringType()),
        StructField("StartTime", StringType()),
        StructField("EndTime", StringType()),
        StructField("callEventStatus", StringType()),
        StructField("EventType", StringType()),
        StructField("callEnded", BooleanType())
    ])


def main():
    spark = create_spark()
    logger.info("🚀 Building SILVER lead_communication_logs_exploded")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver.crm")

    df = spark.read.parquet(BRONZE_S3_PATH)




    if "payload" not in df.columns:
        logger.warning("⚠️ Payload column NOT found in bronze data. Creating NULL payload struct.")
        df = df.withColumn(
            "payload",
            lit(None).cast(payload_schema())
        )
    else:
        logger.info("📦 Payload column found. Parsing payload JSON using defined schema.")
        df = df.withColumn(
            "payload",
            from_json(col("payload"), payload_schema())
        )
   

    final_df = df.select(
        col("_id"),
        col("orgId"),
        col("leadId"),
        
        
        col("payload.isActive").alias("isActive"),
        col("payload.createdAt").alias("createdAt"),
        col("payload.createdBy").alias("createdBy"),
        col("payload.projectId").alias("projectId"),
        col("payload.updatedAt").alias("updatedAt"),
        col("payload.updatedBy").alias("updatedBy"),
        col("payload.assigneeId").alias("assigneeId"),
        col("payload.leadStatus").alias("leadStatus"),
        col("payload.prevAssigneeId").alias("prevAssigneeId"),
        col("payload.leadStatusChangeReason").alias("leadStatusChangeReason"),
        col("payload.logEventType").alias("logEventType"),

        col("payload.prevStatus").alias("prevStatus"),

        col("payload.__v").alias("payload__v"),
        col("payload._id").alias("payload_id"),
        col("payload.agentId").alias("agentId"),
        col("payload.assigneeId").alias("payload_assigneeId"),
        col("payload.assignmentHistory").alias("payload_assignmentHistory"),

        col("payload.background.annualIncome").alias("background_annualIncome"),
        col("payload.background.assignedTo").alias("background_assignedTo"),
        col("payload.background.currentLocation").alias("background_currentLocation"),
        col("payload.background.fundingIsLoaned").alias("background_fundingIsLoaned"),
        col("payload.background.occupation").alias("background_occupation"),
        col("payload.background.possessionDate.month").alias("background_possessionDate_month"),
        col("payload.background.possessionDate.year").alias("background_possessionDate_year"),
        col("payload.background.qualification").alias("background_qualification"),
        col("payload.background.workLocation").alias("background_workLocation"),

        col("payload.bookingId").alias("bookingId"),
        col("payload.buildingId").alias("buildingId"),
        col("payload.buildingName").alias("buildingName"),
        col("payload.cancelledReason").alias("cancelledReason"),

        col("payload.chatHyperLink").alias("chatHyperLink"),
        col("payload.coOwnerDetails").alias("coOwnerDetails"),

        col("payload.conversionPropensity.description").alias("conversion_description"),
        col("payload.conversionPropensity.interactionNumber").alias("conversion_interactionNumber"),
        col("payload.conversionPropensity.probability").alias("conversion_probability"),
        col("payload.conversionPropensity.breakdown.ability").alias("conversion_breakdown_ability"),
        col("payload.conversionPropensity.breakdown.intent").alias("conversion_breakdown_intent"),
        col("payload.conversionPropensity.breakdown.perception").alias("conversion_breakdown_perception"),
        col("payload.conversionPropensity.breakdown.readiness").alias("conversion_breakdown_readiness"),

        col("payload.pricingDetails.agreementValue").alias("pricing_agreementValue"),
        col("payload.pricingDetails.modeOfPayment").alias("pricing_modeOfPayment"),
        col("payload.pricingDetails.tokenAmount").alias("pricing_tokenAmount"),

        col("payload.profile.fullName").alias("profile_fullName"),
        col("payload.profile.email").alias("profile_email"),
        col("payload.profile.gender").alias("profile_gender"),
        col("payload.profile.age").alias("profile_age"),
        col("payload.profile.alternateNumber").alias("profile_alternateNumber"),
        col("payload.profile.countryCode").alias("profile_countryCode"),
        col("payload.profile.referralSource").alias("profile_referralSource"),
        col("payload.profile.sourceOfLead").alias("profile_sourceOfLead"),
        col("payload.profile.subSourceOfLead").alias("profile_subSourceOfLead"),
        col("payload.profile.whatsappNumber").alias("whatsAppNumber"),

        col("payload.propertyInterest.purposeOfProperty").alias("propertyInterest_purposeOfProperty"),
        col("payload.propertyInterest.budgetRange").alias("propertyInterest_budgetRange"),
        col("payload.propertyInterest.carpetAreaSqFtRange").alias("propertyInterest_carpetAreaSqFtRange"),
        col("payload.propertyInterest.otherPreferences").alias("propertyInterest_otherPreferences"),
        col("payload.propertyInterest.preferredFloor").alias("propertyInterest_preferredFloor"),
        col("payload.propertyInterest.preferredUnitType").alias("propertyInterest_preferredUnitType"),
        col("payload.propertyInterest.propertyStatus").alias("propertyInterest_propertyStatus"),

        col("payload.siteVisit.date").alias("siteVisit_date"),
        col("payload.siteVisit.modeOfVisit").alias("siteVisit_modeOfVisit"),
        col("payload.siteVisit.schedule").alias("siteVisit_schedule"),
        col("payload.siteVisit.time").alias("siteVisit_time"),
        col("payload.siteVisitDate").alias("siteVisitDate"),
        col("payload.siteVisitDoneCount").alias("siteVisitDoneCount"),
        col("payload.siteVisitTime").alias("siteVisitTime"),
        
        col("payload.startDateTime").alias("siteVisitstartDateTime"),
        col("payload.endDateTime").alias("siteVisitendDateTime"),

        col("payload.latestComment").alias("latestComment"),
        col("payload.nextFollowUp").alias("nextFollowUp"),
        col("payload.nextSiteVisit").alias("nextSiteVisit"),
        col("payload.stageChangedAt").alias("stageChangedAt"),
        col("payload.totalMissedCalls").alias("totalMissedCalls"),
        col("payload.totalUnreadMsgs").alias("totalUnreadMsgs"),
        col("payload.unitId").alias("unitId"),
        col("payload.unitNumber").alias("unitNumber"),
        col("payload.unitType").alias("unitType"),
        col("payload.remarks").alias("remarks"),
        col("payload.status").alias("payload_status"),
        col("payload.subStatus").alias("payload_subStatus"),
        col("payload.viewed").alias("viewed"),

        col("payload.callDuration").alias("callDuration"),
        col("payload.agentName").alias("agentName"),
        col("payload.visitDate").alias("visitDate"),
        col("payload.visitOutcome").alias("visitOutcome"),
        col("payload.qualificationReason").alias("qualificationReason"),
        col("payload.qualifiedBy").alias("qualifiedBy"),
        col("payload.agentPhone").alias("agentPhone"),
        col("payload.callStatus").alias("callStatus"),
        col("payload.comment").alias("comment"),
        col("payload.meetingNotes").alias("meetingNotes"),
        col("payload.followUpRequired").alias("followUpRequired"),

        col("payload.Direction").alias("call_direction"),
        col("payload.CallFrom").alias("call_from"),
        col("payload.DialWhomNumber").alias("dial_whom_number"),
        col("payload.CallTo").alias("call_to"),
        col("payload.DialCallStatus").alias("dial_call_status"),
        col("payload.CallType").alias("call_type"),
        col("payload.CallSid").alias("call_sid"),
        col("payload.Legs").alias("call_legs"),
        col("payload.RecordingUrl").alias("recording_url"),
        col("payload.StartTime").alias("call_start_time"),
        col("payload.EndTime").alias("call_end_time"),
        col("payload.callEventStatus").alias("call_event_status"),
        col("payload.EventType").alias("event_type"),
        col("payload.callEnded").alias("call_ended")
    )

    final_df.writeTo(TARGET_TABLE).createOrReplace()
    logger.info("✅ lead_communication_logs_exploded READY for dim_lead_360")

    spark.stop()

if __name__ == "__main__":
    main()
