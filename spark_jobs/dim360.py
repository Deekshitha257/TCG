
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, substring, length,
    lower, lit, get_json_object
)
from pyspark.sql.types import StringType

NESSIE_URI = "http://nessie:19120/api/v2"
NESSIE_REF = "main"
SILVER_WAREHOUSE = "s3a://promotionengine-search/TCG_SILVER"

PROJECT_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/sso/"
    "project_fddaa81e-594a-4c2a-9c59-b953445ec5a8/data/"
)

ENTITY_LABEL_PATH = (
    "s3a://promotionengine-search/TCG_BRONZE/sso/"
    "entity_label_6f690a9f-1580-45fb-b70e-ae9d961ef7f1/data/"
)


spark = (
    SparkSession.builder
    .appName("uat_dim_lead_360_raw_to_silver")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
    .config("spark.sql.catalog.nessie.ref", NESSIE_REF)
    .config("spark.sql.catalog.nessie.warehouse", SILVER_WAREHOUSE)
    .config("spark.sql.session.timeZone", "Asia/Kolkata")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ylf = spark.read.table("nessie.crm.lead_flat").alias("ylf")
ylcle = spark.read.table(
    "nessie.crm.yukio_uat_lead_communication_logs_exploded"
).alias("ylcle")

p = spark.read.parquet(PROJECT_PATH).alias("p")
entity = spark.read.parquet(ENTITY_LABEL_PATH)

e = entity.alias("e")
e2 = entity.alias("e2")
e3 = entity.alias("e3")
prev_label = entity.alias("prev_label")


df = (
    ylf
    .join(
        ylcle,
        (ylf["_id"] == ylcle["leadId"]) &
        (ylf["orgId"] == ylcle["orgId"]) &
        (ylf["projectId"] == ylcle["projectId"]),
        "left"
    )
    .join(
        p,
        (ylf["projectId"] == p["_id"]) &
        (ylf["orgId"] == p["orgId"]),
        "left"
    )
    .join(
        e,
        (ylf["leadStatus"] == e["_id"]) &
        (ylf["orgId"] == e["orgId"]),
        "left"
    )
    .join(
        e2,
        (ylf["profile_subSourceOfLead"] == e2["_id"]) &
        (ylf["orgId"] == e2["orgId"]),
        "left"
    )
    .join(
        e3,
        (ylf["profile_sourceOfLead"] == e3["_id"]) &
        (ylf["orgId"] == e3["orgId"]),
        "left"
    )
    .join(
        prev_label,
        (ylcle["prevStatus"] == prev_label["_id"]) &
        (ylcle["orgId"] == prev_label["orgId"]),
        "left"
    )
)


clean_phone = regexp_replace(col("ylf.profile_whatsAppNumber"), "[^0-9]", "")

normalized_whatsapp_col = (
    when(col("ylf.profile_whatsAppNumber").isNull(), lit(None).cast(StringType()))
    .when(
        (clean_phone.startswith("91")) &
        (length(col("ylf.profile_whatsAppNumber")) > 10),
        substring(clean_phone, 3, 20)
    )
    .when(col("ylf.profile_whatsAppNumber").startswith("+91"),
          substring(col("ylf.profile_whatsAppNumber"), 4, 20))
    .when(
        (col("ylf.profile_whatsAppNumber").startswith("91")) &
        (length(col("ylf.profile_whatsAppNumber")) > 10),
        substring(col("ylf.profile_whatsAppNumber"), 3, 20)
    )
    .when(col("ylf.profile_whatsAppNumber").startswith("+"),
          substring(col("ylf.profile_whatsAppNumber"), 2, 20))
    .otherwise(col("ylf.profile_whatsAppNumber"))
)

site_visit_scheduled_col = (
    when(
        (col("ylf.original_siteVisit_json").isNotNull()) &
        (regexp_replace(col("ylf.original_siteVisit_json"), "\\s+", "") != "") &
        (col("ylf.original_siteVisit_json") != "None"),
        True
    )
    .when(
        (col("ylcle.nextSiteVisit").isNotNull()) &
        (regexp_replace(col("ylcle.nextSiteVisit"), "\\s+", "") != "") &
        (col("ylcle.nextSiteVisit") != "None"),
        True
    )
    .when(col("ylcle.siteVisit_date").isNotNull(), True)
    .when(col("ylcle.siteVisit_schedule").isNotNull(), True)
    .when(col("ylcle.siteVisit_time").isNotNull(), True)
    .when(col("ylcle.siteVisitDate").isNotNull(), True)
    .when(col("ylcle.siteVisitTime").isNotNull(), True)
    .otherwise(False)
)

final_df = df.select(
    # ---- LEAD FLAT
    col("ylf._id").alias("lead_flat_id"),
    col("ylf.orgId"),
    col("ylf.leadId"),
    col("ylf.isActive"),
    col("ylf.createdAt"),
    col("ylf.createdBy"),
    col("ylf.creatorId"),
    col("ylf.projectId"),
    col("ylf.original_siteVisit_json").alias("siteVisit"),
    col("ylf.updatedAt"),
    col("ylf.updatedBy"),
    col("ylf.assigneeId"),
    col("ylf.background"),
    col("ylf.leadStatus"),
    col("ylf.followUpDate"),
    col("ylf.followUpTime"),
    col("ylf.nextFollowUp"),
    col("ylf.callHyperLink"),
    col("ylf.chatHyperLink"),
    col("ylf.latestComment"),
    col("ylf.nextSiteVisit"),
    col("ylf.stageChangedAt"),
    col("ylf.totalUnreadMsgs"),
    col("ylf.propertyInterest"),
    col("ylf.totalMissedCalls"),
    col("ylf.assignmentHistory"),
    col("ylf.dispositionCallSid"),
    col("ylf.leadCreationSource"),
    col("ylf.siteVisitDoneCount"),
    col("ylf.conversionPropensity"),
    col("ylf.leadStatusChangeReason"),
    lit(None).cast(StringType()).alias("pms_LeadId"),
    col("ylf.profile_whatsAppNumber"),
    normalized_whatsapp_col.alias("normalized_whatsapp_number"),

    col("ylf.profile_sourceOfLead"),
    col("ylf.profile_subSourceOfLead"),
    col("ylf.profile_fullName"),
    col("ylf.profile_age"),
    col("ylf.profile_gender"),
    col("ylf.profile_currentLocation"),
    col("ylf.profile_alternateNumber"),
    col("ylf.profile_email"),
    col("ylf.profile_referralSource"),
    col("ylf.profile_agentName"),

    # ---- PROJECT
    col("p.projectName"),
    col("p.projectStatus"),
    col("p.projectLogo"),
    col("p.completionDate"),
    col("p.projectPublishStatus"),

    # ---- COMM LOG (FULL)
    col("ylcle._id").alias("communication_log_id"),
    col("ylcle.leadId").alias("leadId_comm"),
    col("ylcle.orgId").alias("orgId_comm"),
    col("ylcle.projectId").alias("projectId_comm"),
    col("ylcle.isActive").alias("isActive_comm"),
    col("ylcle.createdAt").alias("createdAt_comm"),
    col("ylcle.createdBy").alias("createdBy_comm"),
    col("ylcle.updatedAt").alias("updatedAt_comm"),
    col("ylcle.updatedBy").alias("updatedBy_comm"),
    col("ylcle.assigneeId").alias("assigneeId_comm"),
    col("ylcle.leadStatus").alias("leadStatus_comm"),
    col("ylcle.prevStatus").alias("prevStatus_comm"),
    col("ylcle.leadStatusChangeReason").alias("leadStatusChangeReason_comm"),
    col("ylcle.latestComment").alias("latestComment_comm"),
    col("ylcle.nextFollowUp").alias("nextFollowUp_comm"),
    col("ylcle.nextSiteVisit").alias("nextSiteVisit_comm"),
    col("ylcle.stageChangedAt").alias("stageChangedAt_comm"),
    col("ylcle.totalMissedCalls").alias("totalMissedCalls_comm"),
    col("ylcle.totalUnreadMsgs").alias("totalUnreadMsgs_comm"),

    col("ylcle.logEventType"),
    col("ylcle.prevAssigneeId"),
    col("ylcle.payload__v"),
    col("ylcle.payload_id"),
    col("ylcle.agentId"),
    col("ylcle.payload_assigneeId"),
    col("ylcle.payload_assignmentHistory"),

    # ---- BACKGROUND / CONVERSION / PRICING
    col("ylcle.background_annualIncome"),
    col("ylcle.background_assignedTo"),
    col("ylcle.background_currentLocation"),
    col("ylcle.background_fundingIsLoaned"),
    col("ylcle.background_occupation"),
    col("ylcle.background_possessionDate_month"),
    col("ylcle.background_possessionDate_year"),
    col("ylcle.background_qualification"),
    col("ylcle.background_workLocation"),
    col("ylcle.bookingId"),
    col("ylcle.buildingId"),
    col("ylcle.buildingName"),
    col("ylcle.cancelledReason"),
    col("ylcle.coOwnerDetails"),
    col("ylcle.conversion_description"),
    col("ylcle.conversion_interactionNumber"),
    col("ylcle.conversion_probability"),
    col("ylcle.conversion_breakdown_ability"),
    col("ylcle.conversion_breakdown_intent"),
    col("ylcle.conversion_breakdown_perception"),
    col("ylcle.conversion_breakdown_readiness"),
    col("ylcle.pricing_agreementValue"),
    col("ylcle.pricing_modeOfPayment"),
    col("ylcle.pricing_tokenAmount"),

    # ---- SITE VISIT / CALL / EXTRA
    col("ylcle.profile_fullName").alias("profile_fullName_comm"),
    col("ylcle.profile_email").alias("profile_email_comm"),
    col("ylcle.profile_gender").alias("profile_gender_comm"),
    col("ylcle.profile_age").alias("profile_age_comm"),
    col("ylcle.profile_alternateNumber").alias("profile_alternateNumber_comm"),
    col("ylcle.profile_countryCode"),
    col("ylcle.profile_referralSource").alias("profile_referralSource_comm"),
    col("ylcle.profile_sourceOfLead").alias("profile_sourceOfLead_comm"),
    col("ylcle.profile_subSourceOfLead").alias("profile_subSourceOfLead_comm"),

    col("ylcle.propertyInterest_purposeOfProperty"),
    col("ylcle.propertyInterest_budgetRange"),
    col("ylcle.propertyInterest_carpetAreaSqFtRange"),
    col("ylcle.propertyInterest_otherPreferences"),
    col("ylcle.propertyInterest_preferredFloor"),
    col("ylcle.propertyInterest_preferredUnitType"),
    col("ylcle.propertyInterest_propertyStatus"),

    col("ylcle.siteVisit_date"),
    col("ylcle.siteVisit_modeOfVisit"),
    col("ylcle.siteVisit_schedule"),
    col("ylcle.siteVisit_time"),
    col("ylcle.siteVisitDate"),
    col("ylcle.siteVisitDoneCount").alias("siteVisitDoneCount_comm"),
    col("ylcle.siteVisitTime"),
    col("ylcle.siteVisitstartDateTime").alias("siteVisitstartDateTime_comm"),
    col("ylcle.siteVisitendDateTime").alias("siteVisitendDateTime_comm"),

    col("ylcle.remarks"),
    col("ylcle.payload_status"),
    col("ylcle.payload_subStatus"),
    col("ylcle.viewed"),
    col("ylcle.callDuration"),
    col("ylcle.agentName"),
    col("ylcle.visitDate"),
    col("ylcle.visitOutcome"),
    col("ylcle.qualificationReason"),
    col("ylcle.qualifiedBy"),
    col("ylcle.agentPhone"),
    col("ylcle.callStatus"),
    col("ylcle.comment"),
    col("ylcle.meetingNotes"),
    col("ylcle.followUpRequired"),

    col("ylcle.call_direction").alias("call_direction_comm"),
    col("ylcle.call_from").alias("call_from_comm"),
    col("ylcle.dial_whom_number").alias("dial_whom_number_comm"),
    col("ylcle.call_to").alias("call_to_comm"),
    col("ylcle.dial_call_status").alias("dial_call_status_comm"),
    col("ylcle.call_type").alias("call_type_comm"),
    col("ylcle.call_sid").alias("call_sid_comm"),
    col("ylcle.call_legs").alias("call_legs_comm"),
    col("ylcle.recording_url").alias("recording_url_comm"),
    col("ylcle.call_start_time").alias("call_start_time_comm"),
    col("ylcle.call_end_time").alias("call_end_time_comm"),
    col("ylcle.call_event_status").alias("call_event_status_comm"),
    col("ylcle.event_type").alias("event_type_comm"),
    col("ylcle.call_ended").alias("call_ended_comm"),

    site_visit_scheduled_col.alias("site_visit_scheduled"),

    col("e.entityName").alias("sirrus_lead_entity_name"),
    col("e.labelName").alias("sirrus_lead_status"),
    col("prev_label.labelName").alias("previous_lead_status"),
    col("e2.labelName").alias("sirrus_sub_source_of_lead"),
    col("e3.labelName").alias("sirrus_source_of_lead"),

    when(
        (col("ylcle.bookingId").isNotNull()) &
        (lower(col("e.labelName")) == "booked"), 1
    ).otherwise(0).alias("booked_flag"),

    when(col("ylcle.payload_subStatus").isin("SV_DONE"), True)
    .otherwise(False).alias("Site_Visit_Completed"),

    when(
        (col("ylcle.bookingId").isNotNull()) &
        (lower(col("e.labelName")) == "booked"),
        col("ylcle.createdAt")
    ).otherwise(lit(None).cast(StringType())).alias("booked_date"),

    get_json_object(col("ylf.conversionPropensity"), "$.leadIntent")
    .alias("lead_temperature")
)


spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sirrus_ai_silver")

final_df.writeTo(
    "nessie.sirrus_ai_silver.uat_dim_lead_360"
).createOrReplace()
