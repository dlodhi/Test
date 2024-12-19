result_df = joined_df.withColumn(
    "cust_mbr_id",
    coalesce(
        trim(col("psdm_rolb_audit_details.mmb_num")),
        trim(col("psdm_rolb_audit_logg_main.mmb_num")),
        col("psdm_rolb_audit_logg_main.mmb_num").cast("string")
    )
).withColumn(
    "cust_mbr_id",
    col("cust_mbr_id").when(
        (trim(col("psdm_rolb_audit_logg_main.mmb_num")) != '999999999999') &
        (col("psdm_rolb_audit_details.mmb_num").isNotNull()) &
        (trim(col("psdm_rolb_audit_details.mmb_num")) != ''),
        trim(col("psdm_rolb_audit_details.mmb_num"))
    ).otherwise(
        coalesce(trim(col("psdm_rolb_audit_logg_main.mmb_num")), col("psdm_rolb_audit_logg_main.mmb_num").cast("string"))
    )
)


result_df = joined_df.withColumn(
    "cust_mbr_id",
    when(
        (trim(col("psdm_rolb_audit_logg_main.mmb_num")) != "999999999999") &
        (col("psdm_rolb_audit_details.mmb_num").isNotNull()) &
        (trim(col("psdm_rolb_audit_details.mmb_num")) != ""),
        trim(col("psdm_rolb_audit_details.mmb_num"))
    ).otherwise(
        coalesce(
            trim(col("psdm_rolb_audit_logg_main.mmb_num")),
            lit("-99")
        )
    )
)
