from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load the DataFrames
pralm_df = spark.read.table("e_rolb_db.psdm_rolb_audit_log_main")
tdlcuml_df = spark.read.table("e_customer_db.tdlcuml")

# Subquery for `tdl` DataFrame
window_spec = Window.partitionBy("mmb_Ink_mmb_num").orderBy(F.col("log_dte").desc())  # Partition by 'mmb_Ink_mmb_num' to apply the window function
tdl_df = tdlcuml_df.filter((F.col("mmb_Ink_sts") == "AC") & (F.col("active") == "1")) \
    .withColumn("cus_idr", F.last("cus_idr", ignorenulls=True).over(window_spec)) \
    .select("cus_idr", "mmb_Ink_mmb_num").distinct()

# Join operation
joined_df = pralm_df.join(tdl_df, pralm_df.mmb_num == tdl_df.mmb_Ink_mmb_num, how="left")

# Main query logic
final_df = joined_df.withColumn(
    "cust_id", 
    F.when(F.col("cus_idr").isNull(), -99).otherwise(F.col("cus_idr")).cast("bigint")
).filter(
    pralm_df.log_dte == pralm_df.select(F.max("log_dte")).collect()[0][0]
)

# Show the result
final_df.select("cust_id").show()



# Perform the first join and fetch chl_key
join1 = fop_uk_ref_data_b_chn_map.join(rolb_evnt_dil, fop_uk_ref_data_b_chn_map.chl_cd == rolb_evnt_dil.chnl_ed) \
                                 .select(fop_uk_ref_data_b_chn_map.chl_key, rolb_evnt_dil.evnt_od, rolb_evnt_dil.err_cd)

# Perform the second join and fetch evnt_key
join2 = join1.join(fop_uk_ref_data_b_rob_evnt_map, join1.evnt_od == fop_uk_ref_data_b_rob_evnt_map.evnt_ed) \
             .select(join1.chl_key, fop_uk_ref_data_b_rob_evnt_map.evnt_key, join1.err_cd)

# Perform the third join and fetch err_key
final_df = join2.join(fap_uk_ref_data_db_rob_err_map, join2.err_cd == fap_uk_ref_data_db_rob_err_map.err_cd) \
                .select(join2.chl_key, join2.evnt_key, fap_uk_ref_data_db_rob_err_map.err_key)

# Show the final DataFrame
final_df.show()
