

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, substring, lit, when, trim, split

# Initialize Spark session
spark = SparkSession.builder.appName("MappingSheetExample").getOrCreate()

# Load the DataFrames (assuming they are already loaded as DataFrames)
# e_rolb_db_psdm_rolb_audit_log_main, e_customer_db_tdicuml, fdp_uk_ref_data_db_chl_map, rolb_evnt_dil, fdp_uk_ref_data_db_rolb_evnt_map, fdp_uk_ref_data_db_rolb_err_map

# Example DataFrames (replace with actual data loading code)
e_rolb_db_psdm_rolb_audit_log_main = spark.read.csv("path_to_psdm_rolb_audit_log_main.csv", header=True)
e_customer_db_tdicuml = spark.read.csv("path_to_tdicuml.csv", header=True)
fdp_uk_ref_data_db_chl_map = spark.read.csv("path_to_chl_map.csv", header=True)
rolb_evnt_dil = spark.read.csv("path_to_rolb_evnt_dil.csv", header=True)
fdp_uk_ref_data_db_rolb_evnt_map = spark.read.csv("path_to_rolb_evnt_map.csv", header=True)
fdp_uk_ref_data_db_rolb_err_map = spark.read.csv("path_to_rolb_err_map.csv", header=True)

# Perform the joins and transformations
# Join e_rolb_db.psdm_rolb_audit_log_main and e_customer_db.tdicuml
joined_df = e_rolb_db_psdm_rolb_audit_log_main.join(
    e_customer_db_tdicuml,
    e_rolb_db_psdm_rolb_audit_log_main.mmb_num == e_customer_db_tdicuml.mmb_lnk_mmb_num,
    how='left'
).select(
    e_rolb_db_psdm_rolb_audit_log_main["*"],
    e_customer_db_tdicuml.cus_idr
)

# Apply business logic for cus_idr
joined_df = joined_df.withColumn(
    "cus_idr",
    when(col("cus_idr").isNull(), lit(-99)).otherwise(col("cus_idr"))
)

# Join fdp_uk_ref_data_db.chl_map and rolb_evnt_dil
joined_df = joined_df.join(
    fdp_uk_ref_data_db_chl_map,
    fdp_uk_ref_data_db_chl_map.chl_cd == rolb_evnt_dil.chnl_ed,
    how='left'
).select(
    joined_df["*"],
    fdp_uk_ref_data_db_chl_map.chl_key
)

# Join fdp_uk_ref_data_db.rolb_evnt_map and rolb_evnt_dil
joined_df = joined_df.join(
    fdp_uk_ref_data_db_rolb_evnt_map,
    fdp_uk_ref_data_db_rolb_evnt_map.evnt_cd == rolb_evnt_dil.evnt_cd,
    how='left'
).select(
    joined_df["*"],
    fdp_uk_ref_data_db_rolb_evnt_map.evnt_key
)

# Join fdp_uk_ref_data_db.rolb_err_map and rolb_evnt_dil
joined_df = joined_df.join(
    fdp_uk_ref_data_db_rolb_err_map,
    fdp_uk_ref_data_db_rolb_err_map.err_cd == rolb_evnt_dil.err_cd,
    how='left'
).select(
    joined_df["*"],
    fdp_uk_ref_data_db_rolb_err_map.err_key
)

# Apply business logic for err_msg_txt
joined_df = joined_df.withColumn(
    "err_msg_txt",
    when(trim(col("err_msg_txt")).isin("none", "null"), lit("Success")).otherwise(lit("Failure"))
)
