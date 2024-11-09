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
