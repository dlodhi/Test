from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("SQL to PySpark").getOrCreate()

# Load the DataFrames
pralm_df = spark.table("e_rolb_db.psdm_rolb_audit_log_main")
tdlcuml_df = spark.table("e_customer_db.tdlcuml")

# Subquery to get distinct cus_idr and mmb_lnk_mmb_num
window_spec = Window.partitionBy("mmb_Ink_mmb_num").orderBy("mmb_Ink_mmb_num")
tdlcuml_filtered_df = tdlcuml_df.filter((col("mmb_Ink_sts") == 'AC') & (col("active") == '1'))
tdlcuml_last_value_df = tdlcuml_filtered_df.withColumn("cus_idr", last("cus_idr").over(window_spec))
tdlcuml_distinct_df = tdlcuml_last_value_df.select("cus_idr", "mmb_Ink_mmb_num").distinct()

# Join the DataFrames
joined_df = pralm_df.join(tdlcuml_distinct_df, pralm_df["mmb_num"] == tdlcuml_distinct_df["mmb_Ink_mmb_num"].cast("string"), "left")

# Apply the case statement and cast
result_df = joined_df.withColumn("cust_id", when(col("mmb_Ink_mmb_num").isNull(), -99).otherwise(col("cus_idr")).cast("BIGINT"))

# Filter by the maximum log_dte
max_log_dte = pralm_df.agg(spark_max("log_dte")).collect()[0][0]
final_df = result_df.filter(col("log_dte") == max_log_dte)

# Select the final columns
final_df = final_df.select("cust_id")

# Show the result
final_df.show()
