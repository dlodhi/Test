from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, rtrim, split, collect_list, max as spark_max

# Initialize Spark session
spark = SparkSession.builder.appName("MultiConfigTranspose").getOrCreate()

# Step 1 - Create multi config transpose file
# Read fdp_uk_ref_data_db.mlt_chnl_confg_map
mlt_chnl_confg_map = spark.read.table("fdp_uk_ref_data_db.mlt_chnl_confg_map")
chnl_map = spark.read.table("fdp_uk_ref_data_db.chnl_map")

# Get chnl_key
chnl_key = chnl_map.filter((col("active") == 1) & (col("chnl_cd") == 'OLB')).select("chnl_key").first()["chnl_key"]

# Filter and transform
mlt_chnl_confg_map_filtered = mlt_chnl_confg_map.filter((col("active") == 1) & (col("chnl_key") == chnl_key))
mlt_chnl_confg_map_transformed = mlt_chnl_confg_map_filtered.select(
    lower(rtrim(col("filter_coll"))).alias("filter_coll"),
    lower(rtrim(col("filter_co12"))).alias("filter_co12"),
    col("value_col"),
    lower(rtrim(col("filter_condn1"))).alias("filter_condn1"),
    lower(rtrim(col("filter_condn2"))).alias("filter_condn2"),
    lower(rtrim(col("new_trans_col"))).alias("new_col")
)

# Step 2 - Read fdp_uk_ref_data_db.cust_jrny_confg_map and discover unique tag
cust_jrny_confg_map = spark.read.table("fdp_uk_ref_data_db.cust_jrny_confg_map")

# Filter and transform
cust_jrny_confg_map_filtered = cust_jrny_confg_map.filter((col("active") == 1) & (col("chnl_key") == chnl_key))
cust_jrny_confg_map_transformed = cust_jrny_confg_map_filtered.select(
    col("evnt_cd"),
    split(lower(col("cmpx_jrny_tag")), ",").alias("cmpx_jrny_tag_list")
)

# Combine unique tags by grouping on evnt_cd
cust_jrny_confg_map_grouped = cust_jrny_confg_map_transformed.groupBy("evnt_cd").agg(
    collect_list("cmpx_jrny_tag_list").alias("cmpx_jrny_tag_vec")
)

# Step 3 - Read e_rolb_db.psdm_rolb_audit_details table
psdm_rolb_audit_details = spark.read.table("e_rolb_db.psdm_rolb_audit_details")

# Filter and transform
psdm_rolb_audit_details_filtered = psdm_rolb_audit_details.filter(
    (col("log_dte") == "$CURR_BUS_DT") &
    (col("tagvalue").isNotNull()) &
    (col("tagvalue") != "") &
    (lower(rtrim(col("tagvalue"))) != 'null')
)

psdm_rolb_audit_details_transformed = psdm_rolb_audit_details_filtered.withColumn(
    "tagname", 
    when(
        lower(rtrim(col("acn_cat_nme"))).isin(['/olb/bulkpay/grouppaymentstepfour.json', '/olb/fxpay/mrp/payments-json']),
        regexp_replace(rtrim(col("tagname")), "[_0-9]", "*")
    ).otherwise(col("tagname"))
).withColumn(
    "vol",
    when(
        (lower(rtrim(col("acn_cat_nme"))) == '/olb/bulkpay/grouppaymentstepfour.json') &
        (instr(rtrim(col("tagname")), "_") > 0) &
        (size(split(rtrim(col("tagname")), " ")) > 1),
        split(rtrim(col("tagname")), "_")[1]
    ).otherwise(1)
)

# Deduplicate
psdm_rolb_audit_details_dedup = psdm_rolb_audit_details_transformed.dropDuplicates(["uid", "log_dte", "tagname", "tagvalue"])

# Group and apply transformation using lookup 'multi config transpose' file
result = psdm_rolb_audit_details_dedup.groupBy("uid").agg(
    spark_max("vol").alias("vol"),
    *[spark_max(col).alias(col) for col in psdm_rolb_audit_details_dedup.columns if col not in ["uid", "vol"]]
)

# Join with multi config transpose file
result = result.join(mlt_chnl_confg_map_transformed, on="uid", how="left").withColumn(
    "new_col",
    when(
        (lower(col("filter_coll")) == col("filter_condn1")) &
        (lower(col("filter_co12")) == col("filter_condn2")),
        col("value_col")
    ).otherwise(None)
)

# Add tag vector
result = result.withColumn(
    "tag_vec",
    when(
        lower(col("tagname")).isin(cust_jrny_confg_map_grouped.select("cmpx_jrny_tag_vec").collect()[0]["cmpx_jrny_tag_vec"]),
        array(struct(col("tagname"), col("tagvalue")))
    ).otherwise(None)
)

# Show result
result.show()




-------------------------------------------------------------------------------------------------------------------------



from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, rtrim, split, collect_list, max as spark_max,
    when, regexp_replace, size, array, struct, instr
)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MultiConfigTranspose") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 1: Create multi config transpose file
def create_multi_config_transpose():
    # Read channel mapping and get OLB channel key
    chnl_map = spark.table("fdp_uk_ref_data_db.chnl_map")
    chnl_key = chnl_map.filter(
        (col("active") == 1) & 
        (col("chnl_cd") == 'OLB')
    ).select("chnl_key").first()["chnl_key"]

    # Read and transform config mapping
    mlt_chnl_confg = spark.table("fdp_uk_ref_data_db.mlt_chnl_confg_map")
    return mlt_chnl_confg.filter(
        (col("active") == 1) & 
        (col("chnl_key") == chnl_key)
    ).select(
        lower(rtrim(col("filter_coll"))).alias("filter_coll"),
        lower(rtrim(col("filter_co12"))).alias("filter_co12"),
        col("value_col"),
        lower(rtrim(col("filter_condn1"))).alias("filter_condn1"),
        lower(rtrim(col("filter_condn2"))).alias("filter_condn2"),
        lower(rtrim(col("new_trans_col"))).alias("new_col")
    )

# Step 2: Process journey config mapping
def process_journey_config(chnl_key):
    journey_config = spark.table("fdp_uk_ref_data_db.cust_jrny_confg_map")
    return journey_config.filter(
        (col("active") == 1) & 
        (col("chnl_key") == chnl_key)
    ).select(
        col("evnt_cd"),
        split(lower(col("cmpx_jrny_tag")), ",").alias("cmpx_jrny_tag")
    ).groupBy("evnt_cd").agg(
        collect_list("cmpx_jrny_tag").alias("cmpx_jrny_tag_vec")
    )

# Step 3: Process audit details
def process_audit_details(cmplx_jrny_tags, multi_config):
    audit_details = spark.table("e_rolb_db.psdm_rolb_audit_details")
    
    # Initial filtering
    filtered_df = audit_details.filter(
        (col("log_dte") == spark.conf.get("spark.sql.variables.CURR_BUS_DT")) &
        col("tagvalue").isNotNull() &
        (trim(col("tagvalue")) != "") &
        (lower(rtrim(col("tagvalue"))) != "null")
    )

    # Transform tagname and vol
    transformed_df = filtered_df.withColumn(
        "tagname",
        when(
            lower(rtrim(col("acn_cat_nme"))).isin(
                ['/olb/bulkpay/grouppaymentstepfour.json', 
                 '/olb/fxpay/mrp/payments-json']
            ),
            regexp_replace(rtrim(col("tagname")), "[_0-9]", "*")
        ).otherwise(col("tagname"))
    ).withColumn(
        "vol",
        when(
            (lower(rtrim(col("acn_cat_nme"))) == '/olb/bulkpay/grouppaymentstepfour.json') &
            (instr(rtrim(col("tagname")), "_") > 0) &
            (size(split(rtrim(col("tagname")), " ")) > 1),
            split(rtrim(col("tagname")), "_")[1]
        ).otherwise(lit(1))
    )

    # Deduplicate and group
    return transformed_df.dropDuplicates(
        ["uid", "log_dte", "tagname", "tagvalue"]
    ).groupBy("uid").agg(
        spark_max("vol").alias("vol"),
        spark_max("mmb_num").alias("mmb_num"),
        spark_max("ses_idr").alias("ses_idr"),
        spark_max("log_tme").alias("log_tme"),
        spark_max("acn_nme").alias("acn_nme"),
        spark_max("log_millisec").alias("log_millisec"),
        spark_max("acn_cat_nme").alias("acn_cat_nme"),
        spark_max("msg_typ").alias("msg_typ"),
        spark_max("log_dte").alias("log_dte"),
        when(
            (lower(col("filter_coll")) == col("filter_condn1")) &
            (lower(col("filter_co12")) == col("filter_cond2")),
            col("value_col")
        ).alias("new_col"),
        when(
            lower(col("tagname")).isin(cmplx_jrny_tags),
            array(struct(
                col("tagname").alias("tagname"),
                col("tagvalue").alias("tagvalue")
            ))
        ).alias("tag_vec")
    )

def main():
    # Execute pipeline
    multi_config = create_multi_config_transpose()
    journey_config = process_journey_config(chnl_key)
    cmplx_jrny_tags = journey_config.select("cmpx_jrny_tag_vec").collect()[0][0]
    
    final_df = process_audit_details(cmplx_jrny_tags, multi_config)
    final_df.write.mode("overwrite").saveAsTable("output_database.transformed_audit_details")

if __name__ == "__main__":
    main()

------------------------------------------------------------------------------------------------------------



from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, col, split, explode, collect_list, when, regexp_replace, length, lit, max

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

# Step 1: Read and transform multi-channel config map
def get_multi_channel_config():
    # Get channel key
    channel_df = spark.table("fdp_uk_ref_data_db.chnl_map") \
        .filter("active = 1 AND chnl_cd = 'OLB'") \
        .select("chl_key")
    chl_key = channel_df.first()["chl_key"]
    
    # Read and transform config map
    mlt_config_df = spark.table("fdp_uk_ref_data_db.mlt_chnl_confg_map") \
        .filter(f"active = 1 AND chl_key = {chl_key}") \
        .select(
            lower(trim(col("filter_col1"))).alias("filter_col1"),
            lower(trim(col("filter_col2"))).alias("filter_col2"),
            col("val_col").alias("value_col"),
            lower(trim(col("filter_condn1"))).alias("filter_condn1"),
            lower(trim(col("filter_condn2"))).alias("filter_condn2"),
            lower(trim(col("new_trans_col"))).alias("new_col")
        )
    return mlt_config_df

# Step 2: Process customer journey config map
def get_journey_tags():
    channel_df = spark.table("fdp_uk_ref_data_db.chnl_map") \
        .filter("active = 1 AND chnl_cd = 'OLB'") \
        .select("chl_key")
    chl_key = channel_df.first()["chl_key"]
    
    journey_df = spark.table("fdp_uk_ref_data_db.cust_jrny_confg_map") \
        .filter(f"active = 1 AND chl_key = {chl_key}") \
        .select(
            col("evnt_cd"),
            explode(split(lower(col("cmpx_jrny_tag")), ",")).alias("tag")
        ) \
        .groupBy("evnt_cd") \
        .agg(collect_list("tag").alias("cmpx_jrny_tag_vec"))
    return journey_df

# Step 3: Process audit details
def process_audit_details(curr_bus_dt, mlt_config_df, journey_tags_df):
    audit_df = spark.table("e_rolb_db.psdm_rolb_audit_details") \
        .filter(f"log_dte = '{curr_bus_dt}' AND tagvalue IS NOT NULL AND trim(tagvalue) != '' AND lower(trim(tagvalue)) != 'null'")
    
    # Transform tagname based on conditions
    audit_df = audit_df.withColumn(
        "tagname",
        when(
            lower(trim(col("acn_cat_nme"))).isin(
                ['/olb/bulkpay/grouppaymentstepfour.json', '/olb/fxpay/mrp/payments.json']
            ),
            regexp_replace(trim(col("tagname")), "[_0-9]", "")
        ).otherwise(col("tagname"))
    )
    
    # Calculate vol
    audit_df = audit_df.withColumn(
        "vol",
        when(
            (lower(trim(col("acn_cat_nme"))) == '/olb/bulkpay/grouppaymentstepfour.json') &
            (col("tagname").contains("_")) &
            (length(split(trim(col("tagname")), "_")) > 1),
            split(trim(col("tagname")), "_")[1]
        ).otherwise(lit(1))
    )
    
    # Dedup records
    audit_df = audit_df.dropDuplicates(["uid", "log_dte", "tagname", "tagvalue"])
    
    # Group by uid and apply transformations
    grouped_df = audit_df.groupBy(
        "uid", "mmb_num", "ses_idr", "log_tme", "acn_nme",
        "log_millisec", "acn_cat_nme", "msg_typ", "log_dte"
    ).agg(max("vol").alias("vol"))
    
    # Apply multi config transformations and create tag dictionary
    for row in mlt_config_df.collect():
        grouped_df = grouped_df.withColumn(
            row["new_col"],
            when(
                (lower(col(row["filter_col1"])) == row["filter_condn1"]) &
                (lower(col(row["filter_col2"])) == row["filter_condn2"]),
                col(row["value_col"])
            )
        )
    
    # Create tag dictionary for complex journey tags
    journey_tags = journey_tags_df.select("cmpx_jrny_tag_vec").first()["cmpx_jrny_tag_vec"]
    
    return grouped_df

# Main execution
def main():
    curr_bus_dt = spark.sql("SELECT current_date()").first()[0]
    mlt_config_df = get_multi_channel_config()
    journey_tags_df = get_journey_tags()
    final_df = process_audit_details(curr_bus_dt, mlt_config_df, journey_tags_df)
    
    # Save or show results as needed
    final_df.show()

if __name__ == "__main__":
    main()


-------------------------------------------------------------------------------------------------------




from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, rtrim, split, collect_list, max as spark_max, when, lit

# Initialize Spark session
spark = SparkSession.builder.appName("MultiConfigTranspose").getOrCreate()

# Step 1: Create multi config transpose file
# Read fdp_uk_ref_data_db.mlt_chnl_confg_map
mlt_chnl_confg_map = spark.read.table("fdp_uk_ref_data_db.mlt_chnl_confg_map")

# Get chnl_key from fdp_uk_ref_data_db.chnl_map where active=1 and chnl_cd='OLB'
chnl_key = spark.read.table("fdp_uk_ref_data_db.chnl_map").filter((col("active") == 1) & (col("chnl_cd") == 'OLB')).select("chnl_key").first()[0]

# Filter and transform
mlt_chnl_confg_map_filtered = mlt_chnl_confg_map.filter((col("active") == 1) & (col("chnl_key") == chnl_key))
mlt_chnl_confg_map_transformed = mlt_chnl_confg_map_filtered.withColumn("filter_coll", lower(rtrim(col("filter_coll")))) \
    .withColumn("filter_co12", lower(rtrim(col("filter_co12")))) \
    .withColumn("value_col", col("value_col")) \
    .withColumn("filter_condn1", lower(rtrim(col("filter_condn1")))) \
    .withColumn("filter_condn2", lower(rtrim(col("filter_condn2")))) \
    .withColumn("new_col", lower(rtrim(col("new_trans_col"))))

# Step 2: Read fdp_uk_ref_data_db.cust_jrny_confg_map and discover unique tag
cust_jrny_confg_map = spark.read.table("fdp_uk_ref_data_db.cust_jrny_confg_map")

# Filter and transform
cust_jrny_confg_map_filtered = cust_jrny_confg_map.filter((col("active") == 1) & (col("chnl_key") == chnl_key))
cust_jrny_confg_map_transformed = cust_jrny_confg_map_filtered.withColumn("cmpx_jrny_tag", lower(split(col("cmpx_jrny_tag"), ",")))

# Combine unique tag by grouping on evnt_cd
cust_jrny_confg_map_grouped = cust_jrny_confg_map_transformed.groupBy("evnt_cd").agg(collect_list("cmpx_jrny_tag").alias("cmpx_jrny_tag_vec"))

# Step 3: Read e_rolb_db.psdm_rolb_audit_details table
psdm_rolb_audit_details = spark.read.table("e_rolb_db.psdm_rolb_audit_details")

# Filter and transform
psdm_rolb_audit_details_filtered = psdm_rolb_audit_details.filter((col("log_dte") == lit("$CURR_BUS_DT")) & col("tagvalue").isNotNull() & (col("tagvalue") != '') & (lower(rtrim(col("tagvalue"))) != 'null'))
psdm_rolb_audit_details_transformed = psdm_rolb_audit_details_filtered.withColumn("tagname", when(lower(rtrim(col("acn_cat_nme"))).isin(['/olb/bulkpay/grouppaymentstepfour.json', '/olb/fxpay/mrp/payments-json']), regexp_replace(rtrim(col("tagname")), "[_0-9]", "*")).otherwise(col("tagname"))) \
    .withColumn("vol", when((lower(rtrim(col("acn_cat_nme"))) == '/olb/bulkpay/grouppaymentstepfour.json') & (instr(rtrim(col("tagname")), "_") > 0) & (size(split(rtrim(col("tagname")), " ")) > 1), split(rtrim(col("tagname")), "_")[1]).otherwise(lit(1)))

# Deduplicate
psdm_rolb_audit_details_dedup = psdm_rolb_audit_details_transformed.dropDuplicates(["uid", "log_dte", "tagname", "tagvalue"])

# Group and transform using lookup 'multi config transpose' file
psdm_rolb_audit_details_grouped = psdm_rolb_audit_details_dedup.groupBy("uid").agg(
    spark_max("vol").alias("vol"),
    collect_list("tagname").alias("tag_vec")
)

# Join with multi config transpose file
result = psdm_rolb_audit_details_grouped.join(mlt_chnl_confg_map_transformed, on="uid", how="left")

# Apply transformations
result_transformed = result.withColumn("new_col", when((lower(col("filter_coll")) == col("filter_condn1")) & (lower(col("filter_co12")) == col("filter_condn2")), col("value_col")).otherwise(col("new_col"))) \
    .withColumn("tag_vec", when(lower(col("tagname")).isin(cust_jrny_confg_map_grouped.select("cmpx_jrny_tag_vec").collect()[0]), create_map(col("tagname"), col("tagvalue"))).otherwise(col("tag_vec")))

# Show result
result_transformed.show()




-------------------------------------------------------------------------------------------------------------------------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, rtrim, split, collect_list, max as spark_max

# Initialize Spark session
spark = SparkSession.builder.appName("MultiConfigTranspose").getOrCreate()

# Step 1 - Create multi config transpose file
# Read fdp_uk_ref_data_db.mlt_chnl_confg_map
mlt_chnl_confg_map = spark.read.table("fdp_uk_ref_data_db.mlt_chnl_confg_map")
chnl_map = spark.read.table("fdp_uk_ref_data_db.chnl_map")

# Filter active=1 and chnl_key
chnl_key = chnl_map.filter((col("active") == 1) & (col("chnl_cd") == 'OLB')).select("chnl_key").first()[0]
filtered_mlt_chnl_confg_map = mlt_chnl_confg_map.filter((col("active") == 1) & (col("chnl_key") == chnl_key))

# Apply transformations
transformed_mlt_chnl_confg_map = filtered_mlt_chnl_confg_map.withColumn("filter_coll", lower(rtrim(col("filter_coll")))) \
    .withColumn("filter_co12", lower(rtrim(col("filter_co12")))) \
    .withColumn("value_col", col("value_col")) \
    .withColumn("filter_cond1", lower(rtrim(col("filter_cond1")))) \
    .withColumn("filter_cond2", lower(rtrim(col("filter_cond2")))) \
    .withColumn("new_col", lower(rtrim(col("new_trans_col"))))

# Step 2 - Read fdp_uk_ref_data_db.cust_jrny_confg_map and discover unique tag
cust_jrny_confg_map = spark.read.table("fdp_uk_ref_data_db.cust_jrny_confg_map")

# Filter active=1 and chnl_key
filtered_cust_jrny_confg_map = cust_jrny_confg_map.filter((col("active") == 1) & (col("chnl_key") == chnl_key))

# Extract fields and unique tags
unique_tags = filtered_cust_jrny_confg_map.withColumn("cmpx_jrny_tag", lower(split(col("cmpx_jrny_tag"), ","))) \
    .select("evnt_cd", "cmpx_jrny_tag").distinct()

# Combine unique tags by grouping on evnt_cd
combined_tags = unique_tags.groupBy("evnt_cd").agg(collect_list("cmpx_jrny_tag").alias("cmpx_jrny_tag_vec"))

# Step 3 - Read e_rolb_db.psdm_rolb_audit_details table
psdm_rolb_audit_details = spark.read.table("e_rolb_db.psdm_rolb_audit_details")

# Filter log_dte and tagvalue conditions
filtered_psdm_rolb_audit_details = psdm_rolb_audit_details.filter(
    (col("log_dte") == "$CURR_BUS_DT") &
    (col("tagvalue").isNotNull()) &
    (col("tagvalue") != "") &
    (lower(rtrim(col("tagvalue"))) != 'null')
)

# Apply transformations
transformed_psdm_rolb_audit_details = filtered_psdm_rolb_audit_details.withColumn(
    "tagname", 
    when(lower(rtrim(col("acn_cat_nme"))).isin(['/olb/bulkpay/grouppaymentstepfour.json', '/olb/fxpay/mrp/payments-json']),
         regexp_replace(rtrim(col("tagname")), "[_0-9]", "*")
    ).otherwise(col("tagname"))
).withColumn(
    "vol", 
    when((lower(rtrim(col("acn_cat_nme"))) == '/olb/bulkpay/grouppaymentstepfour.json') &
         (instr(rtrim(col("tagname")), "_") > 0) &
         (size(split(rtrim(col("tagname")), " ")) > 1),
         split(rtrim(col("tagname")), "_")[1]
    ).otherwise(1)
)

# Deduplicate
deduped_psdm_rolb_audit_details = transformed_psdm_rolb_audit_details.dropDuplicates(["uid", "log_dte", "tagname", "tagvalue"])

# Group and apply transformations using lookup 'multi config transpose' file
grouped_psdm_rolb_audit_details = deduped_psdm_rolb_audit_details.groupBy("uid").agg(
    spark_max("vol").alias("vol"),
    # Add other fields as needed
)

# Join with multi config transpose file
final_result = grouped_psdm_rolb_audit_details.join(transformed_mlt_chnl_confg_map, on="uid", how="left")

# Apply final transformations
final_result = final_result.withColumn(
    "new_col",
    when((lower(col("filter_coll")) == col("filter_cond1")) &
         (lower(col("filter_co12")) == col("filter_cond2")),
         col("value_col")
    ).otherwise(col("new_col"))
).withColumn(
    "tag_vec",
    when(col("tagname").isin(combined_tags.select("cmpx_jrny_tag_vec").collect()[0]),
         array(struct(col("tagname"), col("tagvalue")))
    ).otherwise(col("tag_vec"))
)

# Show final result
final_result.show()

