
# 1. Filter out journeys with any IncompleteMap='N'
df_incomplete = df.groupBy("journey_id").agg(collect_set("IncompleteMap").alias("IncompleteMapSet"))
df_incomplete = df_incomplete.filter(array_contains(col("IncompleteMapSet"), "N"))
df_remaining = df_incomplete.filter(~array_contains(col("IncompleteMapSet"), "N"))

# 2. Filter out journeys with emplx_jrny_attrb containing both 'And' and 'Or' or other than 'And'/'Or'
df_attrb = df.groupBy("journey_id").agg(collect_set("emplx_jrny_attrb").alias("AttrbSet"))
df_attrb = df_attrb.filter((array_contains(col("AttrbSet"), "And") & array_contains(col("AttrbSet"), "Or")) | 
                           size(col("AttrbSet").filter(lambda x: x not in ["And", "Or"])) > 0)
df_remaining = df_remaining.join(df_attrb, "journey_id", "left_anti")

# 3. Filter out journeys with operators other than the specified ones
valid_operators = ["EQ", "NE", "GT", "LT", "GTEQ", "LTEQ", "LK", "NIK", "B!"]
df_operator = df.groupBy("journey_id").agg(collect_set("operator").alias("OperatorSet"))
df_operator = df_operator.filter(size(col("OperatorSet").filter(lambda x: x not in valid_operators)) > 0)
df_remaining = df_remaining.join(df_operator, "journey_id", "left_anti")

# 4. Filter out journeys with duplicates on (evnt_ed, emplx_jrny_attrb, flow_num)
df_duplicates = df.groupBy("journey_id").agg(countDistinct("evnt_ed", "emplx_jrny_attrb", "flow_num").alias("DistinctCount"),
                                             count("evnt_ed").alias("TotalCount"))
df_duplicates = df_duplicates.filter(col("DistinctCount") != col("TotalCount"))
df_remaining = df_remaining.join(df_duplicates, "journey_id", "left_anti")

# Combine all removed data
df_removed = df_incomplete.union(df_attrb).union(df_operator).union(df_duplicates).distinct()

# Show the final DataFrames
df_remaining.show()
df_removed.show()



--------------------------------------------------------------------------------------------------



# Step 1: Remove journeys with IncompleteMap='N'
df_filtered_1 = df.groupBy("journey").agg(collect_list("IncompleteMap").alias("IncompleteMaps"))
df_filtered_1 = df_filtered_1.filter(~array_contains(col("IncompleteMaps"), "N"))

# Step 2: Remove journeys with both 'And' and 'Or' or other than 'And'/'Or' in emplx_jrny_attrb
df_filtered_2 = df_filtered_1.join(df, "journey").groupBy("journey").agg(collect_list("emplx_jrny_attrb").alias("Attributes"))
df_filtered_2 = df_filtered_2.filter(~((array_contains(col("Attributes"), "And") & array_contains(col("Attributes"), "Or")) | 
                                       (size(col("Attributes")) != size(col("Attributes").filter(lambda x: x in ["And", "Or"])))))

# Step 3: Remove journeys with invalid operators
df_filtered_3 = df_filtered_2.join(df, "journey").groupBy("journey").agg(collect_list("operator").alias("Operators"))
df_filtered_3 = df_filtered_3.filter(size(col("Operators").filter(lambda x: x in valid_operators)) == size(col("Operators")))

# Step 4: Remove journeys with duplicates on (evnt_ed, emplx_jrny_attrb, flow_num)
df_filtered_4 = df_filtered_3.join(df, "journey").groupBy("journey").agg(countDistinct("evnt_ed", "emplx_jrny_attrb", "flow_num").alias("DistinctCount"), count("evnt_ed").alias("TotalCount"))
df_filtered_4 = df_filtered_4.filter(col("DistinctCount") == col("TotalCount"))

# Get the final filtered DataFrame
df_filtered = df_filtered_4.join(df, "journey").select(df.columns)

# Get the removed DataFrame
df_removed = df.join(df_filtered, "journey", "left_anti")

# Show the results
df_filtered.show()
df_removed.show()


--------------------------------------------------------------------------------------------------


# List of valid operators
valid_operators = ["EQ", "NE", "GT", "LT", "GTEQ", "LTEQ", "LK", "NIK", "B!"]

# 1. Remove journeys where IncompleteMap = 'N'
df_filtered = df.filter(col("IncompleteMap") != "N")

# 2. Remove journeys where 'emplx_jrny_attrb' has both 'And' and 'Or'
# Group by journey_id and collect unique values of emplx_jrny_attrb
journeys_with_and_or = df_filtered.groupBy("journey_id").agg(
    collect_list("emplx_jrny_attrb").alias("emplx_jrny_attrb_list")
)

# Filter journeys that have both 'And' and 'Or'
journeys_with_invalid_attrb = journeys_with_and_or.filter(
    (size(array_distinct(col("emplx_jrny_attrb_list"))) > 1) &
    (col("emplx_jrny_attrb_list").contains("And")) &
    (col("emplx_jrny_attrb_list").contains("Or"))
)

# Get the journey ids that need to be removed
invalid_journey_ids = journeys_with_invalid_attrb.select("journey_id").rdd.flatMap(lambda x: x).collect()

# Remove journeys with both 'And' and 'Or'
df_filtered = df_filtered.filter(~col("journey_id").isin(invalid_journey_ids))

# 3. Remove journeys with invalid operators
df_filtered = df_filtered.filter(col("operator").isin(valid_operators))

# 4. Remove journeys with duplicates on (evnt_ed, emplx_jrny_attrb, flow_num)
df_filtered_with_duplicates = df_filtered.groupBy("evnt_ed", "emplx_jrny_attrb", "flow_num").count()
duplicate_journeys = df_filtered_with_duplicates.filter(col("count") > 1).select("evnt_ed", "emplx_jrny_attrb", "flow_num")

# Get the journey ids with duplicates
duplicate_journey_ids = df_filtered.join(duplicate_journeys, on=["evnt_ed", "emplx_jrny_attrb", "flow_num"], how="inner").select("journey_id").distinct().rdd.flatMap(lambda x: x).collect()

# Final filtered DataFrame
df_filtered_final = df_filtered.filter(~col("journey_id").isin(duplicate_journey_ids))

# DataFrame with removed journeys
removed_journeys = df.filter(~col("journey_id").isin(df_filtered_final.select("journey_id").rdd.flatMap(lambda x: x).collect()))

# DataFrame with not removed journeys
not_removed_journeys = df_filtered_final

# Show the results
print("Removed Journeys:")
removed_journeys.show()

print("Not Removed Journeys:")



# Define the regular expression pattern to extract all data between double quotes
pattern = r'\"(.*?)\"'

# Extract all occurrences of data between double quotes
df = df.withColumn("ExtractedData", expr(f"regexp_extract_all(ComplexJourneyAttributes, '{pattern}')"))


# Create DataFrame
df = spark.createDataFrame(data, ["text"])

# Extract text between escaped double quotes using regular expression
df_extracted = df.withColumn("extracted_text", regexp_extract("text", r'\\\"(.*?)\\\"', 1))


# Extract text between \'""\' using regular expression
df_extracted = df.withColumn("extracted_text", regexp_extract("text", r"\\\'\"(.*?)\"\\\'", 1))


not_removed_journeys.show()

# Stop Spark session
spark.stop()
