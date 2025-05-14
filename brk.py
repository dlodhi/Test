modified_df = df.withColumn(
    "modified_text",
        # Add quote before the closing braces
        regexp_replace(col("text"), "(.*?)(}{1,})$", "$1\"$2")
    )

