modified_df = df.withColumn(
    "modified_text",
        # Add quote before the closing braces
        regexp_replace(col("text"), "(.*?)(}{1,})$", "$1\"$2")
    )




from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when

# Create SparkSession
spark = SparkSession.builder.appName("Add Quote Before Braces").getOrCreate()

# Sample DataFrame
data = [
    ("Hello}}",),
    ("Test}}}",),
    ("No braces",),
    ("Multiple}}}}",),
    ("Single}",),
    ("Middle}Text",)
]

# Create DataFrame
df = spark.createDataFrame(data, ["text"])

# Add quote before multiple closing braces at the end
modified_df = df.withColumn(
    "modified_text",
        # Add quote before the closing braces
        regexp_replace(col("text"), "(.*?)(}{1,})$", "$1\"$2")
    )


# Show results
modified_df.show(truncate=False)
