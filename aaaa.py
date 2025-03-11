from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, substring, length, lit, split, trim
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("AccountProcessing").getOrCreate()

# Sample Data
data = [("123456789012345", "234567890123456", "100000.50"),
        ("98765432109876", "87654321098765", "1234567890121212.34"),
        ("12345", "54321", "123233333333332222222222111111.45")]

# Define schema
columns = ["fr_acct_id", "to_acct_id", "tran_amt"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Assuming `df` is your DataFrame and `step5` is an alias for it
df = df.withColumn(
    "fr_acct_id",
    F.when(
        F.length(F.col("fr_acct_id")) >= 14,
        F.concat(
            F.substring(F.col("fr_acct_id"), 0, 6),
            F.lit("|"),
            F.substring(
                F.col("fr_acct_id"),
                -7,
                7
            )
        )
    ).otherwise(F.col("fr_acct_id"))
)

df = df.withColumn(
    "to_acct_id",
    F.when(
        F.length(F.col("to_acct_id")) >= 14,
        F.concat(
            F.substring(F.col("to_acct_id"), 0, 6),
            F.lit("|"),
            F.substring(
                F.col("to_acct_id"),
                -7,
                7
            )
        )
    ).otherwise(F.col("to_acct_id"))
)

df = df.withColumn(
    "tran_amt",
    F.when(
        F.length(F.split(F.col("tran_amt"), "\\.").getItem(0)) > 12,
        F.substring(F.col("tran_amt"), 1, 12)
    ).when(
        F.length(F.split(F.col("tran_amt"), "\\.").getItem(0)) < 12,
        F.col("tran_amt")
    ).otherwise(F.lit(None))
)

df.show()
