from pyspark.sql import SparkSession

from pyspark.sql.functions import *

# Main program
if __name__ == "__main__":
    # Create a SparkSession
    spark = (SparkSession
        .builder
        .appName("Example-3_6")
        .getOrCreate())

    # read file
    sf_fire_file = "data/sf-fire-calls.csv"
    fire_df = (spark.read.format("csv")
        .option("samplingRatio", 0.001)
        .option("header", "true")
        .load(sf_fire_file))

    # a "projection" is a way to return the rows
    # matching a certain relational condition by
    # using filters
    few_fire_df = (fire_df
        .select("IncidentNumber", "AvailableDtTm", "CallType")
        .where(col("CallType") != "Medical Incident"))
    few_fire_df.show(5, truncate=False)

    # distinct call types
    distinct_call_types_df = (fire_df
        .select("CallType")
        .where(col("CallType").isNotNull())
        .distinct()
        .show(10, False))

    # return number of distinct types of calls
    # using countDistinct()
    num_call_type_df = (fire_df
        .select("CallType")
        .where(col("CallType").isNotNull())
        .agg(countDistinct("CallType").alias("DistinctCallTypes"))
        .show())