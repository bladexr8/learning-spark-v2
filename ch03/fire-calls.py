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

    # manually rename a column and show calls where
    # response times were > 5 minutes
    new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedMins")
    (new_fire_df
        .select("ResponseDelayedMins")
        .where(col("ResponseDelayedMins") > 5)
        .show(5, False))

    # convert columns using sparks date/time functions
    fire_ts_df = (new_fire_df
        .withColumn("IncidentDate", to_timestamp(col("CallDate"), "dd/MM/yyyy"))
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "dd/MM/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "dd/MM/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm"))

    # Select the converted columns
    (fire_ts_df
        .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
        .show(5, False))

    # how many years of calls?
    (fire_ts_df
        .select(year('IncidentDate'))
        .distinct()
        .orderBy(year('IncidentDate'))
        .show())

    # find most common types of calls
    (fire_ts_df
        .select("CallType")
        .where(col("CallType").isNotNull())
        .groupBy("CallType")
        .count()
        .orderBy("count", ascending=False)
        .show(n=10, truncate=False))

    # basic statistics
    # 1. sum of alarms
    # 2. average response time
    # 3. minimum and maximum response times
    (fire_ts_df
        .select(sum("NumAlarms"), avg("ResponseDelayedMins"),
            min("ResponseDelayedMins"), max("ResponseDelayedMins"))
            .show())