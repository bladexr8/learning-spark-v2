from pyspark.sql import SparkSession

from pyspark.sql.functions import *

# Main program
if __name__ == "__main__":
    # Create a SparkSession
    spark = (SparkSession
        .builder
        .appName("SparkSQLExampleApp")
        .getOrCreate())

    # file to read
    csv_file = "data/departuredelays.csv"
    
    # Read and create a temporary view
    # infer schema (may want to specify 
    # for larger files)
    df = (spark.read.format("csv")
        .option("samplingRatio", 0.001)
        .option("inferSchema", "true")
        .option("header", "true")
        .load(csv_file))
    df.createOrReplaceTempView("us_delay_flights_tbl")

    # find flights with a distance of
    # > 1000 miles
    spark.sql("""SELECT distance, origin, destination
        FROM us_delay_flights_tbl WHERE distance > 1000 
        ORDER BY distance DESC""").show(10)

    # flights between San Francisco (SFO) and 
    # Chicago (ORD) with at least a 2 hour delay
    spark.sql("""SELECT date, delay, origin, destination 
        FROM us_delay_flights_tbl 
        WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
        ORDER BY delay DESC""").show(10)

    # label flights with an indication of delay experienced
    spark.sql("""SELECT delay, origin, destination,
                    CASE
                        WHEN delay > 360 THEN 'Very Long Delays'
                        WHEN delay > 120 AND delay < 350 THEN 'Long Delays'
                        WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                        WHEN delay > 0 AND delay < 50 THEN 'Tolerable Delays'
                        WHEN delay = 0 THEN 'No Delays'
                        ELSE 'Early'
                    END AS Flight_Delays
                    FROM us_delay_flights_tbl
                    ORDER BY origin, delay DESC""").show(10)

    # create a Spark DB
    # spark.sql("CREATE DATABASE learn_spark_db")
    # spark.sql("USE learn_spaark_db")

    # create a tab;e
    # spark.sql("""CREATE TABLE managed_us_delay_flights_tbl 
    #     (date STRING, delay INT, distance INT, origin STRING, destination STRING""")
