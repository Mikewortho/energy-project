#
def splitOnDate(startDate, endDate, TrainingData, TestData):
    df = spark.sql("SELECT * from temp where TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"'").show(100)

import pyspark
from pyspark.sql import SQLContext
# Create spark context and sparkSQL objects
sc = pyspark.SparkContext.getOrCreate()
spark = SQLContext(sc)

# Use sparkSQL to read in CSV
df = (spark.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .load("data/elec_demand_hourly.csv"))
df.registerTempTable("temp")
df = spark.sql("SELECT *, CAST(CONCAT( Year, '-', Month, '-', Day, ' ', Hour ) as timestamp) as TimeAndDate from temp")
df.registerTempTable("temp")
splitOnDate("2018-02-11 12:00:00","2018-02-11 13:00:00", 80, 20)
# Print first ten rows (0 = header)
# SQL query and print
# Ensure previous spark context has closed (Will fix this)
sc.stop()