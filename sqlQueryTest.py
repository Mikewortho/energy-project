def splitOnDate(startDate, endDate, TrainingData, TestData):
    df = spark.sql("SELECT * from temp where CAST(CONCAT( Year, '-', Month, '-', Day, ' ', Hour ) as timestamp) BETWEEN '"+startDate+"' AND '"+endDate+"'")

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

# Print first ten rows (0 = header)
df.show(10)

# SQL query and print
df2 = spark.sql("SELECT Demand from temp WHERE Hour >= 5 AND Day <= 4").show()
splitOnDate("2018-02-11 12:00:00","2018-02-11 13:00:00", 80, 20)
# Ensure previous spark context has closed (Will fix this)
sc.stop()