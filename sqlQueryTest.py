import pyspark
from pyspark.sql import SQLContext

#aggragates hourly data into days (Average)(BETWEEN dates)
def aggtoDay(startDate, endDate):
    days = spark.sql("SELECT BA, Date, AVG(Demand) TotalDemand from (SELECT BA, Demand, DATE(TimeAndDate) as Date from temp where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY Date, BA ORDER BY Date" ).repartition(1).write.csv("AggragatedToDays.csv")
    return days

#aggragates daily data into weeks (Average) (BETWEEN dates)
def aggtoWeek(startDate, endDate):
    weeks = spark.sql("SELECT Year(Date) year, weekofyear(Date) week, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from temp where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY week, year,BA ORDER BY year, week" ).repartition(1).write.csv("AggragatedToWeeks.csv")
    return weeks

def aggtoMonth(startDate, endDate):
    months = spark.sql("SELECT  Year(Date) year, Month(Date) month, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from temp where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY month,year,BA ORDER BY year,month" ).repartition(1).write.csv("AggragatedToMonths.csv")
    return months
#aggragates weekly data into years (Average) (BETWEEN dates) 
def aggtoYear(startDate, endDate):
    years = spark.sql("SELECT  Year(Date) year, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from temp where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"')  GROUP BY year, BA ORDER BY year").repartition(1).write.csv("AggragatedToYears.csv")
    return years

#performs all aggregations simultaneously (Average) (No date parameters)
def aggtoDWMY():
    days = spark.sql("SELECT BA, Date, AVG(Demand) TotalDemand from (SELECT BA, Demand, DATE(TimeAndDate) as Date from temp where Demand IS NOT NULL) GROUP BY Date, BA ORDER BY Date" ).repartition(1).write.csv("AggragatedToDays.csv")
    weeks = spark.sql("SELECT BA, Year(Date) year, weekofyear(Date) week, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from temp where Demand IS NOT NULL ) GROUP BY week, year,BA ORDER BY year, week" ).repartition(1).write.csv("AggragatedToWeeks.csv")
    months = spark.sql("SELECT BA, Year(Date) year, Month(Date) month, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from temp where Demand IS NOT NULL ) GROUP BY month,year,BA ORDER BY year,month" ).repartition(1).write.csv("AggragatedToMonths.csv")
    years = spark.sql("SELECT BA, Year(Date) year, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from temp where Demand IS NOT NULL) GROUP BY year, BA ORDER BY year").repartition(1).write.csv("AggragatedToYears.csv")
    return days,weeks,months,years

# Split data into test and training (TODO speedup with indexing query instead)
def trainingSplit(trainingRatio, fullData):
    count = fullData.count()
    train_count = int(round((count * trainingRatio)))
    training_data = fullData.limit(train_count)
    test_data = fullData.subtract(training_data)
    return training_data, test_data

# Split data into k folds (TODO speedup with indexing query instead)
def foldsSplit(k, fullData):
    count = fullData.count()
    pivot = int(round((count / k)))  
    folds = []
    for i in range(k):
        if(i == 0):
            folds.append(fullData.limit(pivot))
        else:
            for j in range(len(folds)):
                temp = fullData.subtract(folds[j])
            folds.append(temp.limit(pivot))         
    return folds            
            
# Create spark context and sparkSQL objects
sc = pyspark.SparkContext.getOrCreate()
spark = SQLContext(sc) 

# Use sparkSQL to read in CSV
df = (spark.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .load("data/elec_demand_hourly.csv"))

# Register temporary table including DateTime representation of columns
df.registerTempTable("temp")
df = spark.sql("SELECT *, CAST(CONCAT( Year, '-', Month, '-', Day, ' ', Hour ) as timestamp) as TimeAndDate from temp ORDER BY TimeAndDate ASC")
df.registerTempTable("temp")

# Below shows examples of how to use some queries

#splitOnDate("2017-02-11 12:00:00","2018-02-11 13:00:00", 80, 20)
#aggtoDWMY()
# Print first ten rows (0 = header)
#df.show(10)

# SQL query and print
#df2 = spark.sql("SELECT Demand from temp WHERE Hour >= 5 AND Day <= 4").show()

# Ensure previous spark context has closed (Will fix this)
sc.stop() 
