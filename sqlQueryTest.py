import pyspark
from pyspark.sql.functions import avg, stddev_pop
from pyspark.sql import SQLContext       
import zlib

# extracts data between two dates
def extractDate(startDate, endDate):
    return spark.sql("SELECT * from temp where TimeAndDate BETWEEN '" + startDate+ "' AND '" + endDate + "' ORDER BY TimeAndDate ASC")

#aggragates hourly data into days (Average)(BETWEEN dates)
def aggtoDay(startDate, endDate):
    days = spark.sql("SELECT BA, Date, AVG(Demand) TotalDemand from (SELECT BA, Demand, DATE(TimeAndDate) as Date from demands where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY Date, BA ORDER BY Date" )#.repartition(1).write.csv("data/AggragatedToDays.csv")
    return days

#aggragates daily data into weeks (Average) (BETWEEN dates)
def aggtoWeek(startDate, endDate):
    weeks = spark.sql("SELECT Year(Date) year, weekofyear(Date) week, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from demands where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY week, year,BA ORDER BY year, week" )#.repartition(1).write.csv("data/AggragatedToWeeks.csv")
    return weeks

def aggtoMonth(startDate, endDate):
    months = spark.sql("SELECT  Year(Date) year, Month(Date) month, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from demands where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY month,year,BA ORDER BY year,month" )#.repartition(1).write.csv("data/AggragatedToMonths.csv")
    return months
#aggragates weekly data into years (Average) (BETWEEN dates) 
def aggtoYear(startDate, endDate):
    years = spark.sql("SELECT  Year(Date) year, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from demands where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"')  GROUP BY year, BA ORDER BY year")#.repartition(1).write.csv("data/AggragatedToYears.csv")
    return years

#performs all aggregations simultaneously (Average) (No date parameters)
def aggtoDWMY():
    days = spark.sql("SELECT BA, Date, AVG(Demand) TotalDemand from (SELECT BA, Demand, DATE(TimeAndDate) as Date from demands where Demand IS NOT NULL) GROUP BY Date, BA ORDER BY Date" )#.repartition(1).write.csv("data/AggragatedToDays.csv")
    weeks = spark.sql("SELECT BA, Year(Date) year, weekofyear(Date) week, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from demands where Demand IS NOT NULL ) GROUP BY week, year,BA ORDER BY year, week" )#.repartition(1).write.csv("data/AggragatedToWeeks.csv")
    months = spark.sql("SELECT BA, Year(Date) year, Month(Date) month, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from demands where Demand IS NOT NULL ) GROUP BY month,year,BA ORDER BY year,month" )#.repartition(1).write.csv("data/AggragatedToMonths.csv")
    years = spark.sql("SELECT BA, Year(Date) year, AVG(Demand) TotalDemand from (SELECT BA, Demand, TimeAndDate as Date from demands where Demand IS NOT NULL) GROUP BY year, BA ORDER BY year")#.repartition(1).write.csv("data/AggragatedToYears.csv")
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

# Method to get missing dates for each BA, returns a list of format [[Dataframe(of missing dates for BA), BA],[Dataframe(of missing dates for BA), BA]......]
def getMissingDates():
    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max ,Min(TimeAndDate) min from demands GROUP BY BA")
    sample2 = dftemp.take(dftemp.count())
    listOfDataframes = []
    for BA, ENDTIME, STARTTIME in sample2:
        tempList = []
        tempList.append(spark.sql("Select TimeAndDate from (Select * from demands where TimeAndDate<'"+str(ENDTIME)+"' AND TimeAndDate>'"+str(STARTTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where BA='"+BA+"') AS L WHERE BA IS NULL "))
        tempList.append(BA)
        listOfDataframes.append(tempList)
    return listOfDataframes

# Method that returns outliers in lists for each BA, outlier = 4 * std away from mean
def getOutliers():
    authorities = spark.sql("SELECT DISTINCT BA from demands").collect()
    ba_demands = []
    outliers = []
    for row in range(len(authorities)):
        val = authorities[row]
        ba_demands.append(spark.sql("SELECT Demand, TimeAndDate, BA FROM demands WHERE BA = '%s'" % val))
    for ba in range(len(ba_demands)):
        print(str(authorities[ba]) + ":" + str(ba) + " / " +  str(len(ba_demands)))
        ba_demands[ba].registerTempTable("demand_table")
        mean = ba_demands[ba].select(avg(ba_demands[0]["Demand"])).collect()
        std = ba_demands[ba].select(stddev_pop(ba_demands[0]["Demand"])).collect()        
        lower_bounds = mean[0][0] - 5 * std[0][0]
        upper_bounds = mean[0][0] + 5 * std[0][0]  
        o = spark.sql("SELECT Demand, TimeAndDate, BA FROM demand_table WHERE demand > {0} OR demand < {1} OR demand < 0 OR demand = 0".format(upper_bounds, lower_bounds))
        outliers.append(o)
    return outliers

# Aggregate into regions
def regionDataframe():
    regions = spark.sql("Select BA, Name, TimeZone, Region, Name, AVG(Demand) Demand from (Select * from demands natural join regions) group by BA, Name, TimeZone, Region, Name").show(59)
    return regions

#Turn a dataframe containing TimeAndDate, Demand as well as BA into a Json and compress's it using zlib
def turnDataframeIntoJson(df5):
    df = df5.rdd
    tcp_interactions_out = df.map(lambda df: '{\"BA\":\"%s\",\"TimeAndDate\":\"%s\",\"Demand\":%s}' % (df.BA, df.TimeAndDate, df.Demand))
    temp = tcp_interactions_out.collect()
    sendString=''
    for temp2 in temp:
        sendString += temp2+'\n'
    compressedJson = zlib.compress(sendString.encode())
    return(compressedJson)
    
# Create spark context and sparkSQL objects
sc = pyspark.SparkContext.getOrCreate()
spark = SQLContext(sc) 

# Use sparkSQL to read in CSV
demand_table = spark.read.csv("data/elec_demand_hourly.csv", header=True, inferSchema=True)
date_table = spark.read.csv("data/dates.csv", header=True, inferSchema=True)
region_table = spark.read.csv("data/region_table.csv", header=True, inferSchema=True)
forecast_table = spark.read.csv("data/forecast_hourly.csv", header=True, inferSchema=True)
region_table.registerTempTable("regions")
demand_table.registerTempTable("demands")
date_table.registerTempTable("dates")
forecast_table.registerTempTable("forecasts")


# Ensure previous spark context has closed (Will fix this)
sc.stop() 
