import pyspark
from pyspark.sql import SQLContext
import zlib

# extracts data between two dates
def extractDate(startDate, endDate):
    return spark.sql("SELECT * from temp where TimeAndDate BETWEEN '" + startDate+ "' AND '" + endDate + "' ORDER BY TimeAndDate ASC")

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

# Method to get missing dates for each BA, returns a list of format [[Dataframe(of missing dates for BA), BA],[Dataframe(of missing dates for BA), BA]......]
def getMissingDates():
    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max ,Min(TimeAndDate) min from temp GROUP BY BA")
    sample2 = dftemp.take(dftemp.count())
    listOfDataframes = []
    for BA, ENDTIME, STARTTIME in sample2:
        tempList = []
        tempList.append(spark.sql("Select TimeAndDate from (Select * from temp2 where TimeAndDate<'"+str(ENDTIME)+"' AND TimeAndDate>'"+str(STARTTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from temp where BA='"+BA+"') AS L WHERE BA IS NULL "))
        tempList.append(BA)
        listOfDataframes.append(tempList)
    return listOfDataframes

def regionDataframe():
    letsSee = spark.sql("Select BA, Name, TimeZone, Region, Name, AVG(Demand) Demand from (Select * from temp natural join temp3) group by BA, Name, TimeZone, Region, Name").show(59)
    return

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
df = spark.read.csv("newTable.csv", header=True, inferSchema=True)
df2 = spark.read.csv("temp.csv", header=True, inferSchema=True)
df3 = spark.read.csv("temp3.csv", header=True, inferSchema=True)
df3.registerTempTable("temp3")
df.registerTempTable("temp")
df2.registerTempTable("temp2")






#regionDataframe()
#Query = spark.sql("SELECT Year(TimeAndDate), Month(TimeAndDate), weekOfYear(TimeAndDate), Hour(TimeAndDate), Demand from temp where TimeAndDate between '"+startDate+"' and '"+endDate+"' and BA = '"+region+"'")
#Query.show(10)





# Below shows examples of how to use some queries

#splitOnDate("2017-02-11 12:00:00","2018-02-11 13:00:00", 80, 20)
#aggtoDWMY()
# Print first ten rows (0 = header)
#df.show(10)

# SQL query and print
#df2 = spark.sql("SELECT Demand from temp WHERE Hour >= 5 AND Day <= 4").show()

# Ensure previous spark context has closed (Will fix this)
sc.stop() 
