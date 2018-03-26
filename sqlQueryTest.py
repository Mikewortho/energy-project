import pyspark
from pyspark.sql.functions import avg, stddev_pop
from pyspark.sql import SQLContext       
import zlib
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import TimestampType
from pyspark.sql.types import StringType

# extracts data between two dates
def extractDate(startDate, endDate):
    return spark.sql("SELECT * from demands where TimeAndDate BETWEEN '" + startDate+ "' AND '" + endDate + "' ORDER BY TimeAndDate ASC")

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
    sample2 = dftemp.collect()
    listOfDataframes = []
    for BA, ENDTIME, STARTTIME in sample2:
        listOfDataframes.append(spark.sql("Select '"+BA+"' as BA, TimeAndDate from (Select * from dates where TimeAndDate<'"+str(ENDTIME)+"' AND TimeAndDate>'"+str(STARTTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where BA='"+BA+"') AS L WHERE BA IS NULL "))
    return listOfDataframes


#Concat method
## Method to get missing dates for each BA, returns a list of format [[Dataframe(of missing dates for BA), BA],[Dataframe(of missing dates for BA), BA]......]
#def getMissingDates():
#    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max ,Min(TimeAndDate) min from demands GROUP BY BA")
#    sample2 = dftemp.collect()
#    query = ""
#    firstLine = True
#    for BA, ENDTIME, STARTTIME in sample2:
#        if(firstLine==True):
#            query += "Select '"+BA+"' as BA, TimeAndDate from (Select * from dates where TimeAndDate<'"+str(ENDTIME)+"' AND TimeAndDate>'"+str(STARTTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where BA='"+BA+"') AS L WHERE BA IS NULL "
#            firstLine = False
#        else:
#            query += "union all Select '"+BA+"' as BA, TimeAndDate from (Select * from dates where TimeAndDate<'"+str(ENDTIME)+"' AND TimeAndDate>'"+str(STARTTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where BA='"+BA+"') AS L WHERE BA IS NULL "
#    letsSee = spark.sql(query)
#    return letsSee

#Union method.
## Method to get missing dates for each BA, returns a list of format [[Dataframe(of missing dates for BA), BA],[Dataframe(of missing dates for BA), BA]......]
#def getMissingDates():
#    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max ,Min(TimeAndDate) min from demands GROUP BY BA")
#    sample2 = dftemp.collect()
#    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True)]
#    schema = StructType(field)
#    df = spark.createDataFrame([],schema)
#    for BA, ENDTIME, STARTTIME in sample2:
#        df = df.unionAll(spark.sql("Select '"+BA+"' as BA, TimeAndDate from (Select * from dates where TimeAndDate<'"+str(ENDTIME)+"' AND TimeAndDate>'"+str(STARTTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where BA='"+BA+"') AS L WHERE BA IS NULL "))
#    return df

def getOutliersImproved():
    dftemp = spark.sql("Select BA, MEAN(Demand) Mean , STD(Demand) STD from demands GROUP BY BA").collect()
    listOfDataframes = []
    for BA, Mean, STD in dftemp:
        lower_bounds = Mean - 5 * STD
        upper_bounds = Mean + 5 * STD
        listOfDataframes.append(spark.sql("Select BA, TimeAndDate from (Select * from demands where (demand > {0} OR demand < {1} OR demand < 0 OR demand = 0) and BA = '{2}')".format(upper_bounds, lower_bounds, BA)))
    return listOfDataframes

# Aggregate into regions
def regionDataframe():
    regions = spark.sql("Select Region, AVG(Demand) Demand, TimeAndDate from (Select * from demands natural join regions) group by Region, TimeAndDate").show(59)
    return regions







def createQueryToDataframeHourBA(model, startDate, endDate):
    return spark.sql("SELECT BA, TimeAndDate, Demand from "+model+" where TimeAndDate BETWEEN '" + startDate+ "' AND '" + endDate + "'")

def createQueryToDataframeYearBA(model, startDate, endDate):
    return spark.sql("SELECT BA, CAST(CONCAT(Year(Date)) as timestamp) TimeAndDate, AVG(Demand) Demand from (SELECT BA, Demand, TimeAndDate as Date from "+model+" where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"')  GROUP BY Year(Date), BA")#.repartition(1).write.csv("data/AggragatedToYears.csv")

def createQueryToDataframeMonthBA(model, startDate, endDate):
    return spark.sql("SELECT BA, CAST(CONCAT(Year(Date), '-', Month(Date)) as timestamp) TimeAndDate, AVG(Demand) Demand from (SELECT BA, Demand, TimeAndDate as Date from "+model+" where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY Month(Date),Year(Date),BA" )#.repartition(1).write.csv("data/AggragatedToMonths.csv")

def createQueryToDataframeDayBA(model, startDate, endDate):
    return spark.sql("SELECT BA, Date as TimeAndDate, AVG(Demand) Demand from (SELECT BA, Demand, DATE(TimeAndDate) as Date from "+model+" where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY TimeAndDate, BA" )

def createQueryToDataframeWeekBA(model, startDate, endDate):
    return spark.sql("SELECT BA, CONCAT(Year(Date),'-',weekofyear(Date)) TimeAndDate ,AVG(Demand) Demand from (SELECT BA, Demand, TimeAndDate as Date from "+model+" where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY weekofyear(Date), Year(Date), BA" )#.repartition(1).write.csv("data/AggragatedToWeeks.csv")

def createQueryToDataframeHourRegion(model, startDate, endDate):
    return spark.sql("SELECT Region, TimeAndDate, AVG(Demand) Demand from "+model+" natural join regions where TimeAndDate BETWEEN '" + startDate+ "' AND '" + endDate + "' Group by Region, TimeAndDate")

def createQueryToDataframeYearRegion(model, startDate, endDate):
    return spark.sql("SELECT Region, CAST(CONCAT(Year(Date)) as timestamp) TimeAndDate, AVG(Demand) Demand from (SELECT Region, Demand, TimeAndDate as Date from "+model+" natural join regions where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"')  GROUP BY Year(Date), Region")#.repartition(1).write.csv("data/AggragatedToYears.csv")

def createQueryToDataframeMonthRegion(model, startDate, endDate):
    return spark.sql("SELECT Region, CAST(CONCAT(Year(Date), '-', Month(Date)) as timestamp) TimeAndDate, AVG(Demand) Demand from (SELECT Region, Demand, TimeAndDate as Date from "+model+" natural join regions where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY Month(Date),Year(Date), Region" )#.repartition(1).write.csv("data/AggragatedToMonths.csv")

def createQueryToDataframeDayRegion(model, startDate, endDate):
    return spark.sql("SELECT Region, Date as TimeAndDate, AVG(Demand) Demand from (SELECT Region, Demand, DATE(TimeAndDate) as Date from "+model+" natural join regions where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY TimeAndDate, Region" )

def createQueryToDataframeWeekRegion(model, startDate, endDate):
    return spark.sql("SELECT Region, CONCAT(Year(Date),'-',weekofyear(Date)) TimeAndDate ,AVG(Demand) Demand from (SELECT Region, Demand, TimeAndDate as Date from "+model+" natural join regions where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY weekofyear(Date), Year(Date), Region" )#.repartition(1).write.csv("data/AggragatedToWeeks.csv")

def createQueryToDataframeHourState(model, startDate, endDate):
    return spark.sql("SELECT State, TimeAndDate, AVG(Demand) Demand from "+model+" natural join regions where TimeAndDate BETWEEN '" + startDate+ "' AND '" + endDate + "' Group by State, TimeAndDate")

def createQueryToDataframeYearState(model, startDate, endDate):
    return spark.sql("SELECT State, CAST(CONCAT(Year(Date)) as timestamp) TimeAndDate, AVG(Demand) Demand from (SELECT State, Demand, TimeAndDate as Date from "+model+" natural join regions where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"')  GROUP BY Year(Date), State")#.repartition(1).write.csv("data/AggragatedToYears.csv")

def createQueryToDataframeMonthState(model, startDate, endDate):
    return spark.sql("SELECT State, CAST(CONCAT(Year(Date), '-', Month(Date)) as timestamp) TimeAndDate, AVG(Demand) Demand from (SELECT State, Demand, TimeAndDate as Date from "+model+" natural join regions where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY Month(Date),Year(Date), State" )#.repartition(1).write.csv("data/AggragatedToMonths.csv")

def createQueryToDataframeDayState(model, startDate, endDate):
    return spark.sql("SELECT State, Date as TimeAndDate, AVG(Demand) Demand from (SELECT State, Demand, DATE(TimeAndDate) as Date from "+model+" natural join regions where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY TimeAndDate, State" )

def createQueryToDataframeWeekState(model, startDate, endDate):
    return spark.sql("SELECT State, CONCAT(Year(Date),'-',weekofyear(Date)) TimeAndDate ,AVG(Demand) Demand from (SELECT State, Demand, TimeAndDate as Date from "+model+" natural join regions where Demand IS NOT NULL AND TimeAndDate BETWEEN '"+startDate+"' AND '"+endDate+"') GROUP BY weekofyear(Date), Year(Date), State" )#.repartition(1).write.csv("data/AggragatedToWeeks.csv")

def createDataframeBA(timeUnit, model, startDate, endDate):
    if(timeUnit=="h"): return createQueryToDataframeHourBA(model, startDate, endDate)
    elif(timeUnit=="d"): return createQueryToDataframeDayBA(model, startDate, endDate)
    elif(timeUnit=="w"): return createQueryToDataframeWeekBA(model, startDate, endDate)
    elif(timeUnit=="m"): return createQueryToDataframeMonthBA(model, startDate, endDate)
    elif(timeUnit=="y"): return createQueryToDataframeYearBA(model, startDate, endDate)
    print("invalid timeunit given in createDataFrame. %s"%timeUnit)
    return

def createDataframeRegion(timeUnit, model, startDate, endDate):
    if(timeUnit=="h"): return createQueryToDataframeHourRegion(model, startDate, endDate)
    elif(timeUnit=="d"): return createQueryToDataframeDayRegion(model, startDate, endDate)
    elif(timeUnit=="w"): return createQueryToDataframeWeekRegion(model, startDate, endDate)
    elif(timeUnit=="m"): return createQueryToDataframeMonthRegion(model, startDate, endDate)
    elif(timeUnit=="y"): return createQueryToDataframeYearRegion(model, startDate, endDate)
    print("invalid timeunit given in createDataFrame. %s"%timeUnit)
    return

def createDataframeState(timeUnit, model, startDate, endDate):
    if(timeUnit=="h"): return createQueryToDataframeHourState(model, startDate, endDate)
    elif(timeUnit=="d"): return createQueryToDataframeDayState(model, startDate, endDate)
    elif(timeUnit=="w"): return createQueryToDataframeWeekState(model, startDate, endDate)
    elif(timeUnit=="m"): return createQueryToDataframeMonthState(model, startDate, endDate)
    elif(timeUnit=="y"): return createQueryToDataframeYearState(model, startDate, endDate)
    print("invalid timeunit given in createDataFrame. %s"%timeUnit)
    return

#Turn a dataframe containing TimeAndDate, Demand as well as BA into a Json and compress's it using zlib
#Takes perams timeUnit("d","w","h","m","y"), model("demands","forecasts")
def turnDataframeIntoJson(ByBaOrStateOrRegion, timeUnit, model, startDate, endDate):
    if(ByBaOrStateOrRegion=="B"):
        df = createDataframeBA(timeUnit,model,startDate,endDate).rdd
        tcp_interactions_out = df.map(lambda df: '{\"BA\":\"%s\",\"TimeAndDate\":\"%s\",\"Demand\":%s}' % (df.BA, df.TimeAndDate, df.Demand))
        temp = tcp_interactions_out.collect()
        sendString=''
        for temp2 in temp:
            sendString += temp2+'\n'
        return sendString
    elif ByBaOrStateOrRegion == "R":
        df = createDataframeRegion(timeUnit,model,startDate,endDate).rdd
        tcp_interactions_out = df.map(lambda df: '{\"Region\":\"%s\",\"TimeAndDate\":\"%s\",\"Demand\":%s}' % (df.Region, df.TimeAndDate, df.Demand))
        temp = tcp_interactions_out.collect()
        sendString=''
        for temp2 in temp:
            sendString += temp2+'\n'
        return sendString
    elif ByBaOrStateOrRegion == "S":
        df = createDataframeState(timeUnit,model,startDate,endDate).rdd
        tcp_interactions_out = df.map(lambda df: '{\"State\":\"%s\",\"TimeAndDate\":\"%s\",\"Demand\":%s}' % (df.State, df.TimeAndDate, df.Demand))
        temp = tcp_interactions_out.collect()
        sendString=''
        for temp2 in temp:
            sendString += temp2+'\n'
        #compressedJson = zlib.compress(sendString.encode())
        return sendString
    
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
print(turnDataframeIntoJson("S","h","demands","2014","2017"))
#date_table.printSchema()
#temp1 = getOutliersImproved()[1].show(5)
#temp2 = getMissingDates()[1].show(5)



# Ensure previous spark context has closed (Will fix this)
sc.stop() 
