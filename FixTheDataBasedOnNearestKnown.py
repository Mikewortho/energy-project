#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 17:08:53 2018

@author: robertjohnson
"""
from pyspark.sql.functions import lit
import pyspark
from pyspark.sql import SQLContext       
from pyspark.sql.types import TimestampType
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.functions import udf
from datetime import timedelta
import csv
# Union method.
# Method to get missing dates for each BA, returns a list of format [[Dataframe(of missing dates for BA), BA],[Dataframe(of missing dates for BA), BA]......]
def getMissingDates():
    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max ,Min(TimeAndDate) min from demands GROUP BY BA")
    sample2 = dftemp.collect()
    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True)]
    schema = StructType(field)
    df = spark.createDataFrame([],schema)
    for BA, ENDTIME, STARTTIME in sample2:
        df = df.unionAll(spark.sql("Select '"+BA+"' as BA, TimeAndDate from (Select * from dates where TimeAndDate<'"+str(ENDTIME)+"' AND TimeAndDate>'"+str(STARTTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where BA='"+BA+"') AS L WHERE BA IS NULL "))
    return df

def getOutliersImproved():
    dftemp = spark.sql("Select BA, MEAN(Demand) Mean , STD(Demand) STD from demands GROUP BY BA").collect()
    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True)]
    schema = StructType(field)
    df = spark.createDataFrame([],schema)
    for BA, Mean, STD in dftemp:
        lower_bounds = Mean - 5 * STD
        upper_bounds = Mean + 5 * STD
        df = df.unionAll(spark.sql("Select BA, TimeAndDate from (Select * from demands where (demand > {0} OR demand < {1} OR demand < 0 OR demand = 0 or demand = 'None') and BA = '{2}')".format(upper_bounds, lower_bounds, BA)))
    return df





print("hello")

sc = pyspark.SparkContext.getOrCreate()
spark = SQLContext(sc) 
# Use sparkSQL to read in CSV
demand_table = spark.read.csv("data/elec_demand_hourly.csv", header=True, inferSchema=True)
date_table = spark.read.csv("data/dates.csv", header=True, inferSchema=True)
demand_table.registerTempTable("demands")
date_table.registerTempTable("dates")
ResetFromFresh = False
if ResetFromFresh == True:
    print("Dont use this unless your Robert.")
#    ---------------------------------Do this, then move the files (outliers, missingDates, missing Entries to the Data folder) [ROBERTS USE ONLY.]
#    getOutliersImproved().registerTempTable("outliers")
#    spark.sql("select * from outliers").coalesce(1).write.option("header", "true").save(path='outliers', format='csv', mode='append', sep=',')
#    df = getMissingDates()
#    df.coalesce(1).write.option("header", "true").save(path='missingDates', format='csv', mode='append', sep=',')
#    df.registerTempTable("missingDates")
#    spark.sql("select * from missingDates").unionAll(spark.sql("select * from outliers")).registerTempTable("missingEntries")
#    spark.sql("select * from missingEntries").coalesce(1).write.option("header", "true").save(path='missingEntries', format='csv', mode='append', sep=',')
#    spark.sql("select * from missingEntries where BA == 'FPL' order by TimeAndDate asc").show(5)
#    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max ,Min(TimeAndDate) min from demands GROUP BY BA")
#    sample2 = dftemp.collect()


#    ---------------------------------Then do this [ROBERTS USE ONLY.]
#    missingDates = spark.read.csv("data/missingDates.csv", header=True, inferSchema=True)
#    missingDates.registerTempTable("missingDates")
#    outliers = spark.read.csv("data/outliers.csv", header=True, inferSchema=True)
#    outliers.registerTempTable("outliers")
#    spark.sql("SELECT demands.* FROM demands LEFT JOIN outliers ON (demands.BA = outliers.BA and demands.TimeAndDate = outliers.TimeAndDate) WHERE outliers.TimeAndDate IS NULL").registerTempTable("demands")
#    missingEntries = spark.read.csv("data/missingEntries.csv", header=True, inferSchema=True)
#    missingEntries.registerTempTable("missingEntries")
#    orderedMissingEntries = spark.sql("select * from (select * from missingEntries order by TimeAndDate) order by BA").collect()
#    AvgDemandForBA = spark.sql("select BA, avg(Demand) Demand from (select BA, Demand from Demands order by BA) group by BA").collect()
#    df = spark.sql("Select * from missingEntries")
#    maturity_udf = udf(lambda age,age2: round((abs(age-age2)).total_seconds()), IntegerType())
#    listOfResults = []
#    whatBA = 0
#    previousBA = ""
#    avgOfBA = 0
#    with open('newEntries.csv', 'a', newline="") as f:
#        writer = csv.writer(f)
#        writer.writerows([["BA","TimeAndDate","Demand"]])
#        listOfResults = []
#    for l in range(16081,len(orderedMissingEntries)):
#        if(previousBA == "" or previousBA != orderedMissingEntries[l][0]):
#            previousBA = orderedMissingEntries[l][0]
#            while AvgDemandForBA[whatBA][0] != orderedMissingEntries[l][0]:
#                whatBA += 1
#                if(len(AvgDemandForBA)==whatBA):
#                    whatBA = 0
#            avgOfBA = AvgDemandForBA[whatBA][1]
#            df.unpersist()
#            df = spark.sql("select TimeAndDate, Demand from demands where BA = '%s'" % (orderedMissingEntries[l][0])).cache()
#            print(str(df.count()))
#        UpperBound = orderedMissingEntries[l][1] + timedelta(days=1)
#        LowerBound = orderedMissingEntries[l][1] - timedelta(days=1)
#        df2 = df.filter("TimeAndDate BETWEEN '%s' AND '%s'"%(LowerBound,UpperBound))
#        df3 = df2.withColumn("Difference", maturity_udf(df.TimeAndDate,lit(orderedMissingEntries[l][1])))
#        result = df3.sort(df3.Difference, ascending = True).limit(1)
#        demandAndDifference  = result.select(result.Demand, result.Difference).collect()
#        TimeAndDateString = str(orderedMissingEntries[l][1])
#        Year = TimeAndDateString[0:4]
#        Month = TimeAndDateString[5:7]
#        Day = TimeAndDateString[8:10]
#        Hour = TimeAndDateString[11:13]
#        if(len(demandAndDifference)==0 or len(demandAndDifference[0])==0 or demandAndDifference[0][0]=='None'):
#            listOfResults.append([orderedMissingEntries[l][0],Year+"-"+Month+"-"+Day+"T"+Hour+":00:00",int(avgOfBA)])
#        else:
#            demand = int(demandAndDifference[0][0])
#            difference = demandAndDifference[0][1]
#            demand = ((3000/difference)*demand)+((1-(3000/difference))*avgOfBA)
#            listOfResults.append([orderedMissingEntries[l][0],Year+"-"+Month+"-"+Day+"T"+Hour+":00:00",int(demand)]) 
#        if l%20==0:
#            with open('newEntries.csv', 'a', newline="") as f:
#                writer = csv.writer(f)
#                writer.writerows(listOfResults)
#                listOfResults = []
#            file = open("testfile.txt","w") 
#            file.write(str(l))
#            file.close()
#        print(l)
#    with open('newEntries.csv', 'a', newline="") as f:
#                writer = csv.writer(f)
#                writer.writerows(listOfResults)
#                listOfResults = []
#    file = open("testfile.txt","w") 
#    file.write(str(l))
#    file.close()
else:
#    --------------------------------- Finally do this.
    entries_table = spark.read.csv("data/newEntries.csv", header=True, inferSchema=True)
    entries_table.registerTempTable("entryTable")
    outliers = spark.read.csv("data/outliers.csv", header=True, inferSchema=True)
    outliers.registerTempTable("outliers")
    spark.sql("SELECT demands.* FROM demands LEFT JOIN outliers ON (demands.BA = outliers.BA and demands.TimeAndDate = outliers.TimeAndDate) WHERE outliers.TimeAndDate IS NULL").unionAll(spark.sql("Select BA, Demand, Hour(TimeAndDate) Hour, Day(TimeAndDate) Day, Month(TimeAndDate) Month, Year(TimeAndDate) Year, weekofyear(TimeAndDate) Weekday, TimeAndDate from entryTable")).coalesce(1).write.option("header", "true").save(path='elec_demand_hourly', format='csv', mode='append', sep=',')
sc.stop() 