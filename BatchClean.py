#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 10 10:22:23 2018

@author: robertjohnson
"""
import glob, shutil
from pyspark.sql import SQLContext       
from pyspark.sql.types import TimestampType, StringType, StructType, StructField, DoubleType
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import last, col, when
from pyspark.sql.window import Window
import sys
import pandas as pd
import requests, json, csv, datetime
import time
import os
import pickle
import numpy as np
import statsmodels.api as sm

def getOutliersImproved2():
    BAStats = spark.sql("select A.BA, A.max, A.min, A.Median, B.AvgDemands, B.std from ((Select BA, MAX(TimeAndDate) max , Min(TimeAndDate) min, percentile_approx(Demand, 0.5) Median from demands GROUP BY BA order by BA) as A left join (select * from latestAndEarliest) as B on (A.BA = B.BA))").collect()
    # create empty dataframe
    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True),StructField("Demand", DoubleType())]
    schema = StructType(field)
    df10 = spark.createDataFrame([],schema).coalesce(1)
    # for each BA
    for currentBA in range(len(BAStats)):
        # set the flag if we need to use new avgStd if we have never seen this BA before and get all statistics.
        flag = False
        print('\r', 'Creation of query for (', (currentBA+1),  '/',len(BAStats),') Ba\'s' , end="")
        BA = BAStats[currentBA][0]
        ENDTIME = BAStats[currentBA][1]
        STARTTIME = BAStats[currentBA][2]
        Avg = BAStats[currentBA][4]
        Std = BAStats[currentBA][5]
        # if we have no avg value then set the flag to be true and work out the new std and avg from the current records.
        if Avg == None:
            flag = True
            print(BA)
            avgStd = spark.sql("select avg(Demand), std(Demand) from demands where BA ='"+BA+"'").collect()
            Avg = avgStd[0][0]
            Std = avgStd[0][1]
        # set the lowerbound and upperbound for demand and set the records that do not fit this range to null then fill the missing values.
        lowerBound = max(Avg - (4 * Std),1)
        upperBound = Avg + (10 * Std)
        recordsWithCalender = spark.sql("Select '"+BA+"' as BA, A.TimeAndDate as TimeAndDate, L.Demand as Demand from (Select TimeAndDate from dates where TimeAndDate between '"+str(STARTTIME)+"' AND '"+str(ENDTIME)+"') as A full OUTER JOIN (Select BA, TimeAndDate, Demand from demands where BA='"+BA+"' ) AS L on (L.TimeAndDate==A.TimeAndDate) order by A.TimeAndDate")
        recordsWithCalenderAndCorrectDemands = recordsWithCalender.withColumn("Demand",when((col("Demand")>=lowerBound)&(col("Demand")<=upperBound),col("Demand")))
        w = (Window().orderBy(col("TimeAndDate").cast("timestamp").cast("long")).rangeBetween(-sys.maxsize, 0))
        recordsWithCalenderAndCorrectDemands = recordsWithCalenderAndCorrectDemands.withColumn("Demand", when(col("Demand").isNotNull(),col("Demand")).otherwise(last("Demand",True).over(w)))
        if flag == True:
            Avg = spark.sql("select avg(Demand) from demands where Demand >= "+str(lowerBound)+" and Demand <= "+str(upperBound)+" and BA = '"+BA+"'").collect()[0][0]
        recordsWithCalenderAndCorrectDemands = recordsWithCalenderAndCorrectDemands.fillna({'Demand':Avg})
        df10 = df10.unionAll(recordsWithCalenderAndCorrectDemands)
    return df10

conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "8g")
sc = SparkContext.getOrCreate(conf=conf)
spark = SQLContext(sc)
# create a dataframe of our new rows 2018-05-07 14:00:00+00:00
demand_table = spark.read.csv("data/newRows.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:00:00+00:00").select("BA","TimeAndDate","Demand")
# if there is new records
if demand_table.dtypes[2][1] == "string":
    demand_table = demand_table.select("BA","TimeAndDate","Demand").filter("Demand!='None'")
else:
    demand_table = demand_table.select("BA","TimeAndDate","Demand")
# if we now have no rows then clean out new rows and restart
howManyNewRows = demand_table.count()
if(demand_table.count() == 0):
    with open('data/newRows.csv', 'w', newline="") as f:
        writer = csv.writer(f)
        writer.writerows([["BA","TimeAndDate","Demand"]])
        f.flush()
        os.fsync(f)
else:
    # create the union of our new rows and our old latest records and create a table and cache it.
    demand_table.unionAll(spark.read.csv("data/latestAndEarliest.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00").select("BA","TimeAndDate","Demand")).write.partitionBy("BA").saveAsTable("demands")
    spark.cacheTable('demands')
    # read the old latest dates and get the mean and sd of each ba and cache it.
    latestDates = spark.read.csv("data/latestAndEarliest.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    latestDates.write.saveAsTable("latestAndEarliest")
    spark.cacheTable('latestAndEarliest')
    # get the clean data and write it to a table
    if(os.path.exists("data/elec_demand_hourlyClean.csv")==False):
        with open('data/elec_demand_hourlyClean.csv', 'w', newline="") as f:
            writer = csv.writer(f)
            writer.writerows([["BA","Demand","Hour","Day","Month","Year","Weekday","TimeAndDate"]])
            f.flush()
            os.fsync(f) 
    cleanData = spark.read.csv("data/elec_demand_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    cleanData.write.saveAsTable("demandsClean")
    # create calender dataframe from the starting and end latest dates and cache it.
    dates = spark.sql("select min(TimeAndDate), max(TimeAndDate) from demands").collect()[0]
    dataframe = spark.createDataFrame(pd.DataFrame(pd.date_range(dates[0], dates[1], freq="H", tz = 'UTC')),["TimeAndDate"]).repartition(8).registerTempTable("dates")
    spark.cacheTable('dates')
    # get the missing records and records that are incorrect and approximate them then set it to our cleaned new records table called demandsOut
    getOutliersImproved2().unionAll(spark.read.csv("data/elec_demand_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00").select("BA","TimeAndDate","Demand")).write.saveAsTable("demandsOut")
    spark.cacheTable('demandsOut')
    # unpersist the old tables of dates and old records
    spark.sql("select * from demands").unpersist(True)
    spark.sql("select * from dates").unpersist(True)
    # write the new records to file.
    spark.sql("select BA, Avg(Demand) Demand, Hour(TimeAndDate) Hour, Day(TimeAndDate) Day, Month(TimeAndDate) Month, Year(TimeAndDate) Year, weekofyear(TimeAndDate) Weekday, TimeAndDate from demandsOut group by BA, TimeAndDate order by BA,TimeAndDate").coalesce(1).write.option("header", "true").csv('myfile',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    for file in glob.glob('myfile/part-00000-*.csv'):
        shutil.move(file, 'data/elec_demand_hourlyClean.csv')
    shutil.rmtree('myfile')
    # read the new records and then output the new sd and avg values to file.
    demand_table = spark.read.csv("data/elec_demand_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    demand_table.write.saveAsTable("demandsOut2")
    spark.sql("SELECT x.BA, x.TimeAndDate, x.Demand, y.AvgDemands, y.std FROM demandsOut2 x JOIN (SELECT p.BA, MAX(TimeAndDate) AS maxDate, AVG(Demand) AvgDemands, STD(Demand) std FROM demandsOut2 p GROUP BY p.BA) y ON y.BA = x.BA AND y.maxDate = x.TimeAndDate GROUP BY x.BA, x.TimeAndDate, x.Demand, y.AvgDemands, y.std").coalesce(1).write.option("header", "true").csv('myfile2',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    for file in glob.glob('myfile2/part-00000-*.csv'):
        shutil.move(file, 'data/latestAndEarliest.csv')
    shutil.rmtree('myfile2')
    with open('data/newRows.csv', 'w', newline="") as f:
        writer = csv.writer(f)
        writer.writerows([["BA","TimeAndDate","Demand"]])
        f.flush()
        os.fsync(f)
    # drop all tables.
    spark.sql('drop table demands')
    spark.sql('drop table demandsClean')
    spark.sql('drop table dates')
    spark.sql('drop table demandsOut')
    spark.sql('drop table demandsOut2')
    spark.sql('drop table latestAndEarliest')
sc.stop()