#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  9 14:51:30 2018

@author: robertjohnson
"""


import glob, shutil
from pyspark.sql import SQLContext       
from pyspark.sql.types import TimestampType, StringType, StructType, StructField, DoubleType
from pyspark import SparkContext, SparkConf
import pandas as pd
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
import os

def getOutliersImproved2():
    # get the earliest and latest date for each BA along with its median value
    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max , Min(TimeAndDate) min, percentile_approx(Demand, 0.5) Median from demands GROUP BY BA order by BA").collect()
    # use custom udf to work out the distance to the median
    maturity_udf = udf(lambda age,age2: (abs(float(age)-float(age2))), DoubleType())
    # create an empty dataframe
    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True),StructField("Demand", DoubleType())]
    schema = StructType(field)
    df10 = spark.createDataFrame([],schema).coalesce(1)
    # for each BA
    for currentBA in range(len(dftemp)):
        print("%d,%d"%(currentBA, len(dftemp)))
        BA = dftemp[currentBA][0]
        Median = dftemp[currentBA][3]
        # get the records where the demand is above 0
        df = spark.sql("select * from demands where Demand > 0 and BA = '"+BA+"'")
        # work out the median difference i.e absolute median deviation
        df3 = df.withColumn("Difference", maturity_udf(df.Demand,lit(Median)))
        test2 = df3.stat.approxQuantile("Difference", [0.5], 0)[0]
        # create upper and lower bounds
        lowerBound = max(Median - (5 * test2),1)
        upperBound = Median + (15 * test2)
        #remove outliers
        temp2 = spark.sql("Select BA, TimeAndDate, Demand from demands where (Demand>="+str(lowerBound)+") and (Demand<="+str(upperBound)+") and BA='"+BA+"' ")
        df10 = df10.unionAll(temp2)
    return df10

# start spark context
conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "8g")
sc = SparkContext.getOrCreate(conf=conf)
spark = SQLContext(sc)
# read any new rows
demand_table = spark.read.csv("data/newRows.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:00:00+00:00").select("BA","TimeAndDate","Demand")
# if there are new rows
if demand_table.count() != 0:
    # check if the demand column contains a "None" entry
    if demand_table.dtypes[2][1] == "string":
        demand_table = demand_table.select("BA","TimeAndDate","Demand").filter("Demand!='None'")
    else:
        demand_table = demand_table.select("BA","TimeAndDate","Demand")
        # check that there are still new rows.
    if(demand_table.count() == 0):
        with open('data/newRows.csv', 'w', newline="") as f:
            pass
    else:
        # create a table of the demand data
        demand_table.write.saveAsTable("demands")
        # cache it
        spark.cacheTable('demands')
        # get the earliest and latest date within the new records
        dates = spark.sql("select min(TimeAndDate), max(TimeAndDate) from demands").collect()[0]
        # create a pandas dataframe for the calender
        dataframe = spark.createDataFrame(pd.DataFrame(pd.date_range(dates[0], dates[1], freq="H", tz = 'UTC')),["TimeAndDate"]).repartition(8).registerTempTable("dates")
        # cache the table of dates
        spark.cacheTable('dates')
        # work out mean and sd values
        getOutliersImproved2().write.saveAsTable("demandsOut3")
        # wrtie it out to file
        spark.sql("select * from demandsOut3").coalesce(1).write.option("header", "true").csv('myfile1',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
        # clean up spark files
        for file in glob.glob('myfile1/part-00000-*.csv'):
            shutil.move(file, 'data/removedData.csv')
        shutil.rmtree('myfile1')
        # read it again
        spark.read.csv("data/removedData.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:00:00+00:00").write.saveAsTable("demandsOut2")
        # output to latest and earliest
        spark.sql("SELECT x.BA, x.TimeAndDate, x.Demand, y.AvgDemands, y.std FROM demandsOut2 x JOIN (SELECT p.BA, Min(TimeAndDate) AS maxDate, AVG(Demand) AvgDemands, STD(Demand) std FROM demandsOut2 p GROUP BY p.BA) y ON y.BA = x.BA AND y.maxDate = x.TimeAndDate GROUP BY x.BA, x.TimeAndDate, x.Demand, y.AvgDemands, y.std").coalesce(1).write.option("header", "true").csv('myfile2',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
        for file in glob.glob('myfile2/part-00000-*.csv'):
            shutil.move(file, 'data/latestAndEarliest.csv')
        shutil.rmtree('myfile2')
        # clean up tables.
        spark.sql('drop table demands')
        spark.sql('drop table dates')
        spark.sql('drop table demandsOut2')
        spark.sql('drop table demandsOut3')
        os.remove("data/removedData.csv")
# top spark context
sc.stop()