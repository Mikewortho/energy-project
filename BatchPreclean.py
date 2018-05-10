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
    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max , Min(TimeAndDate) min, percentile_approx(Demand, 0.5) Median from demands GROUP BY BA order by BA").collect()
    maturity_udf = udf(lambda age,age2: (abs(float(age)-float(age2))), DoubleType())
#    fSubset = open('newEntries.csv', 'w', newline="")
#    writer = csv.writer(fSubset)
#    writer.writerows([["BA","TimeAndDate","Demand"]])
    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True),StructField("Demand", DoubleType())]
    schema = StructType(field)
    df10 = spark.createDataFrame([],schema).coalesce(1)
    for currentBA in range(len(dftemp)):
        print("%d,%d"%(currentBA, len(dftemp)))
        BA = dftemp[currentBA][0]
        Median = dftemp[currentBA][3]
        df = spark.sql("select * from demands where Demand > 0 and BA = '"+BA+"'")
        df3 = df.withColumn("Difference", maturity_udf(df.Demand,lit(Median)))
        test2 = df3.stat.approxQuantile("Difference", [0.5], 0)[0]
        lowerBound = max(Median - (5 * test2),1)
        upperBound = Median + (15 * test2)
        temp2 = spark.sql("Select BA, TimeAndDate, Demand from demands where (Demand>="+str(lowerBound)+") and (Demand<="+str(upperBound)+") and BA='"+BA+"' ")
        df10 = df10.unionAll(temp2)
    return df10

conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "8g")
sc = SparkContext.getOrCreate(conf=conf)
spark = SQLContext(sc)
demand_table = spark.read.csv("data/newRows.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:00:00+00:00").select("BA","TimeAndDate","Demand")
if demand_table.count() != 0:
    if demand_table.dtypes[2][1] == "string":
        demand_table = demand_table.select("BA","TimeAndDate","Demand").filter("Demand!='None'")
    else:
        demand_table = demand_table.select("BA","TimeAndDate","Demand")
    if(demand_table.count() == 0):
        with open('data/newRows.csv', 'w', newline="") as f:
            pass
    else:
        demand_table.write.saveAsTable("demands")
        spark.cacheTable('demands')
        dates = spark.sql("select min(TimeAndDate), max(TimeAndDate) from demands").collect()[0]
        dataframe = spark.createDataFrame(pd.DataFrame(pd.date_range(dates[0], dates[1], freq="H", tz = 'UTC')),["TimeAndDate"]).repartition(8).registerTempTable("dates")
        spark.cacheTable('dates')
        getOutliersImproved2().write.saveAsTable("demandsOut3")
        spark.sql("select * from demandsOut3").coalesce(1).write.option("header", "true").csv('myfile1',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
        for file in glob.glob('myfile1/part-00000-*.csv'):
            shutil.move(file, 'data/removedData.csv')
        shutil.rmtree('myfile1')
        spark.read.csv("data/removedData.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:00:00+00:00").write.saveAsTable("demandsOut2")
        spark.sql("SELECT x.BA, x.TimeAndDate, x.Demand, y.AvgDemands, y.std FROM demandsOut2 x JOIN (SELECT p.BA, Min(TimeAndDate) AS maxDate, AVG(Demand) AvgDemands, STD(Demand) std FROM demandsOut2 p GROUP BY p.BA) y ON y.BA = x.BA AND y.maxDate = x.TimeAndDate GROUP BY x.BA, x.TimeAndDate, x.Demand, y.AvgDemands, y.std").coalesce(1).write.option("header", "true").csv('myfile2',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
        for file in glob.glob('myfile2/part-00000-*.csv'):
            shutil.move(file, 'data/latestAndEarliest.csv')
        shutil.rmtree('myfile2')
        spark.sql('drop table demands')
        spark.sql('drop table dates')
        spark.sql('drop table demandsOut2')
        spark.sql('drop table demandsOut3')
        os.remove("data/removedData.csv")
sc.stop()