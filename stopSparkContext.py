#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 20:26:11 2018

@author: robertjohnson
"""

from pyspark.sql import SQLContext       
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "2g")
sc = SparkContext.getOrCreate(conf=conf)
spark = SQLContext(sc)
#
##
#demand_table = spark.read.csv("weather.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd'T'HH:mm:ss.000Z")
#demand_table.registerTempTable("weatherUpToDate")
#
#demand_table = spark.read.csv("data/region_table.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd'T'HH:mm:ss.000Z")
#demand_table.registerTempTable("regions")
#spark.sql("select A.TimeAndDate,A.wind_direction,A.wind_speed,A.temperature,A.temperature_dewpoint,A.air_pressure,B.BA from (select * from weatherUpToDate) as A inner join (select * from regions) as B on (A.state==B.State) order by A.TimeAndDate").coalesce(1).write.option("header", "true").csv('myfile123',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
#
##        
#demand_table = spark.read.csv("data/elec_demand_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
#demand_table.registerTempTable("demandsClean")
#spark.sql("select A.BA,A.max,A.min,B.AvgDemands,B.std,A.squareVal, A.sum, Ceil((A.max-B.AvgDemands)/B.std) Upper ,Ceil((B.AvgDemands-A.min)/B.std) Lower  from ((select BA, max(Demand) max , min(Demand) min, sum(Demand*Demand) as squareVal, sum(Demand) sum from demandsClean group by BA) as A left join (select * from latestAndEarliest) as B on (A.BA == B.BA))").show(60)
sc.stop()