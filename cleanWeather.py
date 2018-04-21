#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 20:31:37 2018

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
import math
from datetime import datetime
from datetime import timedelta
from pyspark.sql.functions import udf


def getOutliersImproved2():
    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max , Min(TimeAndDate) min from Weather GROUP BY BA order by BA").collect()
    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True),StructField("wind_direction", DoubleType()),StructField("wind_speed", DoubleType()),StructField("temperature", DoubleType()),StructField("temperature_dewpoint", DoubleType()),StructField("air_pressure", DoubleType())]
    schema = StructType(field)
    df10 = spark.createDataFrame([],schema).repartition(8)
    for currentBA in range(len(dftemp)):
        BA = dftemp[currentBA][0]
        print("%d,%d%s"%(currentBA,len(dftemp),BA))
        ENDTIME = dftemp[currentBA][1]
        STARTTIME = dftemp[currentBA][2]
        BAdataframe = spark.sql("Select '"+BA+"' as BA, A.TimeAndDate, L.wind_direction, L.wind_speed, L.temperature, L.temperature_dewpoint, L.air_pressure from (Select * from dates where TimeAndDate between '"+str(STARTTIME)+"' AND '"+str(ENDTIME)+"') as A full OUTER JOIN (Select * from Weather where BA='"+BA+"' ) AS L on (L.TimeAndDate==A.TimeAndDate)").repartition(8)
        w = (Window().orderBy(col("TimeAndDate").cast("timestamp").cast("long")).rangeBetween(-sys.maxsize, 0))
        BAdataframe = BAdataframe.withColumn("wind_direction", when(col("wind_direction").isNotNull(),col("wind_direction")).otherwise(last("wind_direction",True).over(w)))
        BAdataframe = BAdataframe.withColumn("wind_speed", when(col("wind_speed").isNotNull(),col("wind_speed")).otherwise(last("wind_speed",True).over(w)))
        BAdataframe = BAdataframe.withColumn("temperature", when(col("temperature").isNotNull(),col("temperature")).otherwise(last("temperature",True).over(w)))
        BAdataframe = BAdataframe.withColumn("temperature_dewpoint", when(col("temperature_dewpoint").isNotNull(),col("temperature_dewpoint")).otherwise(last("temperature_dewpoint",True).over(w)))
        BAdataframe = BAdataframe.withColumn("air_pressure", when(col("air_pressure").isNotNull(),col("air_pressure")).otherwise(last("air_pressure",True).over(w)))
        BAdataframe = BAdataframe.fillna({'wind_direction':0})
        BAdataframe = BAdataframe.fillna({'wind_speed':0})
        BAdataframe = BAdataframe.fillna({'temperature':0})
        BAdataframe = BAdataframe.fillna({'temperature_dewpoint':0})
        BAdataframe = BAdataframe.fillna({'air_pressure':0})
        df10 = df10.unionAll(BAdataframe)
    return df10

def maturity_udf2(date,time):
    year = str(date)[0:4]
    month = str(date)[4:6]
    day = str(date)[6:8]
    hours = str(math.floor((time)/100))
    minuites = round((time%100)/60)*3600
    date = datetime.strptime(year+month+day+hours, '%Y%m%d%H')
    date = date + timedelta(seconds=minuites)
    return date


conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "12g")
sc = SparkContext.getOrCreate(conf=conf)
spark = SQLContext(sc)
newWeather = spark.read.csv("data/newWeather.csv", header=True, inferSchema=True)
#"","usaf_station","date","time","wind_direction","wind_speed","temperature","temperature_dewpoint","air_pressure","State"
if newWeather.count() != 0:
    newWeather = newWeather.select("State","date","time","wind_direction","wind_speed","temperature","temperature_dewpoint","air_pressure")
    square_udf_int = udf(lambda x, y: maturity_udf2(x, y), TimestampType())
    newWeather = newWeather.withColumn('TimeAndDate', square_udf_int(newWeather['date'],newWeather['time'])).select("State","TimeAndDate","wind_direction","wind_speed","temperature","temperature_dewpoint","air_pressure")
    newWeather = newWeather.withColumn("wind_direction",when((col("wind_direction")>0) & (col("wind_direction")<=360),col("wind_direction")))
    newWeather = newWeather.withColumn("wind_speed",when((col("wind_speed")>0) & (col("wind_speed")<=353),col("wind_speed")))
    newWeather = newWeather.withColumn("temperature",when((col("temperature")>-992)&(col("temperature")<=760),col("temperature")))
    newWeather = newWeather.withColumn("temperature_dewpoint",when((col("temperature_dewpoint")>-300)&(col("temperature_dewpoint")<=350),col("temperature_dewpoint")))
    newWeather.withColumn("air_pressure",when((col("air_pressure")>8700)&(col("air_pressure")<=10850),col("air_pressure"))).registerTempTable("WeatherTemp")
    newWeather = spark.sql("select State, TimeAndDate, avg(wind_direction) wind_direction, avg(wind_speed) wind_speed, avg(temperature) temperature, avg(temperature_dewpoint) temperature_dewpoint, avg(air_pressure) air_pressure from WeatherTemp group by State, TimeAndDate").registerTempTable("WeatherTemp")
    region = spark.read.csv("data/region_table.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd'T'HH:mm:ss.000Z")
    region.registerTempTable("regions")
    newWeather = spark.sql("select B.BA, A.TimeAndDate,A.wind_direction,A.wind_speed,A.temperature,A.temperature_dewpoint,A.air_pressure from (select * from WeatherTemp) as A inner join (select * from regions) as B on (A.state==B.State)")
    newWeather.unionAll(spark.read.csv("data/latestAndEarliestWeather.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00").select("BA","TimeAndDate","wind_direction","wind_speed","temperature","temperature_dewpoint","air_pressure")).write.partitionBy("BA").saveAsTable("Weather")
    spark.cacheTable('Weather')
    oldDocs = spark.read.csv("data/latestAndEarliestWeather.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    oldDocs.write.saveAsTable("latestAndEarliestWeather")
    spark.cacheTable('latestAndEarliestWeather')
    oldWeather = spark.read.csv("data/elec_weather_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    oldWeather.write.saveAsTable("weatherClean")
    dates = spark.sql("select min(TimeAndDate), max(TimeAndDate) from Weather").collect()[0]
    print(spark.sql("select * from Weather").schema)
    dataframe = spark.createDataFrame(pd.DataFrame(pd.date_range(dates[0], dates[1], freq="H", tz = 'UTC')),["TimeAndDate"]).write.saveAsTable("dates")
    getOutliersImproved2().unionAll(spark.read.csv("data/elec_weather_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")).write.saveAsTable("weatherOut")
    spark.cacheTable('weatherOut')
    spark.sql("select BA, TimeAndDate,Hour(TimeAndDate) Hour, Day(TimeAndDate) Day, Month(TimeAndDate) Month, Year(TimeAndDate) Year, weekofyear(TimeAndDate) Weekday, avg(wind_direction) wind_direction, avg(wind_speed) wind_speed, avg(temperature) temperature, avg(temperature_dewpoint) temperature_dewpoint, avg(air_pressure) air_pressure from weatherOut group by BA, TimeAndDate").coalesce(1).write.option("header", "true").csv('weatherStorage',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    for file in glob.glob('weatherStorage/part-00000-*.csv'):
        shutil.move(file, 'Data/elec_weather_hourlyClean.csv')
    shutil.rmtree('weatherStorage')
    weatherData = spark.read.csv("weatherData/weather.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    weatherData.write.saveAsTable("demandsOut2")
    spark.sql("SELECT x.BA, x.TimeAndDate, x.wind_direction, x.wind_speed, x.temperature, x.temperature_dewpoint, x.air_pressure FROM demandsOut2 x JOIN (SELECT p.BA, MAX(TimeAndDate) AS maxDate FROM demandsOut2 p GROUP BY p.BA) y ON y.BA = x.BA AND y.maxDate = x.TimeAndDate GROUP BY x.BA, x.TimeAndDate, x.wind_direction, x.wind_speed, x.temperature, x.temperature_dewpoint, x.air_pressure").coalesce(1).write.option("header", "true").csv('weatherStorage2',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
    for file in glob.glob('weatherStorage2/part-00000-*.csv'):
        shutil.move(file, 'Data/latestAndEarliestWeather.csv')
    shutil.rmtree('weatherStorage2')
    with open('data/newWeather.csv', 'w', newline="") as f:
        f.write([["State","date","time","wind_direction","wind_speed","temperature","temperature_dewpoint","air_pressure"]])
    
    
    
    
#    spark.sql('drop table demands')
#    spark.sql('drop table demandsClean')
#    spark.sql('drop table dates')
#    spark.sql('drop table demandsOut')
#    spark.sql('drop table demandsOut2')
#    spark.sql('drop table latestAndEarliest')
    
#    spark.cacheTable('demandsOut')
#    spark.sql("select * from demands").unpersist(True)
#    spark.sql("select * from dates").unpersist(True)
#    spark.sql("select BA, Avg(Demand) Demand, Hour(TimeAndDate) Hour, Day(TimeAndDate) Day, Month(TimeAndDate) Month, Year(TimeAndDate) Year, weekofyear(TimeAndDate) Weekday, TimeAndDate from demandsOut group by BA, TimeAndDate order by BA").coalesce(1).write.option("header", "true").csv('myfile',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
#    for file in glob.glob('myfile/part-00000-*.csv'):
#        shutil.move(file, 'data/elec_demand_hourlyClean.csv')
#    shutil.rmtree('myfile')
#    demand_table = spark.read.csv("data/elec_demand_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
#    demand_table.write.saveAsTable("demandsOut2")
#    spark.sql("SELECT x.BA, x.TimeAndDate, x.Demand, y.AvgDemands, y.std FROM demandsOut2 x JOIN (SELECT p.BA, MAX(TimeAndDate) AS maxDate, AVG(Demand) AvgDemands, STD(Demand) std FROM demandsOut2 p GROUP BY p.BA) y ON y.BA = x.BA AND y.maxDate = x.TimeAndDate GROUP BY x.BA, x.TimeAndDate, x.Demand, y.AvgDemands, y.std").coalesce(1).write.option("header", "true").csv('myfile2',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
#    for file in glob.glob('myfile2/part-00000-*.csv'):
#        shutil.move(file, 'data/latestAndEarliest.csv')
#    shutil.rmtree('myfile2')
#    with open('data/newRows.csv', 'w', newline="") as f:
#        pass
#    spark.sql('drop table Weather')
#    spark.sql('drop table latestAndEarliestWeather')
#    spark.sql('drop table weatherClean')
#sc.stop()
