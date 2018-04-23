#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 17:08:53 2018

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


def getOutliersImproved2():
    dftemp = spark.sql("select A.BA, A.max, A.min, A.Median, B.AvgDemands, B.std from ((Select BA, MAX(TimeAndDate) max , Min(TimeAndDate) min, percentile_approx(Demand, 0.5) Median from demands GROUP BY BA order by BA) as A left join (select * from latestAndEarliest) as B on (A.BA = B.BA))").collect()
    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True),StructField("Demand", DoubleType())]
    schema = StructType(field)
    df10 = spark.createDataFrame([],schema).coalesce(1)
    for currentBA in range(len(dftemp)):
        flag = False
        print("%d,%d"%(currentBA, len(dftemp)))
        BA = dftemp[currentBA][0]
        ENDTIME = dftemp[currentBA][1]
        STARTTIME = dftemp[currentBA][2]
        Avg = dftemp[currentBA][4]
        Std = dftemp[currentBA][5]
        if Avg == None:
            flag = True
            avgStd = spark.sql("select avg(Demand), std(Demand) from demands where BA ='"+BA+"'").collect()
            Avg = avgStd[0][0]
            Std = avgStd[0][1]
        lowerBound = max(Avg - (4 * Std),1)
        upperBound = Avg + (10 * Std)
        temp2 = spark.sql("Select '"+BA+"' as BA, A.TimeAndDate as TimeAndDate, L.Demand as Demand from (Select TimeAndDate from dates where TimeAndDate between '"+str(STARTTIME)+"' AND '"+str(ENDTIME)+"') as A full OUTER JOIN (Select BA, TimeAndDate, Demand from demands where BA='"+BA+"' ) AS L on (L.TimeAndDate==A.TimeAndDate) order by A.TimeAndDate")
        temp2 = temp2.withColumn("Demand",when((col("Demand")>=lowerBound)&(col("Demand")<=upperBound),col("Demand")))
        w = (Window().orderBy(col("TimeAndDate").cast("timestamp").cast("long")).rangeBetween(-sys.maxsize, 0))
        temp2 = temp2.withColumn("Demand", when(col("Demand").isNotNull(),col("Demand")).otherwise(last("Demand",True).over(w)))
        if flag == True:
            Avg = spark.sql("select avg(Demand) from demands where Demand >= "+str(lowerBound)+" and Demand <= "+str(upperBound)+" and BA = '"+BA+"'").collect()[0][0]
        temp2 = temp2.fillna({'Demand':Avg})
        df10 = df10.unionAll(temp2)
    return df10




def append2csv(File_Name,BA_Update):
    with open( File_Name,'a+') as file:
        writer = csv.writer(file, delimiter=',', lineterminator='\n')
        writer.writerows(BA_Update)
        file.flush()
        os.fsync(file)

def csv2panda(name):
     dframe = pd.read_csv(name)
     return dframe


# API keys definition
EIA_API_KEY = '2eb9052e6a0316901fe316a4a5971df1'
WAIT = 5*10
DURATION = 60*60*24

#Get Dictionary of BA'S

request = 'http://api.eia.gov/category/?api_key=' + EIA_API_KEY + '&category_id=2122628'
r = requests.get(request)
x = r.json()
ba, req = ([] for i in range(2))
for i in range(len(x["category"]["childseries"])) : ba.append(json.dumps(x["category"]["childseries"][i]["name"])), req.append(json.dumps(x["category"]["childseries"][i]["series_id"]))
for i in range(len(ba)):
    ba[i] = ba[i][ba[i].find("(")+1:ba[i].find(")")]
d = dict(zip(ba, req))
d.pop('region', None)





# Define APIHandler object
#api_session = APIHandler.APIHandler(EIA_API_KEY)

pre_hour = [[] for i in range(0,56)]
current_hour = []
time_series = [[] for i in range(0,56)]

#pre-hr index
index=0

start = time.time()

df = csv2panda('data/elec_demand_hourly.csv')
row1 = [["BA","TimeAndDate","Demand"]]
append2csv('data/newRows.csv',row1)


counter = 0
updateCounter = 0

while(True):   
    #currenttime index
    cindex = 0
    
    currentTime = time.time()
    if(currentTime-start > WAIT):

        counter+=1
        current_hour = [[] for i in range(0,56) ]
        differenceCounter=0
        now = datetime.datetime.now()


        for ba in d.keys():
            request = 'http://api.eia.gov/series/?api_key=' + EIA_API_KEY + '&series_id=' + d[ba].replace('"', '')
            r = requests.get(request)
            x = r.json()

            if(ba != "EEI" and ba != "WWA"):
                for i in range(len(x["series"][0]["data"])):
                    tempDate = str(x["series"][0]["data"][i][0])
                    year = tempDate[:4]
                    month = tempDate[4:6]
                    day = tempDate[6:8]
                    hour = tempDate[9:11]
                    date = year +"-"+month+"-"+day+"T"+hour+":00:00.000Z"
                    weekday = datetime.date(int(year), int(month), int(day)).weekday()
                    demand = str(x["series"][0]["data"][i][1])
                    current_hour[cindex].append([ba,date,demand])
                    #print(tempDate)

                   
                #print("Previous: ", counter, pre_hour[cindex][0])
                #print("Current : ", counter , current_hour[cindex][0])
                if(updateCounter == 0):
                    difference  =  len(current_hour[cindex])-len(df[df['BA']==ba])
                    #print(difference)
                else:
                    difference  =  len(current_hour[cindex])-len(pre_hour[cindex])

                if(difference>0): 
                    differenceCounter += difference
                    #time_series[cindex].extend(current_hour[cindex][:difference])
                    append2csv('data/newRows.csv',current_hour[cindex][:difference])
                    #append2csv('',current_hour[cindex][:difference)
                    pre_hour[cindex] = current_hour[cindex] 

                #increment index exluding EEI etc ..
                cindex+=1
                print('\r', 'Update at time ', now.strftime("%Y-%m-%d %H:%M"),  '  : [', round((cindex/(56))*100,2),'% ]  complete' , end="")

        print("")
 
        if(differenceCounter >=0):
            print("The csv has been updated  at time ",  now.strftime("%Y-%m-%d %H:%M")  ," with ", differenceCounter, " hrs of new data.")
            updateCounter+=1
        #reset Timer 
        start = time.time()


        conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "8g")
        sc = SparkContext.getOrCreate(conf=conf)
        spark = SQLContext(sc)
        demand_table = spark.read.csv("data/newRows.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm'T'HH:mm:ss.000Z").select("BA","TimeAndDate","Demand")
        if demand_table.count() != 0:
            if demand_table.dtypes[2][1] == "string":
                demand_table = demand_table.select("BA","TimeAndDate","Demand").filter("Demand!='None'")
            else:
                demand_table = demand_table.select("BA","TimeAndDate","Demand")
            if(demand_table.count() == 0):
                with open('data/newRows.csv', 'w', newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows([["BA","TimeAndDate","Demand"]])
                    f.flush()
                    os.fsync(f)
            else:
                demand_table.unionAll(spark.read.csv("data/latestAndEarliest.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00").select("BA","TimeAndDate","Demand")).write.partitionBy("BA").saveAsTable("demands")
                spark.cacheTable('demands')
                demand_table = spark.read.csv("data/latestAndEarliest.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
                demand_table.write.saveAsTable("latestAndEarliest")
            spark.cacheTable('latestAndEarliest')
            demand_table = spark.read.csv("data/elec_demand_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
            demand_table.write.saveAsTable("demandsClean")
            dates = spark.sql("select min(TimeAndDate), max(TimeAndDate) from demands").collect()[0]
            dataframe = spark.createDataFrame(pd.DataFrame(pd.date_range(dates[0], dates[1], freq="H", tz = 'UTC')),["TimeAndDate"]).repartition(8).registerTempTable("dates")
            spark.cacheTable('dates')
            getOutliersImproved2().unionAll(spark.read.csv("data/elec_demand_hourlyClean.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss+00:00").select("BA","TimeAndDate","Demand")).write.saveAsTable("demandsOut")
            spark.cacheTable('demandsOut')
            spark.sql("select * from demands").unpersist(True)
            spark.sql("select * from dates").unpersist(True)
            spark.sql("select BA, Avg(Demand) Demand, Hour(TimeAndDate) Hour, Day(TimeAndDate) Day, Month(TimeAndDate) Month, Year(TimeAndDate) Year, weekofyear(TimeAndDate) Weekday, TimeAndDate from demandsOut group by BA, TimeAndDate order by BA").coalesce(1).write.option("header", "true").csv('myfile',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
            for file in glob.glob('myfile/part-00000-*.csv'):
                shutil.move(file, 'data/elec_demand_hourlyClean.csv')
            shutil.rmtree('myfile')
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
            spark.sql('drop table demands')
            spark.sql('drop table demandsClean')
            spark.sql('drop table dates')
            spark.sql('drop table demandsOut')
            spark.sql('drop table demandsOut2')
            spark.sql('drop table latestAndEarliest')
        sc.stop()



#def getOutliersImproved2():
#    dftemp = spark.sql("select A.BA, A.max, A.min, A.Median, B.AvgDemands, B.std from ((Select BA, MAX(TimeAndDate) max , Min(TimeAndDate) min, percentile_approx(Demand, 0.5) Median from demands GROUP BY BA order by BA) as A left join (select * from latestAndEarliest) as B on (A.BA = B.BA))").collect()
#
#    #maturity_udf = udf(lambda age,age2: (abs(float(age)-float(age2))), DoubleType())
##    fSubset = open('newEntries.csv', 'w', newline="")
##    writer = csv.writer(fSubset)
##    writer.writerows([["BA","TimeAndDate","Demand"]])
#    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True),StructField("Demand", DoubleType())]
#    schema = StructType(field)
#    df10 = spark.createDataFrame([],schema).coalesce(1)
#    for currentBA in range(len(dftemp)):
#        flag = False
#        print("%d,%d"%(currentBA, len(dftemp)))
#        BA = dftemp[currentBA][0]
#        ENDTIME = dftemp[currentBA][1]
#        STARTTIME = dftemp[currentBA][2]
#        #Median = dftemp[currentBA][3]
#        Avg = dftemp[currentBA][4]
#        Std = dftemp[currentBA][5]
#        if Avg == None:
#            flag = True
#            #df = spark.sql("select * from demands where Demand > 0 and BA = '"+BA+"'")
#            #df3 = df.withColumn("Difference", maturity_udf(df.Demand,lit(Median)))
#            #test2 = df3.stat.approxQuantile("Difference", [0.5], 0)[0]
#            avgStd = spark.sql("select avg(Demand), std(Demand) from demands where BA ='"+BA+"'").collect()
#            Avg = avgStd[0][0]
#            Std = avgStd[0][1]
#        lowerBound = max(Avg - (4 * Std),1)
#        upperBound = Avg + (10 * Std)
#        temp2 = spark.sql("Select '"+BA+"' as BA, A.TimeAndDate as TimeAndDate, L.Demand as Demand from (Select TimeAndDate from dates where TimeAndDate between '"+str(STARTTIME)+"' AND '"+str(ENDTIME)+"') as A full OUTER JOIN (Select BA, TimeAndDate, Demand from demands where BA='"+BA+"' ) AS L on (L.TimeAndDate==A.TimeAndDate) order by A.TimeAndDate")
#        temp2 = temp2.withColumn("Demand",when((col("Demand")>=lowerBound)&(col("Demand")<=upperBound),col("Demand")))
#        w = (Window().orderBy(col("TimeAndDate").cast("timestamp").cast("long")).rangeBetween(-sys.maxsize, 0))
#        temp2 = temp2.withColumn("Demand", when(col("Demand").isNotNull(),col("Demand")).otherwise(last("Demand",True).over(w)))
#        if flag == True:
#            Avg = spark.sql("select avg(Demand) from demands where Demand >= "+str(lowerBound)+" and Demand <= "+str(upperBound)+" and BA = '"+BA+"'").collect()[0][0]
#        temp2 = temp2.fillna({'Demand':Avg})
#        df10 = df10.unionAll(temp2)
##    fSubset.flush()
##    os.fsync(fSubset)
##    fSubset.close()
#    print("here")
#    return df10



#def getOutliersImproved2():
#    dftemp = spark.sql("Select BA, MAX(TimeAndDate) max , Min(TimeAndDate) min, percentile_approx(Demand, 0.5) Median from demands GROUP BY BA order by BA").collect()
#    maturity_udf = udf(lambda age,age2: (abs(float(age)-float(age2))), DoubleType())
##    fSubset = open('newEntries.csv', 'w', newline="")
##    writer = csv.writer(fSubset)
##    writer.writerows([["BA","TimeAndDate","Demand"]])
#    field = [StructField("BA", StringType(), True),StructField("TimeAndDate", TimestampType(), True),StructField("Demand", DoubleType())]
#    schema = StructType(field)
#    df10 = spark.createDataFrame([],schema).coalesce(1)
#    for currentBA in range(len(dftemp)):
#        print("%d,%d"%(currentBA, len(dftemp)))
#        BA = dftemp[currentBA][0]
#        ENDTIME = dftemp[currentBA][1]
#        STARTTIME = dftemp[currentBA][2]
#        Median = dftemp[currentBA][3]
#        df = spark.sql("select * from demands where Demand > 0 and BA = '"+BA+"'")
#        df3 = df.withColumn("Difference", maturity_udf(df.Demand,lit(Median)))
#        test2 = df3.stat.approxQuantile("Difference", [0.5], 0)[0]
#        lowerBound = max(Median - (5 * test2),1)
#        upperBound = Median + (15 * test2)
#        temp2 = spark.sql("Select '"+BA+"' as BA, TimeAndDate from (Select * from dates where TimeAndDate between '"+str(STARTTIME)+"' AND '"+str(ENDTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where (Demand>="+str(lowerBound)+") and (Demand<="+str(upperBound)+") and BA='"+BA+"' ) AS L WHERE BA IS NULL ")
#        df6 = spark.sql("SELECT Demand, TimeAndDate FROM demands where BA = '"+BA+"'")
#        average = df6.join(temp2,(temp2.TimeAndDate==df6.TimeAndDate), "left_outer").filter(temp2.BA.isNull()).select(mean(df6.Demand)).collect()[0][0]
#        temporary = spark.sql("Select '"+BA+"' as BA, TimeAndDate from (Select * from dates where TimeAndDate between '"+str(STARTTIME)+"' AND '"+str(ENDTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where (Demand>="+str(lowerBound)+") and (Demand<="+str(upperBound)+") and BA='"+BA+"' ) AS L WHERE BA IS NULL ").withColumn("Demand", lit(average))
##        newRecords = spark.sql("Select '"+BA+"' as BA, TimeAndDate from (Select * from dates where TimeAndDate<'"+str(ENDTIME)+"' AND TimeAndDate>'"+str(STARTTIME)+"') NATURAL LEFT JOIN (Select BA, TimeAndDate from demands where (Demand>="+str(lowerBound)+") and (Demand<="+str(upperBound)+") and BA='"+BA+"' ) AS L WHERE BA IS NULL ").withColumn("Demand", lit(average)).collect()
##        fSubset = open('newEntries.csv', 'a', newline="")
##        writer = csv.writer(fSubset)
##        writer.writerows(newRecords)
##        print(len(newRecords))
#        temporary = temporary.coalesce(1)
#        df10 = df10.unionAll(temporary)
##    fSubset.flush()
##    os.fsync(fSubset)
##    fSubset.close()
#    print("here")
#    return df10