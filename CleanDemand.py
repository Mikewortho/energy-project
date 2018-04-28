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
        print("%d,%d"%(currentBA, len(BAStats)))
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

#------------------------------------------------------------------------------------------------------------------------





def write2csv(File_Name,BA_Update,fields):
    with open( File_Name,'w+') as file:
        writer = csv.writer(file, delimiter=',', lineterminator='\n')
        writer.writerow(fields)
        writer.writerows(BA_Update)
        file.flush()
        os.fsync(file)







# API keys definition
EIA_API_KEY = '2eb9052e6a0316901fe316a4a5971df1'
WAIT = 1*10

#Get Dictionary of BA'S

request = 'http://api.eia.gov/category/?api_key=' + EIA_API_KEY + '&category_id=2122627'
r = requests.get(request)
x = r.json()
ba, req = ([] for i in range(2))
for i in range(len(x["category"]["childseries"])) : ba.append(json.dumps(x["category"]["childseries"][i]["name"])), req.append(json.dumps(x["category"]["childseries"][i]["series_id"]))
for i in range(len(ba)):
    ba[i] = ba[i][ba[i].find("(")+1:ba[i].find(")")]
d = dict(zip(ba, req))
d.pop('region', None)


def append2csv(File_Name,BA_Update):
    with open( File_Name,'a+') as file:
        writer = csv.writer(file, delimiter=',', lineterminator='\n')
        writer.writerows(BA_Update)
        file.flush()
        os.fsync(file)

def csv2panda(name):
     dframe = pd.read_csv(name)
     return dframe



#Get Dictionary of BA'S

request1 = 'http://api.eia.gov/category/?api_key=' + EIA_API_KEY + '&category_id=2122628'
r1 = requests.get(request1)
x1 = r1.json()
ba1, req1 = ([] for i in range(2))
for i in range(len(x1["category"]["childseries"])) : ba1.append(json.dumps(x1["category"]["childseries"][i]["name"])), req1.append(json.dumps(x1["category"]["childseries"][i]["series_id"]))
for i in range(len(ba1)):
    ba1[i] = ba1[i][ba1[i].find("(")+1:ba1[i].find(")")]
d1 = dict(zip(ba1, req1))
d1.pop('region', None)


# Define APIHandler object
#api_session = APIHandler.APIHandler(EIA_API_KEY)

pre_hour1 = [[] for i in range(0,56)]
current_hour1 = []

#pre-hr index
index1=0

start = time.time()

df1 = csv2panda('data/elec_demand_hourly.csv')
row1 = [["BA","TimeAndDate","Demand"]]
append2csv('data/newRows.csv',row1)


counter = 0
updateCounter = 0


# Define APIHandler object
#api_session = APIHandler.APIHandler(EIA_API_KEY)

pre_hour = []
current_hour = []


index=0
now = datetime.datetime.now()
fields = ["BA", "Demand", "Hour", "Day", "Month", "Year",  "Weekday", "TimeAndDate"]    

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
                #BA,Demand,Hour,Day,Month,Year,Weekday,TimeAndDate
                pre_hour.append([ba, demand, hour, day, month, year, weekday, date])
            index+=1
            print('\r', 'First update of forecast at time ', now.strftime("%Y-%m-%d %H:%M"),  '  : [', round((index/(56))*100,2),'% ]  complete' , end="")

write2csv("forecastedData.csv",pre_hour, fields)
start = time.time()

BA_LIST = ['DUK', 'GCPD', 'TAL', 'SEC', 'SCEG', 'CPLW', 'CISO', 'FPC', 'TPWR', 'AZPS', 'LDWP', 'AVA', 'OVEC', 'PSCO', 'SOCO', 'CHPD', 'JEA', 'TEPC']
labels = ["Daily", "Weekly", "Monthly"]
s = [24, 168, 720]
days_back = [28, 30, 60]



#row1 = [["BA","TimeAndDate","Demand"]]
#append2csv('newRows.csv',row1)


while(True):   
    #currenttimels index
    cindex = 0
    now = datetime.datetime.now()

    currentTime = time.time()
    if(currentTime-start > WAIT):
        differenceCounter =0
        #counter+=1
        current_hour = []
        now = datetime.datetime.now()

        fields = ["BA", "Demand", "Hour", "Day", "Month", "Year",  "Weekday", "TimeAndDate"]    

        baindex = 0
        for (ba) in d.keys():
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
                    #BA,Demand,Hour,Day,Month,Year,Weekday,TimeAndDate
                    current_hour.append([ba, demand, hour, day, month, year, weekday, date])

                print('\r', 'The forecast data is being updated at time ', now.strftime("%Y-%m-%d %H:%M"),  '  : [', round((baindex/(56))*100,2),'% ]  complete' , end="")
                baindex +=1
        write2csv('forecasteddata.csv',current_hour,fields)  
                #print("Previous: ", counter, pre_hour[cindex][0])
                #print("Current : ", counter , current_hour[cindex][0])
#--------------------------------                

        current_hour1 = [[] for i in range(0,56)]    
        for ba in d1.keys():
            request1 = 'http://api.eia.gov/series/?api_key=' + EIA_API_KEY + '&series_id=' + d[ba].replace('"', '')
            r1 = requests.get(request1)
            x1 = r1.json()

            if(ba != "EEI" and ba != "WWA"):
                for i in range(len(x1["series"][0]["data"])):
                    tempDate = str(x1["series"][0]["data"][i][0])
                    year = tempDate[:4]
                    month = tempDate[4:6]
                    day = tempDate[6:8]
                    hour = tempDate[9:11]
                    date = year +"-"+month+"-"+day+"T"+hour+":00:00.000Z"
                    weekday = datetime.date(int(year), int(month), int(day)).weekday()
                    demand = str(x1["series"][0]["data"][i][1])
                    current_hour1[cindex].append([ba,date,demand])
                    #print(tempDate)
                   
                #print("Previous: ", counter, pre_hour[cindex][0])
                #print("Current : ", counter , current_hour[cindex][0])
                if(updateCounter == 0):
                    difference  =  len(current_hour1[cindex])-len(df1[df1['BA']==ba])
                    #print(difference)
                else:
                    difference  =  len(current_hour1[cindex])-len(pre_hour1[cindex])

                if(difference>0): 
                    differenceCounter += difference
                    #time_series[cindex].extend(current_hour[cindex][:difference])
                    append2csv('data/newRows.csv',current_hour1[cindex][:difference])
                    #append2csv('',current_hour[cindex][:difference)
                    pre_hour1[cindex] = current_hour1[cindex] 

                #increment index exluding EEI etc ..
                cindex+=1
                print('\r', 'Update of demand at time ', now.strftime("%Y-%m-%d %H:%M"),  '  : [', round((cindex/(56))*100,2),'% ]  complete' , end="")

        print("")
 
        if(differenceCounter >=0):
            print("The demand data has been updated  at time ",  now.strftime("%Y-%m-%d %H:%M")  ," with ", differenceCounter, " hrs of new data.")
            updateCounter+=1
        #reset Timer 



                #increment index exluding EEI etc ..        
 

            

        #reset Timer 
        start = time.time()
#------------------------------------------------------------------------------------------------------------------------
        print("helloworld")

        
        # Create spark configuration for localhost
        conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "8g")
        sc = SparkContext.getOrCreate(conf=conf)
        spark = SQLContext(sc)
        # create a dataframe of our new rows
        demand_table = spark.read.csv("data/newRows.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm'T'HH:mm:ss.000Z").select("BA","TimeAndDate","Demand")
        # if there is new records
        howManyNewRows = demand_table.count()
        if demand_table.count() != 0:
            # if the type of the demand column is a string then remove the "None" value else leave it.
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
                spark.sql("select BA, Avg(Demand) Demand, Hour(TimeAndDate) Hour, Day(TimeAndDate) Day, Month(TimeAndDate) Month, Year(TimeAndDate) Year, weekofyear(TimeAndDate) Weekday, TimeAndDate from demandsOut group by BA, TimeAndDate order by BA").coalesce(1).write.option("header", "true").csv('myfile',timestampFormat="yyyy-MM-dd HH:mm:ss+00:00")
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
        
        # Forecast
        if(howManyNewRows != 0):
           
            # Daily/Weekly/Monthly updates
            for i in range(len(BA_LIST)):#len(1)):#BA_LIST)):
           
                print(BA_LIST[i])
               
                # Get new clean data - check if anything new
                new_clean = pd.read_csv("data/elec_demand_hourlyClean.csv")
                ba_condition = new_clean["BA"] == BA_LIST[i]
                clean = new_clean[ba_condition]
                clean.index = pd.to_datetime(clean["TimeAndDate"], format='%Y-%m-%dT%H')
                clean = clean.sort_index()
           
                for j in range(len(labels)):
           
                    old_data = pd.read_csv("gen_data/" + BA_LIST[i] + "_hourly_" + labels[j] + ".csv")
                    old_data.index = pd.to_datetime(old_data["Date"], format='%Y-%m-%dT%H')
                   
                    # Get last known dates
                    print(labels[j])
                    f = open("gen_data/" + BA_LIST[i] + "_dates_" + labels[j] + ".txt", 'r')
                    date_info = f.read()
                    date_info = date_info.split("\n")        
                    date_info[0] = pd.to_datetime(date_info[0], format='%Y-%m-%dT%H')        
                    ij = 0
           
           
                    while(ij == 0):
           
                        print(date_info[0])  
                                   
                        # Is there any new data
                        new_known_data_condition = clean.index > date_info[0] - pd.Timedelta(hours=1)
                        new_known_data = clean[new_known_data_condition]
                        if(len(new_known_data > 0)):
                            new_known_data = new_known_data.sort_index()
                                           
                            # Is there anything new to forecast?
                            updated_demand = pd.merge(new_known_data[["Demand"]], old_data, how='inner', left_index=True, right_index=True)
                            updated_demand = updated_demand[["Date", "Demand_x", "Prediction", "RF Prediction", "MLP Prediction", "SVM Prediction", "US Forecast"]]
                            updated_demand.columns = ["Date", "Demand", "Prediction", "RF Prediction", "MLP Prediction", "SVM Prediction", "US Forecast"]
                            new_known_data_condition = old_data.index < date_info[0]
                            old_data = old_data[new_known_data_condition]
                            old_data = old_data.append(updated_demand)
                           
                            # 24hr lookahead forecast
                            lookahead_model = sm.tsa.statespace.SARIMAX(old_data.Demand[-days_back[j]*24:].values, order=(1,1,0), seasonal_order=(1,1,0,24), enforce_stationarity=False, enforce_invertibility=False)
                            lookahead_fit = lookahead_model.fit()    
                            yhat = lookahead_fit.forecast(steps=s[j])
                            last_known_date = old_data.tail(1).index
                            test = pd.date_range(last_known_date[0] + pd.Timedelta(hours=1), last_known_date[0] + pd.Timedelta(hours=s[j]), freq='H')
                            yhat = pd.DataFrame(yhat)
                            yhat.columns = ["Prediction"]
                            yhat.index = test
                            yhat["Hour"] = yhat.index.hour
                            yhat["Weekday"] = yhat.index.weekday
                            yhat["Month"] = yhat.index.month
                            model_input = yhat[["Hour", "Weekday", "Month", "Prediction"]]    
                            old_data = old_data.append(pd.DataFrame(yhat[["Prediction"]]))
                            old_data.Date = old_data.index
                            date_info[0] = date_info[0] + pd.Timedelta(hours=s[j])        
                            old_data = old_data.fillna(0)
                            old_data.to_csv("gen_data/" + BA_LIST[i] + "_hourly_" + labels[j] + ".csv", index=False)
                       
                        else:
                            break