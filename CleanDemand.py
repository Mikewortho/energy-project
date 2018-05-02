"""
Created on Mon Mar 26 17:08:53 2018
@author: robertjohnson, Omid Soltan, Jack Taylor
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



def write2csv(File_Name,BA_Update,fields):
    with open( File_Name,'w+') as file:
        writer = csv.writer(file, delimiter=',', lineterminator='\n')
        writer.writerow(fields)
        writer.writerows(BA_Update)
        file.flush()
        os.fsync(file)

def append2csv(File_Name,BA_Update):
    with open( File_Name,'w') as file:
        writer = csv.writer(file, delimiter=',', lineterminator='\n')
        writer.writerows(BA_Update)
        file.flush()
        os.fsync(file)

def csv2panda(name):
     dframe = pd.read_csv(name)
     return dframe






# API keys definition
EIA_API_KEY = '2eb9052e6a0316901fe316a4a5971df1'
WAIT = 1*240

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

fields = ["BA", "Demand", "Hour", "Day", "Month", "Year",  "Weekday", "TimeAndDate"]    

# List of BAs
BA_LIST = ['DUK', 'GCPD', 'TAL', 'SEC', 'SCEG', 'CPLW', 'CISO', 'FPC', 'TPWR', 'AZPS', 'LDWP', 'AVA', 'OVEC', 'PSCO', 'SOCO', 'CHPD', 'JEA', 'TEPC', 'WALC', 'PACE'\
           , 'CPLE', 'SCL','IID', 'TVA', 'NYIS', 'EPE', 'MISO', 'FPL', 'BANC', 'PNM', 'LGEE', 'HST', 'IPCO', 'PACW', 'NSB', 'FMPP', 'TIDC', 'NEVP', 'SWPP', 'WAUW'\
           , 'ISNE', 'PGE', 'SPA','TEC', 'SC', 'PJM', 'DOPD', 'NWMT', 'SRP', 'ERCO', 'WACM', 'GVL', 'PSEI', 'AECI', 'BPAT', 'AEC']

# Parameters
labels = ["Daily", "Weekly", "Monthly"]
s = [24, 168, 720]
days_back = [28, 30, 60]

# Parameters for aggregated statistics
region = ["AL", "AZ", "CA", "CO", "FL", "GA", "ID", "IL", "KY", "MA", "MN", "MO", "NV", "NM", "NY", "NC", "OH", "OK", "OR", "PA", "SC", "SD", "TN", "TX", "WA"]
ba_state = [["AEC"],["AZPS", "SRP", "SWPP", "TEPC", "WALC"],["BANC","CISO","IID","LDWP","TIDC","WWA"],["PSCO", "WACM", "WAUW"],["FMPP", "FPC", "FPL", "GVL", "HST", "JEA", "NSB", "SEC", "TAL", "TEC"],["SOCO"],["IPCO"],["EEI"],["LGEE"],["ISNE"],["MISO"],["AECI"],["NEVP"],["PNM"],["NYIS"],["CPLE", "CPLW", "DUK"],["OVEC"],["SPA"],["BPAT", "PACE", "PACW", "PGE"],["PJM"],["SC", "SCEG"],["NWMT"],["TVA"],["EPE", "ERCO"],["CHPD", "DOPD", "GCPD", "PSEI", "SCL", "TPWR"]]
totals = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
counts = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
percentages = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

start = time.time()-WAIT
while(True):   
    cindex = 0
    currentTime = time.time()
    #25, 15 16
    timeToSleep = start+WAIT-currentTime
    if(timeToSleep > 1):
        time.sleep(timeToSleep)
    if(currentTime-start > WAIT):
        start = time.time()
        now = datetime.datetime.now()
        df2 = pd.read_csv('data/latestAndEarliest.csv')
        df2["TimeAndDate"] = pd.to_datetime(df2["TimeAndDate"], format="%Y-%m-%d %H:00:00+00:00")
        df2.index = df2['TimeAndDate']
        newRecords  = pd.DataFrame(columns = ['BA','TimeAndDate','Demand'])
        #print(df2.loc[df2['BA']=="AEC"])
        current_hour1 = [[] for i in range(0,56)]    
        for ba in d1.keys():
            try:
                request1 = 'http://api.eia.gov/series/?api_key=' + EIA_API_KEY + '&series_id=' + d1[ba].replace('"', '')
                r1 = requests.get(request1)
                x1 = r1.json()
            except ConnectionError:
                print("Connection timed out.")
                newRecords  = pd.DataFrame(columns = ['BA','TimeAndDate','Demand'])
                break
            if(ba != "EEI" and ba != "WWA"):
                for i in range(len(x1["series"][0]["data"])):
                    tempDate = str(x1["series"][0]["data"][i][0])
                    demand = str(x1["series"][0]["data"][i][1])
                    current_hour1[cindex].append([ba,tempDate,demand])
                BARecords = current_hour1[cindex]
                df = pd.DataFrame(np.array(BARecords), columns = list(["BA","TimeAndDate","Demand"]))
                df["TimeAndDate"] = pd.to_datetime(df["TimeAndDate"], format="%Y%m%dT%HZ")
                newRecords = newRecords.append(df[df['TimeAndDate']>df2.loc[df2['BA']==ba].index.tolist()[0]])
                cindex+=1
            print('\r', 'Update of demand at time ', now.strftime("%Y-%m-%d %H:%M"),  '  : [', round((cindex/(56))*100,2),'% ]  complete' , end="")        
        #reset Timer 
        if(newRecords.shape[0]>0):
            newRecords.to_csv("data/newRows.csv",index=False,date_format='%Y-%m-%dT%H:00:00.000Z')
            print("")
            print(" The demand data has been updated  at time ",  now.strftime("%Y-%m-%d %H:%M")  ," with ", newRecords.shape[0], " hrs of new data.")
            print("")
            differenceCounter =0
            current_hour = []
            now = datetime.datetime.now()
    
            fields = ["BA", "Demand", "Hour", "Day", "Month", "Year",  "Weekday", "TimeAndDate"]    
    
            baindex = 0
            error = 0
            for (ba) in d.keys():
                try:
                    request = 'http://api.eia.gov/series/?api_key=' + EIA_API_KEY + '&series_id=' + d[ba].replace('"', '')
                    r = requests.get(request)
                    x = r.json()
                except ConnectionError:
                    print("Connection timed out.")
                    error = 1
                    break
                if(ba != "EEI" and ba != "WWA"):
                    for i in range(len(x["series"][0]["data"])):
                        tempDate = str(x["series"][0]["data"][i][0])
                        year = tempDate[:4]
                        month = tempDate[4:6]
                        day = tempDate[6:8]
                        hour = tempDate[9:11]
                        date = year +"-"+month+"-"+day+" "+hour+":00:00"
                        weekday = datetime.date(int(year), int(month), int(day)).weekday()
                        demand = str(x["series"][0]["data"][i][1])
                        #BA,Demand,Hour,Day,Month,Year,Weekday,TimeAndDate
                        current_hour.append([ba, demand, hour, day, month, year, weekday, date])
                    baindex +=1
                print('\r', 'The forecast data is being updated at time ', now.strftime("%Y-%m-%d %H:%M"),  '  : [', round((baindex/(56))*100,2),'% ]  complete' , end="")
            current_hour = list(reversed(current_hour))
            write2csv('forecasteddata.csv',current_hour,fields)  
            if(error == 0):
                print("")
                print("Now cleaning the data.")
                # Create spark configuration for localhost
                conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "8g")
                sc = SparkContext.getOrCreate(conf=conf)
                spark = SQLContext(sc)
                # create a dataframe of our new rows
                demand_table = spark.read.csv("data/newRows.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm'T'HH:mm:ss.000Z").select("BA","TimeAndDate","Demand")
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
                # Forecast
                if(howManyNewRows != 0):
                    # Get new clean data - check if anything new
                    new_clean = pd.read_csv("data/elec_demand_hourlyClean.csv")
                    forecast_data = pd.read_csv("forecastedData.csv")
                    # Daily/Weekly/Monthly updates
                    for i in range(len(BA_LIST)):#len(1)):#BA_LIST)):
                        secondPrint = 0
                       
                        # Get specific BA
                        ba_condition = new_clean["BA"] == BA_LIST[i]
                        clean = new_clean[ba_condition]
                        clean.index = pd.to_datetime(clean["TimeAndDate"], format='%Y-%m-%dT%H')
                        clean = clean.sort_index()
                        
                        # Open US forecasts to join new forecasted data
                        ba_condition = forecast_data["BA"] == BA_LIST[i]
                        us_forecast = forecast_data[ba_condition]
                        us_forecast.index = pd.to_datetime(us_forecast["TimeAndDate"], format='%Y-%m-%d %H')
                        us_forecast = us_forecast[["Demand"]]
                        us_forecast.columns = ["US Forecast"]
                        us_forecast = us_forecast.sort_index()
                        us_forecast = us_forecast[~us_forecast.index.duplicated()]
                        for j in range(len(labels)):
                            firstPrint = 0
                            old_data = pd.read_csv("gen_data/" + BA_LIST[i] + "_hourly_" + labels[j] + ".csv")
                            old_data.index = pd.to_datetime(old_data["Date"], format='%Y-%m-%dT%H')
                           
                            # Get last known dates
                            f = open("gen_data/" + BA_LIST[i] + "_dates_" + labels[j] + ".txt", 'r')
                            date_info = f.read()
                            f.close()
                            date_info = date_info.split("\n")        
                            date_info[0] = pd.to_datetime(date_info[0], format='%Y-%m-%dT%H')        
                            ij = 0
                   
                            while(ij == 0):
                                
                                           
                                # Is there any new data
                                new_known_data_condition = clean.index > date_info[0] - pd.Timedelta(hours=1)
                                new_known_data = clean[new_known_data_condition]
                                if(len(new_known_data > 0)):
                                    if(secondPrint==0):
                                        print(BA_LIST[i])
                                        secondPrint = 1
                                    if(firstPrint==0):
                                        print(labels[j])
                                        firstPrint = 1
                                    print(date_info[0]) 
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
                                                              
                                    # Load and use pretrained models to make demand predictions
                                    rf_clf = pickle.load(open("models/" + BA_LIST[i] + "_" + labels[j] + "_RF.sav", "rb"))
                                    mlp_clf = pickle.load(open("models/" +BA_LIST[i] + "_" + labels[j] + "_MLP.sav", "rb"))
                                    svm_clf = pickle.load(open("models/" +BA_LIST[i] + "_" + labels[j] + "_SVM.sav", "rb"))
                                    
                                    # Get predictions
                                    predicted =  rf_clf.predict(model_input)
                                    p = pd.DataFrame(predicted)
                                    p.index = yhat.index    
                                    out = p[0].add(yhat["Prediction"], axis=0)
                                    yhat["RF Prediction"] = out                
                                    predicted =  mlp_clf.predict(model_input)
                                    p = pd.DataFrame(predicted)
                                    p.index = yhat.index    
                                    out = p[0].add(yhat["Prediction"], axis=0)
                                    yhat["MLP Prediction"] = out
                                    predicted =  svm_clf.predict(model_input)
                                    p = pd.DataFrame(predicted)
                                    p.index = yhat.index    
                                    out = p[0].add(yhat["Prediction"], axis=0)
                                    yhat["SVM Prediction"] = out                             
                                    
                                    # Write out updated data
                                    old_data = old_data.append(pd.DataFrame(yhat[["Prediction", "RF Prediction", "MLP Prediction", "SVM Prediction"]]))
                                    old_data.Date = old_data.index
                                    date_info[0] = date_info[0] + pd.Timedelta(hours=s[j])        
                                    old_data = old_data[["Date", "Demand", "MLP Prediction", "Prediction", "RF Prediction", "SVM Prediction"]]
                                    old_data["US Forecast"] = us_forecast
                                    old_data = old_data.fillna(0)
                                    old_data = old_data[["Date", "Demand", "US Forecast", "Prediction", "RF Prediction", "MLP Prediction", "SVM Prediction"]]
                                    old_data.to_csv("gen_data/" + BA_LIST[i] + "_hourly_" + labels[j] + ".csv", index=False)
                                
                                else:
                                    # Stats for graph
                                    last_date = old_data["Date"].tail(1)
                                    lower_condition = old_data.index > (last_date.index[0] - pd.Timedelta(hours=48))
                                    last_dates = old_data[lower_condition]
                                    demand_condition = last_dates.Demand != 0
                                    last_dates = last_dates[demand_condition]
                                    
                                    # Tally counts and percent changes
                                    if(j == 0):
                                        for state in range(len(ba_state)):
                                            if BA_LIST[i] in ba_state[state]:
                                                print(BA_LIST[i])
                                                print(region[state])
                                                counts[state] += 1
                                                sum_demand = sum(last_dates["Demand"])
                                                classifier_demand = sum(last_dates["RF Prediction"])
                                                totals[state] += (((classifier_demand - sum_demand) / sum_demand)*100)
                                                print(counts)
                                                print(totals)                                    
                                    break
                                
                            # Store new dates of predictions
                            f = open("gen_data/" + BA_LIST[i] + "_dates_" + labels[j] + ".txt", 'w+')
                            f.write(str(date_info[0]) + "\n" + str(date_info[1]))
                            f.flush()
                            os.fsync(f)
                            f.close()
                            
                # Work out final percentages
                for p in range(len(counts)):
                    if(counts[p] > 0):
                        percentages[p] = totals[p] / counts[p]
                    percentages[p] = percentages[p] * 6
                    if (percentages[p] > 100):
                        percentages[p] = 100
                    elif (percentages[p] < -100):
                        percentages[p] = -100
                percent_df = pd.DataFrame()
                percent_df["State"] = region
                percent_df["PredictionPercent"] = percentages
                percent_df.to_csv("gen_data/stateColours.csv", index=False)
                            
                            

