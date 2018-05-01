"""

# get dictionary of weather forecasts.

"""

import requests, json, csv, datetime
import time, itertools, pandas as pd



W_API_KEY = '&APPID=c6616e79b7633bd07406c6e2b0b46bf5'


lonlats = json.loads(open('USstates_avg_latLong.json').read())
df = pd.read_json('USstates_avg_latLong.json')

def forecast():
    forecast = []
    for i in range(len(df)):
        lon = df.loc[i]['longitude']
        lat = df.loc[i]['latitude']
        state = df.loc[i]['state']
        W_URL = 'http://api.openweathermap.org/data/2.5/forecast?lat='+str(lat)+'&'+'lon='+str(lon)+W_API_KEY
        r = requests.get(W_URL)
        x = r.json()
        for i in range(0,len(x['list'])):
            tempdata = x['list'][i]['main']
            temperature = tempdata['temp']
            unixtime = x['list'][i]['dt']
            utctime = datetime.datetime.fromtimestamp(unixtime)
            forecast.append([utctime,state,temperature])

    df1 = pd.DataFrame(forecast,columns=['Date', 'State' , 'Temperature'])
    return(df1)

hist_df = forecast().sort_values(by = ['State','Date'])
hist_df.to_csv("temperature_forecast.csv",sep='\t')

start = time.time()

while(True):
    current = time.time()
    if(current-start > 3*60*60):
        currentdf = forecast()
        update_df = hist_df.combine_first(currentdf).sort_values(by = ['State','Date'])
        update_df.to_csv("temperature_forecast.csv",sep = '\t')
        start = time.time()
        hist_df = update_df