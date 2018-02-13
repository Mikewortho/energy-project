import pandas as pd
import requests, json, csv, datetime

class APIHandler:
        
    def __init__(self, API_KEY):
        self.API_KEY = API_KEY

    # Generate dictionary of balancing authorities and their API keys
    def gen_ba_demand_dictionary(self):
        request = 'http://api.eia.gov/category/?api_key=' + self.API_KEY + '&category_id=2122628'
        r = requests.get(request)
        x = r.json()
        ba, req = ([] for i in range(2))
        for i in range(len(x["category"]["childseries"])) : ba.append(json.dumps(x["category"]["childseries"][i]["name"])), req.append(json.dumps(x["category"]["childseries"][i]["series_id"]))
        for i in range(len(ba)):
            ba[i] = ba[i][ba[i].find("(")+1:ba[i].find(")")]
        d = dict(zip(ba, req))
        d.pop('region', None)
        self.ELEC_DEMAND_DICT = d

    # Return balancing authority lookup dictionary
    def get_ba_demand_dictionary(self):
        return self.ELEC_DEMAND_DICT

    # Write demand dictionary to CSV
    def write_ba_demand_dictionary(self, FILE_NAME):
        with open(FILE_NAME, 'w+', newline='') as file:
            fields = ['Authority', 'Demand_API']
            writer = csv.DictWriter(file, fieldnames = fields)
            writer.writeheader()
            data = [dict(zip(fields, [k, v])) for k, v in self.ELEC_DEMAND_DICT.items()]
            writer.writerows(data)

    # Generate data frame of hourly electricity demand data
    def gen_ba_demand_table(self):
        hourly_data = []
        for ba in self.ELEC_DEMAND_DICT.keys():
            request = 'http://api.eia.gov/series/?api_key=' + self.API_KEY + '&series_id=' + self.ELEC_DEMAND_DICT[ba].replace('"', '')
            r = requests.get(request)
            x = r.json()
            for i in range(len(x["series"][0]["data"])):
                date = str(x["series"][0]["data"][i][0])
                year = date[:4]
                month = date[4:6]
                day = date[6:8]
                hour = date[9:11]
                weekday = datetime.date(int(year), int(month), int(day)).weekday()
                demand = str(x["series"][0]["data"][i][1])
                hourly_data.append([ba, demand, hour, day, month, year, weekday])
        self.ELEC_DEMAND_TABLE = hourly_data;

    # Return electricity demand data frame
    def get_ba_demand_table(self):
        return self.ELEC_DEMAND_TABLE

    # Write demand table to csv
    def write_ba_demand_table(self, FILE_NAME):
        fields = ["BA", "Demand", "Hour", "Day", "Month", "Year",  "Weekday"]
        with open(FILE_NAME, 'w+') as file:
            writer = csv.writer(file, delimiter=',', lineterminator='\n')
            writer.writerow(fields)
            writer.writerows(self.ELEC_DEMAND_TABLE)
