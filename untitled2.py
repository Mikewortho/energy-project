import pandas as pd
import csv
pd.DataFrame(pd.date_range("2001-01-01", "2018-08-11", freq="H")).to_csv("temp.csv", header=["TimeAndDate"])
