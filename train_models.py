import pandas as pd
import numpy as np
from sklearn import preprocessing, cross_validation, svm
from sklearn.linear_model import LinearRegression
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.arima_model import ARMA
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from matplotlib import pyplot 
import statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier, MLPRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.datasets import make_regression
import math
from sklearn.metrics import r2_score
from scipy.stats import spearmanr, pearsonr
from sklearn.decomposition import PCA
from sklearn.svm import SVR
import warnings
from sklearn.model_selection import RandomizedSearchCV
import pickle

BA_LIST = ['DUK', 'GCPD', 'TAL', 'SEC', 'SCEG', 'CPLW', 'CISO', 'FPC', 'TPWR', 'AZPS', 'LDWP', 'AVA', 'OVEC', 'PSCO', 'SOCO', 'CHPD', 'JEA', 'TEPC', 'WALC', 'PACE'\
           , 'CPLE', 'SCL','IID', 'TVA', 'NYIS', 'EPE', 'MISO', 'FPL', 'BANC', 'PNM', 'LGEE', 'HST', 'IPCO', 'PACW', 'NSB', 'FMPP', 'TIDC', 'NEVP', 'SWPP', 'WAUW'\
           , 'ISNE', 'PGE', 'SPA','TEC', 'SC', 'PJM', 'DOPD', 'NWMT', 'SRP', 'ERCO', 'WACM', 'GVL', 'PSEI', 'AECI', 'BPAT', 'AEC']

labels = ["Daily", "Weekly", "Monthly"]
s = [720, 168, 720]
days_back = [60, 30, 60]  

# Get new clean data - check if anything new
new_clean = pd.read_csv("data/elec_demand_hourlyClean.csv")

# Daily/Weekly/Monthly updates
for i in range(len(BA_LIST)):

    print(BA_LIST[i])

    for j in range(len(labels)):

        # Get full data
        old_data = pd.read_csv("gen_data/" + BA_LIST[i] + "_hourly_" + labels[j] + ".csv")
        old_data.index = pd.to_datetime(old_data["Date"], format='%Y-%m-%dT%H')
        full_condition = old_data.Demand > 0
        full_data = old_data[full_condition]
        difference = full_data[["Demand"]].sub(full_data["Prediction"], axis=0)
        y = difference["Demand"]
        
        # Model vectors        
        full_data["Hour"] = full_data.index.hour
        full_data["Weekday"] = full_data.index.weekday
        full_data["Month"] = full_data.index.month
        train_condition =  full_data.index < pd.to_datetime("2017-01-01", format='%Y-%m-%d')
        not_train_condition =  full_data.index >= pd.to_datetime("2017-01-01", format='%Y-%m-%d')
        test_condition =  y.index < pd.to_datetime("2017-01-01", format='%Y-%m-%d')
        not_test_condition =  y.index >= pd.to_datetime("2017-01-01", format='%Y-%m-%d')
        new_full_train = full_data[train_condition]
        new_full_test = full_data[not_train_condition]
        X_train = new_full_train[["Hour", "Weekday", "Month", "Prediction"]]
        X_test = new_full_test[["Hour", "Weekday", "Month", "Prediction"]]
        y_train = y[test_condition]
        y_test = y[not_test_condition]
        
        # RF
        rf = RandomForestRegressor(n_estimators = 100, oob_score=False, random_state=0, bootstrap=True, max_depth=10)
        rf.fit(X_train, y_train)
    
        # MLP
        X_train["Hour"] = X_train["Hour"].apply(lambda x: math.cos(((2 * math.pi) * x) / 24))   
        X_train["Weekday"] = X_train["Weekday"].apply(lambda x: math.cos(((2 * math.pi) * x) / 7))   
        X_train["Month"] = X_train["Month"].apply(lambda x: math.cos(((2 * math.pi) * x) / 12))     
        X_test["Hour"] = X_test["Hour"].apply(lambda x: math.cos(((2 * math.pi) * x) / 24))   
        X_test["Weekday"] = X_test["Weekday"].apply(lambda x: math.cos(((2 * math.pi) * x) / 7))   
        X_test["Month"] = X_test["Month"].apply(lambda x: math.cos(((2 * math.pi) * x) / 12))  
        mlp = MLPRegressor(max_iter=50000, verbose=False, early_stopping=False, shuffle=True, tol=0.0000001, learning_rate = 'adaptive')
        mlp.fit(X_train,y_train)
        
        # SVM
        clf = SVR(C=1.0, epsilon=0.2, kernel='rbf')
        clf.fit(X_train,y_train)
        
        #        
        pickle.dump(clf, open("models/" + BA_LIST[i] + "_" + labels[j] + "_SVM.sav", 'wb'))
        pickle.dump(rf, open("models/" + BA_LIST[i] + "_" + labels[j] + "_RF.sav", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(mlp, open("models/" + BA_LIST[i] + "_" + labels[j] + "_MLP.sav", 'wb'))
    