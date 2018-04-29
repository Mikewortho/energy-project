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

warnings.filterwarnings("ignore")
actual_data = pd.read_csv("data/elec_demand_hourlyClean.csv")
forecast_data = pd.read_csv("data/forecast_hourly.csv")
weather_data = pd.read_csv("data/elec_weather_hourlyCLean.csv")
weather_data.index = pd.to_datetime(weather_data["TimeAndDate"], format='%Y-%m-%dT%H')

# Split into date time and columnar styled data] 
actual = actual_data[["BA", "Demand"]] 
actual_date = actual_data[["Hour", "Day", "Month", "Year"]]
forecast = forecast_data[["BA", "Demand"]] 
forecast_date = forecast_data[["Hour", "Day", "Month", "Year"]]

# Get data
actual.index = pd.to_datetime(actual_date)
actual = actual.sort_index(ascending=True)
forecast.index = pd.to_datetime(forecast_date)
forecast = forecast.sort_index(ascending=True)
forecast = forecast.rename(index=str, columns={"Demand": "Forecast"})

# List of BAs
BA_LIST = ['LGEE', 'HST', 'IPCO', 'PACW', 'NSB', 'FMPP', 'TIDC', 'NEVP', 'SWPP', 'WAUW'\
           , 'ISNE', 'PGE', 'SPA','TEC', 'SC', 'PJM', 'DOPD', 'NWMT', 'SRP', 'ERCO', 'WACM', 'GVL', 'PSEI', 'AECI', 'BPAT', 'AEC']

# Parameters
s = [24, 168, 720]
labels = ["Daily", "Weekly", "Monthly"]
days_back = [14, 30, 60]  
                
# Rolling forecast generation for each BA
for i in range(len(BA_LIST)):
        
    print(str(i) + ": " + BA_LIST[i])
    
    # Query
    COND_1 = actual["BA"] == BA_LIST[i]
    COND_2 = forecast["BA"] == BA_LIST[i]   
    COND_3 = weather_data["BA"] == BA_LIST[i]   
    ba_actual = actual[COND_1]['Demand']
    ba_forecast = forecast[COND_2]['Forecast']
    weather = weather_data[COND_3]
        
    # Cleaning
    merge = pd.merge(pd.DataFrame(ba_actual), pd.DataFrame(ba_forecast), how='inner', left_index=True, right_index=True)
    merge['Demand'].replace('None', 0, inplace=True)
    merge['Forecast'].replace('None', 0, inplace=True)
    merge['Demand'] = merge['Demand'].astype(float).fillna(0.0)
    merge['Forecast'] = merge['Forecast'].astype(float).fillna(0.0)    
        
    for j in range(len(s)):         
        
        # Parameters
        predictions = []
        t = 1
        X = pd.Series(merge['Demand'])
        Y = []   
        full = pd.DataFrame()
        
        print("Days back: " + str(days_back))
                
        # Sarimax model - rolling forecast
        while ((days_back[j]*24 + (t * s[j])) < len(X)):
                    
            # Test & Train
            train, test = X[(t*s[j]):(days_back[j]*24 + (t*s[j]))].astype(float), X[(days_back[j]*24 + (t*s[j])):(days_back[j]*24 + ((t+1)*s[j]))].astype(float)
            date_index = test.index
            print(str(days_back[j]*24 + (t * s[j])) + " / " + str(len(X)))
                                  
            # Model fitting
            model = sm.tsa.statespace.SARIMAX(train.values, order=(1,1,0), seasonal_order=(1,1,0,24), enforce_stationarity=False, enforce_invertibility=False)
            model_fit = model.fit(disp=1)
            yhat = model_fit.forecast(steps=len(test))               
            error = 0
            predictions = pd.DataFrame(test)
            predictions["Prediction"] = yhat
            full = full.append(pd.merge(pd.DataFrame(merge), pd.DataFrame(predictions["Prediction"]), how='inner', left_index=True, right_index=True))
            t += 1   
            
        # 24hr lookahead forecast
        lookahead_model = sm.tsa.statespace.SARIMAX(X[-days_back[j]*24:].values, order=(1,1,0), seasonal_order=(1,1,0,24), enforce_stationarity=False, enforce_invertibility=False)
        lookahead_fit = lookahead_model.fit()    
        yhat = lookahead_fit.forecast(steps=s[j])
    
        # print overall error
        print(labels[j])
        print(BA_LIST[i] + ": US Forecast error = " + str(mean_squared_error(full["Demand"], full["Forecast"])) + " Model error = " + str(mean_squared_error(full["Demand"], full["Prediction"])))
                              
        # Write out
        full.to_csv("output_data/" + BA_LIST[i] + "_data.csv", header=True)
                
        # Output
        pyplot.plot(full["Demand"], color='k', label="actual")
        pyplot.plot(full["Forecast"], color='b', label="US forecast")
        pyplot.plot(full["Prediction"], color='r', label="SARIMAX prediction")
        pyplot.legend(loc=2, fontsize = 'small')
        pyplot.savefig("forecasts/24hr/" + BA_LIST[i] + " " + str(t) + " .png")       
        pyplot.close()
            
        # Join weather
        full = pd.merge(pd.DataFrame(full), pd.DataFrame(weather["wind_direction"]), how='inner', left_index=True, right_index=True)
        full = pd.merge(pd.DataFrame(full), pd.DataFrame(weather["wind_speed"]), how='inner', left_index=True, right_index=True)
        full = pd.merge(pd.DataFrame(full), pd.DataFrame(weather["temperature"]), how='inner', left_index=True, right_index=True)
        full = pd.merge(pd.DataFrame(full), pd.DataFrame(weather["temperature_dewpoint"]), how='inner', left_index=True, right_index=True)
        full = pd.merge(pd.DataFrame(full), pd.DataFrame(weather["air_pressure"]), how='inner', left_index=True, right_index=True)
        
        # Featre extraction - sin(2pihour/24) 
        difference = full[["Demand"]].sub(full["Prediction"], axis=0)
        new_full = full[["Demand", "Forecast", "Prediction", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure"]]       
        new_full["Hour"] = full.index.hour
        new_full["Weekday"] = full.index.weekday
        new_full["Month"] = full.index.month
        new_full["demand-1"] = new_full.Prediction - new_full.Prediction.shift(1)
        new_full["demand-2"] = new_full.Prediction - new_full.Prediction.shift(2)
        new_full["demand-3"] = new_full.Prediction - new_full.Prediction.shift(3)
        new_full["wind_direction-1"] = new_full.wind_direction - new_full.wind_direction.shift(1)
        new_full["wind_speed-1"] = new_full.wind_speed - new_full.wind_speed.shift(1)
        new_full["temperature-1"] = new_full.temperature - new_full.temperature.shift(1)
        new_full["temperature_dewpoint-1"] = new_full.temperature_dewpoint - new_full.temperature_dewpoint.shift(1)
        new_full["air_pressure-1"] = new_full.air_pressure - new_full.air_pressure.shift(1)
        new_full["wind_direction-2"] = new_full.wind_direction - new_full.wind_direction.shift(2)
        new_full["wind_speed-2"] = new_full.wind_speed - new_full.wind_speed.shift(2)
        new_full["temperature-2"] = new_full.temperature - new_full.temperature.shift(2)
        new_full["temperature_dewpoint-2"] = new_full.temperature_dewpoint - new_full.temperature_dewpoint.shift(2)
        new_full["air_pressure-2"] = new_full.air_pressure - new_full.air_pressure.shift(2)
        new_full["wind_direction-3"] = new_full.wind_direction - new_full.wind_direction.shift(3)
        new_full["wind_speed-3"] = new_full.wind_speed - new_full.wind_speed.shift(3)
        new_full["temperature-3"] = new_full.temperature - new_full.temperature.shift(3)
        new_full["temperature_dewpoint-3"] = new_full.temperature_dewpoint - new_full.temperature_dewpoint.shift(3)
        new_full["air_pressure-3"] = new_full.air_pressure - new_full.air_pressure.shift(3)
        y = difference["Demand"]
        
        # Data manipulation
        new_full = new_full.fillna(0)
        train_condition =  new_full.index < pd.to_datetime("2017-01-01", format='%Y-%m-%d')
        not_train_condition =  new_full.index >= pd.to_datetime("2017-01-01", format='%Y-%m-%d')
        test_condition =  y.index < pd.to_datetime("2017-01-01", format='%Y-%m-%d')
        not_test_condition =  y.index >= pd.to_datetime("2017-01-01", format='%Y-%m-%d')
        new_full_train = new_full[train_condition]
        new_full_test = new_full[not_train_condition]
        y_train = y[test_condition]
        y_test = y[not_test_condition]
        X_train = new_full_train[["Hour", "Weekday", "Month", "Prediction", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "demand-1", "demand-2", "demand-3", "temperature-1", "temperature_dewpoint-1", "temperature-2", "wind_speed-1"]]
        X_test = new_full_test[["Hour", "Weekday", "Month", "Prediction", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "demand-1", "demand-2", "demand-3", "temperature-1", "temperature_dewpoint-1", "temperature-2", "wind_speed-1"]]
        X_simple_train = new_full_train[["Hour", "Weekday", "Month", "Prediction"]]
        X_simple_test = new_full_test[["Hour", "Weekday", "Month", "Prediction"]]
        
        # Random forest:    
        rf = RandomForestRegressor(n_estimators = 1000, oob_score=False, random_state=0, bootstrap=True)
        rf_simple = RandomForestRegressor(n_estimators = 1000, oob_score=False, random_state=0, bootstrap=True)
        rf.fit(X_train, y_train)
        rf_simple.fit(X_simple_train, y_train)

        predicted_train_simple =  rf_simple.predict(X_simple_train)
        predicted_test_simple = rf_simple.predict(X_simple_test)    
        p = pd.DataFrame(predicted_test)
        p_simple = pd.DataFrame(predicted_test_simple)
        p.index = y_test.index    
        p_simple.index = y_test.index
        expected = pd.DataFrame(y_test)
        rf_output = p[0].add(X_test["Prediction"], axis=0)
        rf_simple_output = p_simple[0].add(X_simple_test["Prediction"], axis=0)
        output_expected = expected["Demand"].add(X_test["Prediction"], axis=0)
    
        # Old output
        us_test_score = r2_score(new_full_test["Demand"], new_full_test["Forecast"])
        us_spearman = spearmanr(new_full_test["Demand"], new_full_test["Forecast"])
        us_pearson = pearsonr(new_full_test["Demand"], new_full_test["Forecast"])    
        print(f'US Forecast R-2 score: {us_test_score:>5.3}')
        print(f'US Forecast Spearman correlation: {us_spearman[0]:.3}')
        print(f'US Forecast Pearson correlation: {us_pearson[0]:.3}\n')    
        arima_test_score = r2_score(new_full_test["Demand"], new_full_test["Prediction"])
        arima_spearman = spearmanr(new_full_test["Demand"], new_full_test["Prediction"])
        arima_pearson = pearsonr(new_full_test["Demand"], new_full_test["Prediction"])
        print(f'ARIMA Forecast R-2 score: {arima_test_score:>5.3}')
        print(f'ARIMA Forecast Spearman correlation: {arima_spearman[0]:.3}')
        print(f'ARIMA Forecast Pearson correlation: {arima_pearson[0]:.3}\n')   
        
        # Model output
        print(rf.feature_importances_)     
        rf_test_score = r2_score(output_expected, rf_output)
        rf_spearman = spearmanr(output_expected, rf_output)
        rf_pearson = pearsonr(output_expected, rf_output)
        rf_simple_test_score = r2_score(output_expected, rf_simple_output)
        rf_simple_spearman = spearmanr(output_expected, rf_simple_output)
        rf_simple_pearson = pearsonr(output_expected, rf_simple_output)
        print(f'RF Test data R-2 score: {rf_test_score:>5.3}')
        print(f'RF Test data Spearman correlation: {rf_spearman[0]:.3}')
        print(f'RF Test data Pearson correlation: {rf_pearson[0]:.3}')    
        print(f'RF Simple Test data R-2 score: {rf_simple_test_score:>5.3}')
        print(f'RF Simple Test data Spearman correlation: {rf_simple_spearman[0]:.3}')
        print(f'RF Simple Test data Pearson correlation: {rf_simple_pearson[0]:.3}\n')    
         
        # Machine Learning - MLP
        X_train["Hour"] = X_train["Hour"].apply(lambda x: math.cos(((2 * math.pi) * x) / 24))   
        X_train["Weekday"] = X_train["Weekday"].apply(lambda x: math.cos(((2 * math.pi) * x) / 7))   
        X_train["Month"] = X_train["Month"].apply(lambda x: math.cos(((2 * math.pi) * x) / 12))     
        X_test["Hour"] = X_test["Hour"].apply(lambda x: math.cos(((2 * math.pi) * x) / 24))   
        X_test["Weekday"] = X_test["Weekday"].apply(lambda x: math.cos(((2 * math.pi) * x) / 7))   
        X_test["Month"] = X_test["Month"].apply(lambda x: math.cos(((2 * math.pi) * x) / 12))  
        X_simple_train["Hour"] = X_train["Hour"].apply(lambda x: math.cos(((2 * math.pi) * x) / 24))   
        X_simple_train["Weekday"] = X_train["Weekday"].apply(lambda x: math.cos(((2 * math.pi) * x) / 7))   
        X_simple_train["Month"] = X_train["Month"].apply(lambda x: math.cos(((2 * math.pi) * x) / 12))     
        X_simple_test["Hour"] = X_test["Hour"].apply(lambda x: math.cos(((2 * math.pi) * x) / 24))   
        X_simple_test["Weekday"] = X_test["Weekday"].apply(lambda x: math.cos(((2 * math.pi) * x) / 7))   
        X_simple_test["Month"] = X_test["Month"].apply(lambda x: math.cos(((2 * math.pi) * x) / 12)) 
        mlp = MLPRegressor(max_iter=50000, verbose=False, early_stopping=False, shuffle=True, tol=0.0000001, learning_rate = 'adaptive')
        mlp.fit(X_train,y_train)
        mlp_simple = MLPRegressor(max_iter=50000, verbose=False, early_stopping=False, shuffle=True, tol=0.0000001, learning_rate = 'adaptive')
        mlp_simple.fit(X_simple_train,y_train)
        predictions = mlp.predict(X_test)
        predictions_simple = mlp_simple.predict(X_simple_test)
        p = pd.DataFrame(predictions)
        p_simple = pd.DataFrame(predictions_simple)
        p.index = y_test.index  
        p_simple.index = y_test.index
        expected = pd.DataFrame(y_test)
        mlp_output = p[0].add(X_test["Prediction"], axis=0)
        mlp_simple_output = p_simple[0].add(X_simple_test["Prediction"], axis = 0)
        output_expected = expected["Demand"].add(X_test["Prediction"], axis=0)   
        mlp_test_score = r2_score(output_expected, mlp_output)
        mlp_spearman = spearmanr(output_expected, mlp_output)
        mlp_pearson = pearsonr(output_expected, mlp_output)
        mlp_simple_test_score = r2_score(output_expected, mlp_simple_output)
        mlp_simple_spearman = spearmanr(output_expected, mlp_simple_output)
        mlp_simple_pearson = pearsonr(output_expected, mlp_simple_output)
        print(f'MLP Test data R-2 score: {mlp_test_score:>5.3}')
        print(f'MLP Test data Spearman correlation: {mlp_spearman[0]:.3}')
        print(f'MLP Test data Pearson correlation: {mlp_pearson[0]:.3}')
        print(f'MLP Test data R-2 score: {mlp_simple_test_score:>5.3}')
        print(f'MLP Test data Spearman correlation: {mlp_simple_spearman[0]:.3}')
        print(f'MLP Test data Pearson correlation: {mlp_simple_pearson[0]:.3}\n')        
    
        # SVM
        clf = SVR(C=1.0, epsilon=0.2, kernel='rbf')
        clf_simple = SVR(C=1.0, epsilon=0.2, kernel='rbf')
        clf.fit(X_train,y_train)
        clf_simple.fit(X_simple_train,y_train)
        predictions = clf.predict(X_test)
        simple_predictions = clf_simple.predict(X_simple_test)
        p = pd.DataFrame(predictions)
        p_simple = pd.DataFrame(simple_predictions)
        p.index = y_test.index  
        p_simple.index = y_test.index
        expected = pd.DataFrame(y_test)
        svm_output = p[0].add(X_test["Prediction"], axis=0)
        svm_simple_output = p_simple[0].add(X_simple_test["Prediction"], axis=0)
        svm_test_score = r2_score(output_expected, svm_output)
        svm_simple_test_score = r2_score(output_expected, svm_simple_output)
        svm_spearman = spearmanr(output_expected, svm_output)
        svm_simple_spearman = spearmanr(output_expected, svm_simple_output)
        svm_pearson = pearsonr(output_expected, svm_output)
        svm_simple_pearson = pearsonr(output_expected, svm_simple_output)
        print(f'SVM (RBF) Test data R-2 score: {svm_test_score:>5.3}')
        print(f'SVM (RBF) Test data Spearman correlation: {svm_spearman[0]:.3}')
        print(f'SVM (RBF) Test data Pearson correlation: {svm_pearson[0]:.3}')   
        print(f'SVM (RBF) Test data R-2 score: {svm_simple_test_score:>5.3}')
        print(f'SVM (RBF) Test data Spearman correlation: {svm_simple_spearman[0]:.3}')
        print(f'SVM (RBF) Test data Pearson correlation: {svm_simple_pearson[0]:.3}\n')       
        
        # Output
        output = pd.merge(pd.DataFrame(full[["Demand", "Forecast", "Prediction"]]), pd.DataFrame(rf_output), how='inner', left_index=True, right_index=True)
        output = pd.merge(pd.DataFrame(output), pd.DataFrame(mlp_output), how='inner', left_index=True, right_index=True)
        output = pd.merge(pd.DataFrame(output), pd.DataFrame(svm_output), how='inner', left_index=True, right_index=True)
        output.columns = ["Demand", "US Forecast", "Prediction", "RF Prediction", "MLP Prediction", "SVM Prediction"]
        
        # Write
        last_known_date = output.tail(1).index
        test = pd.date_range(last_known_date[0] + pd.Timedelta(hours=1), last_known_date[0] + pd.Timedelta(hours=s[j]), freq='H')
        yhat = pd.DataFrame(yhat)
        yhat.columns = ["Prediction"]
        yhat.index = test 
        yhat["Hour"] = yhat.index.hour
        yhat["Weekday"] = yhat.index.weekday
        yhat["Month"] = yhat.index.month
        model_input = yhat[["Hour", "Weekday", "Month", "Prediction"]]
        
        # Append simple model predictions
        predicted =  rf_simple.predict(model_input)
        p = pd.DataFrame(predicted)
        p.index = yhat.index    
        p_simple.index = y_test.index
        expected = pd.DataFrame(y_test)
        rf_output = p[0].add(yhat["Prediction"], axis=0)
        yhat["RF Prediction"] = rf_output       
        predicted =  mlp_simple.predict(model_input)
        p = pd.DataFrame(predicted)
        p.index = yhat.index    
        p_simple.index = y_test.index
        expected = pd.DataFrame(y_test)
        mlp_output = p[0].add(yhat["Prediction"], axis=0)
        yhat["MLP Prediction"] = mlp_output    
        predicted =  clf_simple.predict(model_input)
        p = pd.DataFrame(predicted)
        p.index = yhat.index    
        p_simple.index = y_test.index
        expected = pd.DataFrame(y_test)
        svm_output = p[0].add(yhat["Prediction"], axis=0)
        yhat["SVM Prediction"] = svm_output
     
        # Append previous arima forecast
        first_predicted_date = output.head(1).index
        prior_condition =  full.index < first_predicted_date[0]
        previous_predictions = full[prior_condition]
        
        # Append predictions
        output = output.append(yhat[["Prediction", "RF Prediction", "MLP Prediction", "SVM Prediction"]])
        output = output.append(previous_predictions[["Prediction", "Demand"]])
        output["Date"] = output.index
        output = output.fillna(0.0)  
        output = output.sort_index(ascending=True)
        last_date = output.tail(1).index
        
        # Write to file    
        f = open("gen_data/" + BA_LIST[i] + "_settings_" + labels[j] + ".txt", 'w+')
        out = (str(us_test_score) + "," + str(us_spearman[0]) + "," + str(us_pearson[0]) + "\n" + str(arima_test_score) + "," + str(arima_spearman[0]) + "," + str(arima_pearson[0]) + "\n" + str(rf_test_score) + "," + str(rf_spearman[0]) + "," + str(rf_pearson[0]) + "\n" + str(rf_simple_test_score) + "," + str(rf_simple_spearman[0]) + "," + str(rf_simple_pearson[0]) + "\n" + str(svm_test_score) + "," + str(svm_simple_spearman[0]) + "," + str(svm_simple_pearson[0]) + "\n" + str(mlp_test_score) + "," + str(mlp_spearman[0]) + "," + str(mlp_pearson[0])+ "\n" + str(mlp_simple_test_score) + "," + str(mlp_simple_spearman[0]) + "," + str(mlp_simple_pearson[0]))
        f.write(out)
        f.close()
        f = open("gen_data/" + BA_LIST[i] + "_dates_" + labels[j] + ".txt", 'w+')
        f.write(str(last_known_date[0] + pd.Timedelta(hours=1)) + "\n" + str(last_date[0]))
        f.close()
        new_output = output[["Date", "Demand", "US Forecast", "Prediction", "RF Prediction", "MLP Prediction", "SVM Prediction"]]
        new_output.to_csv("gen_data/" + BA_LIST[i] + "_hourly_" + labels[j] + ".csv", index=False)
        
        # Write models
#        pickle.dump(clf, open("gen_data/" + BA_LIST[i] + "_" + labels[j] + "_SVM.sav", 'wb'))
#        pickle.dump(clf_simple, open("gen_data/" + BA_LIST[i] + "_" + labels[j] + "_Simple_SVM.sav", 'wb'))
#        pickle.dump(rf, open("gen_data/" + BA_LIST[i] + "_" + labels[j] + "_RF.sav", 'wb'))
#        pickle.dump(rf_simple, open("gen_data/" + BA_LIST[i] + "_" + labels[j] + "_Simple_RF.sav", 'wb'))
#        pickle.dump(mlp, open("gen_data/" + BA_LIST[i] + "_" + labels[j] + "_MLP.sav", 'wb'))
#        pickle.dump(mlp_simple, open("gen_data/" + BA_LIST[i] + "_" + labels[j] + "_Simple_MLP.sav", 'wb'))
#        
#    
