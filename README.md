# User guide - Big Energy

This piece of software was made to produce forecasts for EIA in relation to balancing authorities across the US, these forecasts will be viewable locally using html and javascript and be produced using Python, Sparksql and R.
 
## Getting Started
The Advanced User section is for users who want to regenerate the batch data, and can be ignored if you download the batch data directly. For beginner users just wanting to use the website, follow the User tutorial to learn how to interact with the system at http://www.bigenergy.xyz, and users wanting the real-time data and setting up the local website read the Display the Real-time data part of the document.

The advanced user or any user wanting the real-time data update will need to install all the packages listed below, and clone the repository stored at https://github.com/sgmshaik/energy-project, as well as download the data.zip file stored at http://playingthefield.xyz/bigenergy/data/.

Languages required
Python version 3.6.3
Packages required
pySpark version 2.3.0
Pandas
Numpy 1.14.0
Statsmodels 0.8.0
Sklearn
Scipy
pickle

R - (Advanced)
Packages required
Noaa (Installed using install.packages("rnoaa"))

If you are planning on using this tool you will need to set your system time to UTC.


# Advanced Users:

## Generating Data from Scratch:
To generate the required demand data from EIA, the program.py program in the /lib/ folder needs to be run, this will then generate the relevant demand data needed in the /data/ folder.

In order to gather the raw weather data the user must run the script entitled: Pull_Weather_Data.R. This must be done in an R studio environment - however please do not use the R studio environment within the Anaconda package as this does not have the needed library “rnoaa”. When using an R studio environment (that isn't anaconda based) please ensure that the rnoaa package is installed using the following command via the R studio command line - install.packages("rnoaa"). Finally all that is left to do is run the script (the script will take a LARGE amount of time to gather the data) it will finally be outputted into a csv file named WEATHER.csv. If the user wishes to change the name of the outputted csv file this can be done by altering its name on line 821 of the code.

Once you have all of the batch data you now need to start cleaning it, firstly we work out the upper and lower bounds needed for each BA to be able to remove outliers, this is done using the BatchPreclean.py, which uses sparkSql and takes input of data/newRows.csv first identifying the medium value as well as the absolute medium standard of this data, this is used to remove major outliers, such that a new mean and standard deviation can be produced and output to a file named data/LatestAndEarliest.csv, this should take around 2 minutes.

Next we need to actually clean the demand data using this upper and lower bound and mean value to do this we simply run the BatchClean.py file, which takes inputs data/newRows as well as data/LatestAndEarliest.csv this then removes values which were greater or less than the mean and standard deviation to some constant value and replaces them using backwards based on date this uses Sparksql as well as Pandas. This outputs the clean data for each BA in one file called data/elec_demand_hourlyClean.csv, this could take around 8 minutes.

Finally we need to clean the batch weather data this takes input WEATHER.csv and cleans it using spark.sql, enforcing the lowest and highest possible observed measures as bounds, this outputs a files called data/latestAndEarliestWeather.csv as well as elec_weather_hourlyClean.csv which is the clean weather data.

The batch forecast data from the machine learning models is generated through the forecasting_ml.py file. The previous steps of data generation must be ran and then forecasting_ml.py can be run. This takes a very long time to generate, if required to generated these batch forecasts. The output of the batch forecasts are written into the “gen_data” subdirectory and the various .csvs are related to the forecasts of individual balancing authorities over different time scales.

## Optional
To get three-hourly weather forecasts for 5 days in advance run weather.py in the same directory as the json file USstates_avg_latLong.json. Executing the code will output a file in the local directory, temperature_forecast.csv. This is a time-series of forecasts which is updated every 3hrs with a new row (3hr forecast) of data and also existing values in the file that have changed are also updated. After doing this all that is left to do is create the batch forecasts.

# Guide to running the python web app
## Server Configuration
When configuring this on a dreamhost server follow these steps, these should
follow for any major hosting service with wsgi passenger services, usually
used for hosting ruby apps, however also fine for python apps.

During initial configuration, ensure that the server has wsgi enabled (with
python enabled), create a file on the server with the name passenger_wsgi.py
and copy the code from the supplied file with the same name. Create another
folder in the root directory that will contain the name of your application
(in our case productionApp) copy the folders across from the github
folder bigenergy.xyz/productionApp. Modify the last two lines of
passenger_wsgi.py to point to this folder.

For dreamhost create a folder in the root directory called tmp and from the
root directory run the command:

touch tmp/restart.txt

If enough time has passed for DNS propagation to occur then the web app should
be running at this point.

## Local Server Configuration

To run the application from a local host this is much simpler, ensure that the
latest version of flask is installed via your python package manager. Download
the latest version of bigenergy.xyz and navigate to the folder
bigenergy.xyz/productionApp, run the program with

python run.py

Access through a web browser and navigate to 

localhost:5000

## Static file server configuration
Ensure you have set up a static file server also, in this case it was a spare
domain www.playingthefield.xyz/bigenergy/data, create the appropriate
subdirectories to store the data in and ftp all the csv files to this folder

Ensure you have created an appropriate .htaccess file for cross origin requests
headers to allow cross site access, in our case we have left this extremely
unsecure by enabling all websites full access to our static file server, which
is a room for improvement.

In our .htaccess file on this server the file contents is:

<IfModule mod_headers.c>
	Header set Access-Control-Allow-Origin "*"
    Header add Access-Control-Allow-Headers "origin, x-requested-with, content-type"
	Header add Access-Control-Allow-Methods "PUT, GET, POST, DELETE, OPTIONS"
</IfModule>

Update the main.js and CreateGraphsBisHourly.js files to point to the new
file server url on the main application server.

# Displaying the Real-time data:

## Keeping the data up to date:
Now you have a clean batch version of the forecasts, to keep this up to date if you wish you need to run CleanDemand.py which takes inputs  data/LatestAndEarliest.csv and data/elec_demand_hourlyClean.csv and will request data from EIA at regular intervals, clean these requests using sparkSql and append and update data/elec_demand_hourlyClean.csv, data/LatestAndEarliest.csv, models (folder) and gen_data (folder) to produce new models within gen_data.

## A guide to setting up an offline website on a local host:
This is a fairly simple procedure and simply requires hosting the folder with
any HTTP hosting service. In our testing we used the built in http server
bundled with all python distributions. First clone the repository found at https://github.com/SurgeArrester/energy-project

Direct your command line prompt to the folder that contains the gen_data
folder, which if downloading from the latest github distribution should be
unzipped into the energy-project folder.

From here run the command for python 2:
python -m SimpleHttpServer 8000

For python 3:
python -m http.server 8000

Then direct any modern web browser to:
localhost:8000 

Go to the Offline Website folder and follow as per the usability document

# User Tutorial:
## Using the web application.
This tutorial is a walk through the website, explaining the different stages and the features that can be exploited. The website can be found at http://www.bigenergy.xyz/, where this page will be displayed:

![01](http://playingthefield.xyz/bigenergy/data/img/01.png)

From the map, it is possible to select any state where the colouring is based on the Red Green Blue (RGB) scale. When the colour is dark blue it is underpredicting the demand about -25%, green synonym of a good prediction, and dark red is overpridicting the real demand about +25%.
By selecting a state, a list of the available Balancing Authorities (BA) of the state will be displayed (see following figure).
![02](http://playingthefield.xyz/bigenergy/data/img/02.PNG)

By selecting one of the BAs, a menu will appear requesting the date range and time precision (Daily/Weekly/Monthly).  
![03](http://playingthefield.xyz/bigenergy/data/img/03.PNG)

Finally, after filling the requested information, a graph and a comparison of the similarity score of the highest scoring prediction model with the actual US demand, compared to the official US forecast will be displayed.
![04](http://playingthefield.xyz/bigenergy/data/img/04.PNG)

In order to obtain more details on the graph, it is possible to zoom on a specific date range using a two finger pinch.
![05](http://playingthefield.xyz/bigenergy/data/img/05.PNG)

It is also possible to select specific information from the graph in order to compare by clicking on the legend.
![06](http://playingthefield.xyz/bigenergy/data/img/06.PNG)
