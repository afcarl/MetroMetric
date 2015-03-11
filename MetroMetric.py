# -*- coding: utf-8 -*-
"""
MetroMetric
Created on Wed Jan 07 20:45:50 2015

@author: abrown1

The goal of this project is to develop one or more metrics for performance of WMATA's next bus algorithm
"""

"""
GatherMetroData will run over a set of time and pull 

inputs: interval (defualt 30 seconds): the time interval to pull data, in seconds
routes: a list of bus routes by identifier (ROUTEID)

For each route, it will:
1 - look up the directions
2 - find the stop list once
Then, every interval, it will query every bus and every stop on each route to store the values
Bus locations come from a "Bus Position" JSON / XML
Predictions come from a "Next Buses" JSON / XML
Key values to save are each bus' location and the prediction times for each stop (which includes the bus id for each prediction)

note I am limited to "Rate limited to 10 calls/second and 50,000 calls per day"
so, probably should actually run a check in the function to make sure I'm not going to exceed that.
API info:
primary key: qkk2x7wckemx2bxgs8g2sq8a
secondary key: 9401d150cb6d4c75bbfdfa4a941e8b3e

tier 3 key:
primary key: 83151dadf5be461f96c84af142a9c984

Bus positions API:
https://api.wmata.com/Bus.svc/json/jBusPositions[?RouteID][&Lat][&Lon][&Radius]&api_key=qkk2x7wckemx2bxgs8g2sq8a
Next Bus Positions API:
https://api.wmata.com/NextBusService.svc/json/jPredictions[?StopID]&api_key=qkk2x7wckemx2bxgs8g2sq8a
Route Details API:
https://api.wmata.com/Bus.svc/json/jRouteDetails[?RouteID][&Date]&api_key=qkk2x7wckemx2bxgs8g2sq8a

This process builds a data frame of predictions. Each is a single predictions of a bus and when it will arrive at a stop.
This means the strucutre will have the same bus many times, since for each pull it will show up in many stops
It will also be populated with the actual time the bus arrived where possible (since this is actually determined after the rest of the data)
API abbreviations: BP = BusPosition; NBP = NextBusPosition; RD = route details
Where they use both, we'll use to match records.

DATA STRUCTURE DEFINITION: | API SOURCE
PID: prediction ID. Assigned as an internal identifier
RouteID: route the event is from | NBP:RouteID
DirectionNum: 0 or 1| BP:DirectionNum 
DirectionText: description of the | BP:DirectionText
StopID: the ID of the stop | NBP: StopID
StopLat: the stop latitude | RD:Stop:Lat
StopLon: the stop longitude | RD:Stop:Lon
VehicleID: the identifier of the bus | NBP:VehicleID
TripID: the identified of the trip | NBP:VehilceID
BusLat: the bus longitude | BP:Lat
BusLon: the bus longitude | BP:Lon
DateTime: a datetime object so we can do time math later
TimeStr: the time of the query (full string)
Time: just the time
Year: the year
Month: the month of the year
Day: the day of the month
Temp: Current Temp in F
Weather: a description of weather
Deviation: Deviation, in minutes, from schedule. Positive values indicate that the bus is running late while negative ones are for buses running ahead of schedule.
PA: predicted arrival: When the bus is predicted to arrive at this stop
AA1: actual arrival 1: When the bus actually arrived (filled in on later calls), determined by status "arriving"
AA2: actual arrival 2: When the bus actually arrived (filled in on later calls), determined by close lat - lon from position data

['PID','RouteID','Direction','StopID','StopLat','StopLon','VehicleID', 'TripID','BusLat','BusLon','Datetime','Timestr','Time','Year','Month','Day','Temp','Weather','PA','AA1','AA2']

"""

import os
import csv
import json
import httplib, urllib, urllib2, base64
import numpy as np
import pandas as pd
import time
from datetime import datetime

# my free API key from WMATA
#api_key = 'qkk2x7wckemx2bxgs8g2sq8a'

# my tier 3 WMATA API key
api_key = '83151dadf5be461f96c84af142a9c984'

# my open weathermap API key
OWM_api = 'e3530bdb1863c62ef30a7699ba2e3cdd'

# file path - since I'm running on different comps this makes it easy to switch
# for laptop
#filepath = '../../MetroMetric/'
#for desktop
filepath = '../GitHub/MetroMetric/'

# metro API functions
# Route Details API
def RD(RouteID = 'B30'):
    headers = {
        # Basic Authorization Sample
        # 'Authorization': 'Basic %s' % base64.encodestring('{username}:{password}'),
    }
    params = urllib.urlencode({
        # Specify your subscription key
        'api_key': api_key,
        # Specify values for the following required parameters
        'RouteID': RouteID,
        # Specify values for optional parameters, as needed
        #'Date': '',
        })
 
    try:
        conn = httplib.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/Bus.svc/json/jRouteDetails?%s" % params, "", headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))
    return data

# next bus prediction API
def NBP(StopID = ''):
    headers = {
    # Basic Authorization Sample
    # 'Authorization': 'Basic %s' % base64.encodestring('{username}:{password}'),
    }
    params = urllib.urlencode({
        # Specify your subscription key
        'api_key': api_key,
        # Specify values for the following required parameters
        'StopID': StopID,
        })
 
    try:
        #use the new test URL as request by WMATA
        URL = "/NextBusService.svc/json/jPredictions?StopID="+StopID+"&api_key=550034573ca5440caf376c6aef51a01d"
        conn = httplib.HTTPSConnection('wmataapibeta.azure-api.net')        
        conn.request("GET", URL)        
        
        # the original URL
        #conn = httplib.HTTPSConnection('api.wmata.com')
        #conn.request("GET", "/NextBusService.svc/json/jPredictions?%s" % params, "", headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))
    return data
    
# Bus Position API
def BP(RouteID = ''):
    headers = {
    # Basic Authorization Sample
    # 'Authorization': 'Basic %s' % base64.encodestring('{username}:{password}'),
    }
 
    params = urllib.urlencode({
        # Specify your subscription key
        'api_key': 'qkk2x7wckemx2bxgs8g2sq8a',
        # Specify values for optional parameters, as needed
        'RouteID': RouteID,
        #'Lat': '',
        #'Lon': '',
        #'Radius': '',
        })
 
    try:
        conn = httplib.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/Bus.svc/json/jBusPositions?%s" % params, "", headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))

    return data

def Inc(RouteID = ''):  
    headers = {
    # Basic Authorization Sample
    # 'Authorization': 'Basic %s' % base64.encodestring('{username}:{password}'),
    }
 
    params = urllib.urlencode({
    # Specify your subscription key
    'api_key': api_key,
    # Specify values for optional parameters, as needed
    'Route': RouteID,
    })
 
    try:
        conn = httplib.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/Incidents.svc/json/BusIncidents?%s" % params, "", headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))
    
    return data

# Initialize Route Struct will create a strucutre of routes and stops
def InitializeRouteStruct(routes = []):
    #create a dictionary object that has all the routes as keys and a frame of stop IDs, lats, lons, and direction
    RouteStruct = {}    
    for route in routes:
        # look up the route with API, and use the json module to decode
        RouteData = json.loads(RD(route))
        # make a dataframe for each direction and append them
        df0 = pd.DataFrame(RouteData['Direction0']['Stops'], dtype = float)
        df1 = pd.DataFrame(RouteData['Direction1']['Stops'], dtype = float)
        # note the direction in the df for future use
        df0['Direction']=0
        df1['Direction']=1
        # add to the structure
        RouteStruct[route] = df0.append(df1, ignore_index = True)
        #make the stop IDs integers
        RouteStruct[route]['StopID'] = RouteStruct[route]['StopID'].astype(int)
    return RouteStruct
    
# Initialize MetroDataFrame
def InitializeMetroDataFrame():
    dfCols = ['PID','RouteID','DirectionNum','StopID','StopLat','StopLon','VehicleID', 'TripID','BusLat','BusLon','Datetime','Timestr','Time','Year','Month','Day','Temp','Weather','PA','AA1','AA2']
    MetroDataFrame = pd.DataFrame(columns = dfCols)
    return MetroDataFrame
    
# a function to get weather descriptor and current temperature from Open Weather Map
def GetWeather():
    # set defaults in case the API fails
    temp = 0
    weather = 'unknown'    
    try:
        f = urllib2.urlopen('http://api.openweathermap.org/data/2.5/weather?q=Washington,dc')
        json_string = f.read()
        parsed_json = json.loads(json_string)
        # convert from K to F
        temp = (parsed_json['main']['temp']-273.15)*9/5+32
        weather = parsed_json['weather'][0]['main']
        f.close()
    except Exception as e:
        print('weather API error')
    return temp, weather

# GatherMetroMoment will hit WMATA APIs to grab all bus positions and predictions for the selected routes
def GatherMetroMoment(MDF, RouteStruct, interval):
    #Grab bus positions first, once per moment
    BusPos = pd.DataFrame(json.loads(BP('70'))['BusPositions'], dtype = float)
    #Write the bus positions for fun. 
    DT = str(datetime.now())[0:10]
    filename = filepath + 'BusPositions' + DT + '.csv'
    # If the file is new, include the header, otherwise don't
    if os.path.isfile(filename):
        with open(filename, 'ab') as f:
            BusPos.to_csv(f, header = False)
    else:
        with open(filename, 'ab') as f:
            BusPos.to_csv(f)   
    
    # Get the weather
    temp, weather = GetWeather()
  
    # set up a 'skip number' to do only every nth stop
    SkipNumber = 4   
    
    # calculate how many stops we need to run, and set a seconds to wait between calls to keep calls / second low  
    CycleTime = (interval - 2.) / sum((len(RouteStruct[R]) for R in RouteStruct)) * SkipNumber
 
      
    #For each route, grab each stop prediction data and populate the MetroDataFrame  
    for R in RouteStruct:
        # iterator to help with stop skipping
        i = 0
        # for each stop
        for S in RouteStruct[R]['StopID']:
            # add to the iterator and if it is not a multiple of skipnumber, skip this iteration
            i+=1
            if i%SkipNumber != 0: continue       
            
            #grab the time at the beginning of this iteration so we can sleep at the end
            IntervalS = datetime.now()
            
            NBData = InitializeMetroDataFrame()
            # grab next bus predictions for the stop
            NBData = pd.DataFrame(json.loads(NBP(str(int(S))))['Predictions'], dtype = int)
            # filter only the predictions for the route we are calling
            NBData = NBData[NBData['RouteID']==int(R)]
            NBData['StopID'] = S
            
            # fill in the stop data with a merge (left join) on the stop id as a key, only grabbing lat and lon
            NBData = pd.merge(NBData, RouteStruct[R][['StopID','Lat','Lon']], on='StopID', how='left')
            #rename Lat and Lon to be StopLat and StopLon, and call Minutes PA while we're at it
            NBData.rename(columns={'Minutes':'PA','Lat':'StopLat','Lon':'StopLon'}, inplace=True)
            
            # fill in the bus position data with a merge (left join)  
            NBData = pd.merge(NBData, BusPos[['VehicleID','Lat','Lon','Deviation']], on='VehicleID', how='left')
            #rename Lat and Lon to be BusLat and BusLon
            NBData.rename(columns={'Lat':'BusLat','Lon':'BusLon'}, inplace=True)            
            
            # fill in day and time information
            Datetime = datetime.now()            
            timestr = str(datetime.now()).split('.')[0]
            NBData['Timestr'] = timestr
            NBData['Time'] = timestr[-8:]
            NBData['Year'] = timestr[:4]
            NBData['Month'] = timestr[5:7]
            NBData['Day'] = timestr[8:10]
            NBData['Datetime'] = Datetime
            
            #add temp and weather data to NBDAta
            NBData['Temp'] = temp
            NBData['Weather'] = weather            
            
            # make the PIDs
            #give a prediction ID larger than any so far
            if MDF.empty:
                PIDstart = 1
            else:
                PIDstart = int(max(MDF['PID'])) + 1
            NBData['PID']=range(PIDstart, PIDstart + int(len(NBData.index)))
            # concat it to the full data frame            
            MDF = pd.concat([MDF,NBData])    
            
            filename = filepath + 'MetroMetric.csv'
            #append to the output. If itis a new file, add a header
            if os.path.isfile(filename):
                with open(filename, 'ab') as f:
                    NBData.to_csv(f, header = False)
            else:
                with open(filename, 'ab') as f:
                    NBData.to_csv(f)
                
            # compute any extra time in the cyclee and pause
            Interval = datetime.now() - IntervalS
            ExtraTime = CycleTime - Interval.seconds - Interval.microseconds*1./1000000
            if ExtraTime > 0:
                time.sleep(ExtraTime)
            else:
                print('warning, time for cycle exceeded')      
      
    return MDF   


# code to test the protocol and gather some data
# initialize (run once)
MDF = InitializeMetroDataFrame()
RS = InitializeRouteStruct(['70'])

# do a cycle (run every interval, currently 30 seconds)
MDF = GatherMetroMoment(MDF,RS, 30)

# a clumsy temporary while loop to get data while I'm away
while True:
    # only run between 5 am and midnight
    if datetime.now().hour >= 5:
        MDF = InitializeMetroDataFrame()
        MDF = GatherMetroMoment(MDF, RS, 30)
    else:
        time.sleep(600)
        
    

# create a version that can run repeatedly and not rely on any variable in memory
def MetroMetricCron():
    # grab the parameters and route structure

    # run the script

    # store the parameters and route structure




# old approach
        #for index, row in MDF[MDF['PA']==0].iterrows():
        #    arrivals = (MDF['VehicleID']==row['VehicleID']) & ((MDF['TripID']==row['TripID']) & (MDF['StopID']==row['StopID']))
        #    MDF[arrivals]['AA1']=(MDF[arrivals]['Datetime'].minute-row['Datetime'].minute) 
        
          
        # Method 2: go through each bus ID and stop on route to find 
            ## DistanceThreshold = 1
            ## use google maps API to check distance  


"""  
# GatherMetroData will run continually and gather metro moments at an interval set in the call.
def GatherMetroData(interval = 30, routes = []):    
    # define hours to operate
    # times are in seconds from midnight. 5 am is 18000. 10 pm is 79200
    StartTime = datetime.time(5,0,0,0)
    EndTime = datetime.time(17,0,0,0)
    RunSeconds = EndTime.hour*60*60+EndTime.minute*60+EndTime.second-StartTime.hour*60*60-StartTime.minute*60-StartTime.second  
    
    # initial data survey
    # get route stop lists
    RouteStruct = InitializeRouteStruct(routes)

    # estimate the per second and per day calls
    # per second should be sum of stops over routes + # of routes / interval and can't be greater than 10
    # per day should be per second * 86400 (s/ day) * fraction of the day to run
    # to run 24 hours, therefore, need to have <= 0.57 calls / second
    NStops = sum((len(RouteStruct[R]) for R in RouteStruct))
    CPS = (NStops + 1)/interval
    CPD = CPS*RunSeconds

    # create a blank dataframe
    MDF = InitializeMetroDataFrame()
"""

""" CORRECTED sample code for bus positions
import httplib, urllib, base64
 
headers = {
    # Basic Authorization Sample
    # 'Authorization': 'Basic %s' % base64.encodestring('{username}:{password}'),
}
 
params = urllib.urlencode({
    # Specify your subscription key
    'api_key': 'qkk2x7wckemx2bxgs8g2sq8a',
    # Specify values for optional parameters, as needed
    #'RouteID': 'B30',
    #'Lat': '',
    #'Lon': '',
    #'Radius': '',
})
 
try:
    conn = httplib.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Bus.svc/json/jBusPositions?%s" % params, "", headers)
    response = conn.getresponse()
    data = response.read()
    print(data)
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))

"""


""" CORRECTED sample code for next bus predictions
import httplib, urllib, base64
 
headers = {
    # Basic Authorization Sample
    # 'Authorization': 'Basic %s' % base64.encodestring('{username}:{password}'),
}
 
params = urllib.urlencode({
    # Specify your subscription key
    'api_key': 'qkk2x7wckemx2bxgs8g2sq8a',
    # Specify values for the following required parameters
    'StopID': '1001195',
})
 
try:
    conn = httplib.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/NextBusService.svc/json/jPredictions?%s" % params, "", headers)
    response = conn.getresponse()
    data = response.read()
    print(data)
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror)

"""


""" CORRECTED sample code for Route Details
import httplib, urllib, base64
 
headers = {
    # Basic Authorization Sample
    # 'Authorization': 'Basic %s' % base64.encodestring('{username}:{password}'),
}
 
params = urllib.urlencode({
    # Specify your subscription key
    'api_key': 'qkk2x7wckemx2bxgs8g2sq8a',
    # Specify values for the following required parameters
    'RouteID': 'B30',
    # Specify values for optional parameters, as needed
    #'Date': '',
})
 
try:
    conn = httplib.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Bus.svc/json/jRouteDetails?%s" % params, "", headers)
    response = conn.getresponse()
    data = response.read()
    print(data)
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))
    
    
"""

""" sample code for incidents

import httplib, urllib, base64
 
headers = {
    # Basic Authorization Sample
    # 'Authorization': 'Basic %s' % base64.encodestring('{username}:{password}'),
}
 
params = urllib.urlencode({
    # Specify your subscription key
    'api_key': '',
    # Specify values for optional parameters, as needed
    #'Route': '',
})
 
try:
    conn = httplib.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Incidents.svc/json/BusIncidents?%s" % params, "", headers)
    response = conn.getresponse()
    data = response.read()
    print(data)
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))