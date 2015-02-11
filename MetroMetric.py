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
Temp:
Weather: a description of weahter
PA: predicted arrival: When the bus is predicted to arrive at this stop
AA1: actual arrival 1: When the bus actually arrived (filled in on later calls), determined by status "arriving"
AA2: actual arrival 2: When the bus actually arrived (filled in on later calls), determined by close lat - lon from position data

['PID','RouteID','Direction','StopID','StopLat','StopLon','VehicleID', 'TripID','BusLat','BusLon','Datetime','Timestr','Time','Year','Month','Day','Temp','Weather','PA','AA1','AA2']

"""

import json
import httplib, urllib, base64
import numpy as np
import pandas as pd
import datetime

# my API key from WMATA
api_key = 'qkk2x7wckemx2bxgs8g2sq8a'

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
        conn = httplib.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/NextBusService.svc/json/jPredictions?%s" % params, "", headers)
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

# GatherMetroMoment will hit WMATA APIs to grab all bus positions and predictions for the selected routes
def GatherMetroMoment(MDF, RouteStruct):
    #Grab bus positions first, once per moment
    BusPos = pd.DataFrame(json.loads(BP())['BusPositions'], dtype = float)
  
    #For each route, grab each stop prediction data and populate the MetroDataFrame  
    for R in RouteStruct:
        # optionally, filter for every X stops
        ## ASK
        # for each stop
        for S in RouteStruct[R]['StopID']:
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
            NBData = pd.merge(NBData, BusPos[['VehicleID','Lat','Lon']], on='VehicleID', how='left')
            #rename Lat and Lon to be BusLat and BusLon
            NBData.rename(columns={'Minutes':'PA','Lat':'BusLat','Lon':'BusLon'}, inplace=True)            
            
            # fill in day and time information
            Datetime = datetime.datetime.now()            
            timestr = str(datetime.datetime.now()).split('.')[0]
            NBData['Timestr'] = timestr
            NBData['Time'] = timestr[-8:]
            NBData['Year'] = timestr[:4]
            NBData['Month'] = timestr[5:7]
            NBData['Day'] = timestr[8:10]
            NBData['Datetime'] = Datetime
            
            # Check if there are any arrivals we can fill in
            # Method 1: look for times of zero. Remove from the predictions database, and go backwards to find
            # predictions within a set time for this tripID
          
            #for Arrival in NBData[NBData['PA']==0]:
            #    # use the vehicle, trip, and stop IDs to go back through to fill in using the arrival time
            #    arrivals = MDF['VehicleID']==Arrival['VehicleID']&MDF['TripID']==Arrival['TripID']&MDF['StopID']==Arrival['StopID']
            #    MDF[arrivals]['AA1']=((MDF[arrivals]['Datetime']-Arrival['Datetime']).seconds/60)         
            # Method 2: go through each bus ID and stop on route to find 
            ## DistanceThreshold = 1
            ## use google maps API to check distance            
            
            # make the PIDs
            #give a prediction ID larger than any so far
            if MDF.empty:
                PIDstart = 1
            else:
                PIDstart = int(max(MDF['PID'])) + 1
            NBData['PID']=range(PIDstart, PIDstart + int(len(NBData.index)))
            # concat it to the full data frame            
            MDF = pd.concat([MDF,NBData])    
    
    return MDF
    
# GatherMetroData will run continually and gather metro moments at an interval set in the call.
def GatherMetroData(interval = 30, routes = []):
    # initial data survey
    # get route stop lists
    RouteStruct = InitializeRouteStruct(routes)

    # estimate the per second and per day calls
    # per second should be sum of stops over routes + # of routes / interval and can't be greater than 10
    # per day should be per second * 86400 (s/ day) * fraction of the day to run
    # to run 24 hours, therefore, need to have <= 0.57 calls / second

    # create a blank dataframe
    MDF = InitializeMetroDataFrame()

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