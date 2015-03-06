# -*- coding: utf-8 -*-
"""
MetroMetric Analysis
Created on Wed Mar 04 18:43:21 2015

@author: abrown1

Loads a MetroMetric dataframe of bus predictions. Identifies the arrivals and removes them from the database
then uses a join to get the actual times into the predictions where there is a match
then a time difference will give the actual arrival

"""
import numpy as np
import pandas as pd
from datetime import datetime

# load the full csv
with open('../../MetroMetric/MetroMetric.csv', 'rb') as f:
    DF = pd.read_csv(f)

# data processing script.
# split the data into predictions and arrivals
# Method 1: look for times of zero. Remove from the predictions database, and go backwards to find

DF_A = DF[DF['PA']==0]
DF_P = DF[DF['PA']!=0]

# to get the actual time as a column in the prediction database do a join on trip ID and the date
DF_A.rename(columns={'Datetime':'ArrivalTime1'}, inplace=True)
DF_P = pd.merge(DF_P,DF_A[['TripID','Year','Month','Day','ArrivalTime1']], on=['TripID','Year','Month','Day'], how = 'left')

# calculate the difference between Datetime (the time of the prediction) and ArrivalTime (the time of the actual arrival)
DF_P['AA1'] = 

        # predictions within a set time for this tripID
# use the vehicle, trip, and stop IDs to go back through to fill in using the arrival time
