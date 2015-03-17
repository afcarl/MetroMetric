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
import matplotlib.pyplot as plt
import time

# load the full csvs

#from 3/5
with open('../../MetroMetric/MetroMetric2.csv', 'rb') as f:
    DF1 = pd.read_csv(f)

# from 3/8 - 3/9
with open('../../MetroMetric/MetroMetric3.csv', 'rb') as f:
    DF2 = pd.read_csv(f)

# from 3/10
with open('../../MetroMetric/MetroMetric.csv', 'rb') as f:
    DF3 = pd.read_csv(f)

# from 3/11
with open('../../MetroMetric/MetroMetric312.csv', 'rb') as f:
    DF4 = pd.read_csv(f)

DF = pd.concat([DF1,DF2,DF3,DF4])
#combine the data frames

# data processing script.
# split the data into predictions and arrivals
# Method 1: look for times of zero. Remove from the predictions database, and go backwards to find

DF_A = DF[DF['PA']==0]
DF_P = DF[DF['PA']!=0]

# to get the actual time as a column in the prediction database do a join on trip ID and the date
DF_A.rename(columns={'Datetime':'ArrivalTime1'}, inplace=True)
DF_C = pd.merge(DF_P,DF_A[['TripID','StopID','ArrivalTime1']], on=['TripID','StopID'], how = 'left')

# calculate the difference between Datetime (the time of the prediction) and ArrivalTime (the time of the actual arrival)
temparray=[np.nan]*len(DF_C)
for i, row in DF_C.iterrows():
    if i % 50000 == 0: print(i)
    if type(row['ArrivalTime1']) == str:
        temparray[i] = float(row['ArrivalTime1'][11:13])*60 + float(row['ArrivalTime1'][14:16])+float(row['ArrivalTime1'][17:19])/60 - (float(row['Time'][0:2])*60.+float(row['Time'][3:5])+float(row['Time'][6:8])/60)
DF_C['AA1'] = temparray
DF_C['Error'] = DF_C['AA1']-DF_C['PA']

#create a 'bad weather' dummy variable for rain and snow (use 2 for snow)
DF_C['BadWeather'] = DF_C.Weather.map({'Snow':2, 'Rain':1, 'Mist':1, 'Clouds':0, 'Clear':0,'unknown':0})

# calculate prediction 'qualities'
# use a |predicted - actual|/actual

DF_C['Q'] = 1 - abs(DF_C['PA']-DF_C['AA1'])/DF_C['AA1']

# make a time type variable
temparray=[np.nan]*len(DF_C)
for i, row in DF_C.iterrows():
    if i % 50000 == 0: print(i)
    if type(row['Time']) == str:
        temparray[i] = int(DF_C['Time'][i][0:2])*60*60+int(DF_C['Time'][i][3:5])*60+int(DF_C['Time'][i][6:8])
DF_C['Secs'] = temparray

# remove negative arrivals
DF_R = DF_C[DF_C['AA1']>0]

DF_fit = DF_R[DF_R.PA.notnull()&DF_R.Deviation.notnull()&DF_R.BadWeather.notnull()&DF_R.Secs.notnull()]

# do regression to predict AA1 using PA and other possible influencers
# create X and y
feature_cols = ['PA', 'Deviation', 'BadWeather', 'Secs']
X = DF_fit[feature_cols]
y = DF_fit['AA1']

# follow the usual sklearn pattern: import, instantiate, fit
from sklearn.cross_validation import train_test_split
from sklearn.linear_model import LinearRegression
lm = LinearRegression()
lm.fit(X, y)

# print intercept and coefficients
print lm.intercept_
print lm.coef_
zip(feature_cols, lm.coef_)
lm.score(X, y)

# do regression with train test split
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=4)
lm.fit(X_train, y_train)

# make new predictions
DF_fit['Lin_PA'] = lm.predict(DF_fit[feature_cols])

# calculate errors and "qualities" for the updated predictions
DF_fit['Error2']=DF_C['AA1']-DF_fit['Lin_PA']
DF_fit['Q2'] = (1-abs(DF_fit['Lin_PA']-DF_fit['AA1'])/DF_fit['AA1'])


# graph results

#scatter plot of prediction versus actual arrival


plt.scatter(DF_R['PA'], DF_R['AA1'], marker = '.', alpha= 0.01)
plt.xlabel("Predicted Arrival (min)")
plt.ylabel("Actual Arrival (min)")
plt.show()

#scatter of only the near-term predictitons
plt.scatter(DF_R[DF_R['PA']<10]['PA'], DF_R[DF_R['PA']<10]['AA1'], marker='o', alpha = 0.01)
plt.show()

plt.hist(DF_R['Error'], bins = 100, range = [-10,40])
plt.hist(DF_fit['Error2'], bins = 100, range = [-10,40])
plt.hist(DF_R['Q'],bins = 100, range = [0,1])
plt.hist(DF_fit['Q2'], bins = 100, range = [0,1])
plt.hist(DF_R[DF_R['PA']<10]['Error'], bins = 100, range = [-10,40])
plt.hist(DF_R[DF_R['Weather']=='Snow']['Error'], bins = 100, range = [-20,80])

#scatters of interesting cases and errors
# box plot of error by weather 
DF_R.boxplot(column='Error', by = 'Weather')

#scatter of error by deviation
plt.scatter(DF_R['Deviation'],DF_R['Error'], marker = 'o', alpha = 0.01)
plt.xlabel("Deviation (min)")
plt.ylabel("Prediction Error (min)")
plt.show()

# scatter of error by time of day
plt.scatter(DF_R['Secs'],DF_R['Error'], marker = 'o', alpha = 0.004)
plt.xlabel("Time")
plt.ylabel("Prediction Error (min)")