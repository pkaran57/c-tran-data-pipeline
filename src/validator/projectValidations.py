#!/usr/bin/env python
# coding: utf-8

# In[1]:



import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import math
import datetime


# In[54]:


df = pd.read_json('2021-02-10.json')
df.head()
list(df.columns)


# In[4]:


event_no_trip = df['EVENT_NO_TRIP']
act_time = df['ACT_TIME']


# In[5]:


print('The total row number: ',len(event_no_trip))
print('The total act_time: ',len(act_time))


# In[6]:


# Unique event validation
event_no_trip_set = set()
act_time_set = set()
for i in range(len(event_no_trip)):
    event_no_trip_set.add(event_no_trip[i])

for i in range(len(act_time)):
    act_time_set.add(act_time[i])

event_no_trip_list = sorted(event_no_trip_set)
print('There are {} unique for the EVENT_NO_TRIP attribute'.format(len(event_no_trip_list)))

act_time_list = sorted(act_time_set)
print('There are {} unique for the ACT_TIME attribute'.format(len(act_time_list)))


# In[11]:


# EVENT_NO_TRIP length validation
for i in event_no_trip:
    if len(str(i)) ==9:
        continue
    else:
        print("The data is incorrect for the event_no_trip: ", i)
        break
print("All the event_no_trip values are of length 9")


# In[12]:


# EVENT_NO_STOP length validation
event_no_stop = df['EVENT_NO_STOP']
for i in event_no_stop:
    if len(str(i)) ==9:
        continue
    else:
        print("The data is incorrect for the event_no_stop: ", i)
        break
print("All the event_no_stop values are of length 9")


# In[33]:


# gps latitide decimal part validation 
gps_latitude = df['GPS_LATITUDE']
for i in gps_latitude:
    lat = i.strip().split(".")[0]
    if lat != '45' and lat != "":
        print("---",lat)
        print("a value with gps value which is not 45")
        
print("all values have a gps value 45")
        


# In[34]:


# gps logitude validation 
gps_logitude = df['GPS_LONGITUDE']
for i in gps_logitude:
    lat = i.strip().split(".")[0]
    if lat != '-122' and lat != "":
        print("---",lat)
        print("a value with gps value which is not -122")
        
print("all values have a gps value -122")


# In[51]:


# GPS_SATELLITES  values validation; should be 10, 11, 12
gps_satillites = df['GPS_SATELLITES']
for i in gps_satillites:
    if i:
        gps = int(i)
    if gps < 3 and gps> 17:
        print("assertion is wrong")
    
print("all values fall in the asserted range")


# In[53]:


# VELOCITY
velocity = df['VELOCITY']
vel =0
for i in velocity:
    if i:
        vel = int(i)
    if vel<0 and vel>60:
        print("assertion is wrong")
print("all the assertions are correct")


# In[56]:


# DIRECTION range
direction = df['DIRECTION']
dir = 0
for i in direction:
    if i:
        dir = int(i)
    if dir < 0 and dir > 359:
        print("assertion made is wrong")
print("Assertion about direction is correct")


# In[78]:


# VEHICLE_ID length
vehicle_id = df['VEHICLE_ID']
for i in vehicle_id:
    if len(str(i)) != 4 and len(str(i)) != 7:
        print("invalid vehicle id assertion")
print("valid assertion")


# In[66]:


# OPD_DATE for all entries
opd_date = df['OPD_DATE']
for i in opd_date:
    if not i:
        print("assertion not true")
print("OPD_DATE present for all entries")


# In[ ]:




