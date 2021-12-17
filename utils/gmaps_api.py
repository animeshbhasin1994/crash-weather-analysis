#!/usr/bin/env python
# coding: utf-8

# In[134]:


import json
import googlemaps
from datetime import datetime

gmaps = googlemaps.Client(key='AIzaSyCe3u-TDSAFjSJztX5AlaMXKrGjmuI2l5s')


def get_latlong(zipcode):
    
    geocode_result = gmaps.geocode(zipcode)
    
    get_lat, get_long = 0.0,0.0

    for i in geocode_result[0]['geometry']['bounds']:
        get_lat = geocode_result[0]['geometry']['bounds'][i]['lat']
        get_long = geocode_result[0]['geometry']['bounds'][i]['lng']

        break
        
    return get_lat,get_long



def get_zipcode(lat,long):
    reverse_geocode_result = gmaps.reverse_geocode((lat, long))
    
    s1 = json.dumps(reverse_geocode_result )
    address_response = json.loads(s1)

    get_zipcode = address_response[0]['address_components'][-1]['long_name']
    
    return get_zipcode
    

