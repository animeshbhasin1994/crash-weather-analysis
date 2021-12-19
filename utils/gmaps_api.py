#!/usr/bin/env python
# coding: utf-8

# In[134]:


import json
import googlemaps
from datetime import datetime

gmaps = googlemaps.Client(key='AIzaSyCe3u-TDSAFjSJztX5AlaMXKrGjmuI2l5s')


def get_zipcode(df, gmaps, lat_field, lon_field):
    reverse_geocode_result = gmaps.reverse_geocode((df[lat_field], df[lon_field]))
    s1 = json.dumps(reverse_geocode_result)
    address_response = json.loads(s1)
    if address_response:
        get_zipcode = address_response[0]['address_components'][-1]['long_name']
        return get_zipcode
    return None


def get_lat_long(row, gmaps):
    zipcode = row['zip_code']
    geocode_result = gmaps.geocode(zipcode)

    get_lat, get_long = 0.0, 0.0
    if geocode_result:
        for i in geocode_result[0]['geometry']['bounds']:
            get_lat = geocode_result[0]['geometry']['bounds'][i]['lat']
            get_long = geocode_result[0]['geometry']['bounds'][i]['lng']
            break
    return get_lat, get_long