'''
Author : Animesh Bhasin
Version : 1
Version Date : 16th Nov 2022
Description : This script is to preprocess the crashes data
'''

import requests
import get_crashes
import pandas as pd
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine
import geopy
import json
import googlemaps

def main():
    print ("Script started at " + str(datetime.now()))
    database_url = 'postgresql+psycopg2://postgres:postgres@34.69.230.53/bda'
    schema_name = 'crash'
    crashes_table_name = 'crashes_clean'
    engine = create_engine(database_url, echo=False)

    crash_start_date = get_crashes.get_date_max_date_in_db(engine, schema_name, crashes_table_name)

    date_list = get_crashes.get_list_of_dates_to_process(crash_start_date)
    # start_date = end_date = '2021-12-22'
    # start_date = '2021-11-09'
    # end_date = '2021-11-15'
    start_date = date_list[0]
    end_date = date_list[-1]

    sql_statement = '''select * from crash.crashes where date(crash_date) between '{}' and '{}';'''.format(start_date,
                                                                                                          end_date)

    df = pd.read_sql_query(sql_statement, database_url)

    gmaps = googlemaps.Client(key='AIzaSyCe3u-TDSAFjSJztX5AlaMXKrGjmuI2l5s')
    if not df.empty:
        '''Fill missing zipcode'''
        mask = (df['zip_code'].isna()) & (df['location_latitude'] != 0) & (df['location_latitude'].notna())

        if mask.any():
            df.loc[mask, 'zip_code'] = df.loc[mask].apply \
                (get_zipcode, axis=1, gmaps = gmaps, lat_field='location_latitude', lon_field='location_longitude')

        '''Fill missing lat long'''
        mask = ((df['location_latitude'].isna()) | (df['location_latitude'] == '0')) & (df['zip_code'].notna())
        if mask.any():
            df.loc[mask, ['location_latitude', 'location_longitude']] = df.loc[mask].apply(get_lat_long, axis=1, gmaps = gmaps).tolist()

        '''Write data to clean table'''
        write_to_db(engine, df, collision_id_list_str=get_collision_id_list(df), schema_name='crash',
                    table_name='crashes_clean')
    print ("Script ended at " + str(datetime.now()))

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

def get_collision_id_list(df):
    collision_id_list = df['collision_id'].tolist()

    collision_id_list_str = str(collision_id_list)[1:-1]

    return collision_id_list_str


def write_to_db(engine, df, collision_id_list_str, schema_name, table_name):
    sql_statement = 'delete from {}.{} where collision_id in ({})'.format(schema_name, table_name,
                                                                          collision_id_list_str)

    engine.execute(sql_statement)
    df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    main()
