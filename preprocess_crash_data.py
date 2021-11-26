'''
Author : Animesh Bhasin
Version : 1
Version Date : 16th Nov 2022
Description : This script is to preprocess the crashes data
'''

import requests
import get_crashes
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine
import geopy


def main():

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

    geolocator = geopy.Nominatim(user_agent='hello')
    if not df.empty:
        '''Fill missing zipcode'''
        mask = (df['zip_code'].isna()) & (df['location_latitude'] != 0)
        if mask.any():
            df.loc[mask, 'zip_code'] = df.loc[mask].apply \
                (get_zipcode, axis=1, geolocator=geolocator, lat_field='location_latitude', lon_field='location_longitude')

        '''Fill missing lat long'''
        mask = ((df['location_latitude'].isna()) | (df['location_latitude'] == '0')) & (df['zip_code'].notna())
        if mask.any():
            df.loc[mask, ['location_latitude', 'location_longitude']] = df.loc[mask].apply(get_lat_long, axis=1).tolist()

        '''Write data to clean table'''
        write_to_db(engine, df, collision_id_list_str=get_collision_id_list(df), schema_name='crash',
                    table_name='crashes_clean')


def get_zipcode(df, geolocator, lat_field, lon_field):
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    if location and 'postcode' in location.raw['address']:
        zip_code = location.raw['address']['postcode']
        if '-' in zip_code:
            return zip_code.split('-')[0]
        elif ':' in zip_code:
            return zip_code.split(':')[0]
        else:
            return zip_code


def get_lat_long(row):
    geolocator = geopy.Nominatim(user_agent='geoapiExercises')
    zipcode = row['zip_code']
    location = geolocator.geocode(zipcode)
    if location and 'lat' in location.raw and 'lon' in location.raw:
        return [location.raw['lat'], location.raw['lon']]
    return ['-1', '-1']


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
