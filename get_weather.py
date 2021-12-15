"""
Author : Shivam Ojha
Version : 3
Version Date : 15th Dec 2021
Description : This script is to scrape the weather data from
https://www.wunderground.com/history/daily/us/ny/new-york-city/KJFK/date/2021-11-18
and load into postgres db
"""
import requests
import pandas as pd
import datetime
from datetime import date
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

HISTORICAL_DATA_FLG = True

def format_data(date):
    date = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')
    # The API key is kept static for now
    url = f'https://api.weather.com/v1/location/KJFK:9:US/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=e&startDate={date}&endDate={date}'
    response = requests.get(url).json()
    mappings = { 'Hour': 'hour', "Date": 'date', 'Temp': 'temp', 'Dew': 'dewPt', 'humidity': 'rh', 'Wind Cardinal': 'wdir_cardinal', 'Wind Speed': 'wspd', 'Wind Gust': 'gust', 'Pressure List': 'pressure', 'Precip Rate': 'precip_hrly', 'Condition': 'wx_phrase'}
    formatted_object = []
    for tuple in response['observations']:
        timestamp = tuple['valid_time_gmt']
        date = datetime.datetime.fromtimestamp(timestamp)
        tuple['date'] = date.strftime("%d %b %Y")
        tuple['hour'] = date.strftime("%I:%M %p")
        formatted_tuple = {}
        for el in mappings.keys():
            formatted_tuple[el] = tuple[mappings[el]] if tuple[mappings[el]] else 0
        formatted_object.append(formatted_tuple)
    transposed_object = {}

    for el in mappings.keys():
        temp_list = list()
        for tuple in formatted_object:
            temp_list.append(tuple[el])
        transposed_object[el] = temp_list

    return transposed_object

def generate_dates(yesterday_date):
    days_in_month = {1: 31, 2: 28, 3: 31, 4: 30,
                  5: 31, 6: 30, 7: 31, 8: 31,
                  9: 30, 10: 31, 11: 30, 12: 31}
    days_in_month_leap = {1: 31, 2: 29, 3: 31, 4: 30,
                  5: 31, 6: 30, 7: 31, 8: 31,
                  9: 30, 10: 31, 11: 30, 12: 31}
    dates_list = []
    for year in range(2014, 2022):
        for month in range(1, 13):
            if year % 4 == 0:
                day_cnt = days_in_month_leap[month]
            else:
                day_cnt = days_in_month[month]
            for day in range(1, day_cnt+1):
                date_str = "{}-{}-{}".format(year, month, day)
                date_object = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                if date_object > yesterday_date:
                    break
                dates_list.append(date_str)
    return dates_list


def generate_incremental_links(given_date):
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    today_obj = datetime.datetime.strptime(today_str, "%Y-%m-%d")
    dates = []
    dates.append(given_date)
    date_obj = datetime.datetime.strptime(given_date, "%Y-%m-%d")
    while date_obj < today_obj:
        date_obj = date_obj + datetime.timedelta(days=1)
        date_str = date_obj.strftime("%Y-%m-%d")
        dates.append(date_str)
    return dates


def get_date_max_date_in_db(engine, schema_name, table_name):
    sql_statement = '''select max({}."Date") from {}.{};'''.format(table_name, schema_name, table_name)
    with engine.connect() as connection:
        result = connection.execute(sql_statement)
        for row in result:
            max_date = row[0]
    return max_date


def main():
    # Database credentials
    database_url = 'postgresql+psycopg2://postgres:postgres@34.69.230.53/bda'
    schema_name = 'weather'
    weather_table_name = 'weather_data'
    engine = create_engine(database_url, echo=False)

    yesterday = date.today() - datetime.timedelta(days=1)
    yes_date_str = yesterday.strftime("%Y-%m-%d")
    yesterday_obj = datetime.datetime.strptime(yes_date_str, "%Y-%m-%d")
    
    #for station in ('KJFK'):
    if HISTORICAL_DATA_FLG: 
        #load data from 2015 to now
        dates = generate_dates(yesterday_obj)
    else:
        #load incremental data, compared to last date in db
        max_date = get_date_max_date_in_db(engine, schema_name, weather_table_name)
        dates = generate_incremental_links(str(max_date))
    for d1 in dates:
        df = pd.DataFrame(format_data(d1))
        if isinstance(df, pd.core.frame.DataFrame):
            df['Date'] = d1
            df['station'] = 'KJFK'
            print("Storing values for date: {}".format(d1))

            write_to_db(df, table_name= weather_table_name,schema_name = schema_name,engine= engine,date_list_str= d1)


def write_to_db(df, table_name, schema_name, engine, date_list_str):
    # Remove duplicates, if any
    sql_statement = '''delete from {}.{} where {}."Date" in ('{}')'''.format(schema_name, table_name, table_name,
                                                                         date_list_str)
    engine.execute(sql_statement)
    df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False, method='multi')


if __name__ == '__main__':
    main()
