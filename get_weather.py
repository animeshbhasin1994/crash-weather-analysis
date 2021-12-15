"""
Author : Shivam Ojha
Version : 2.1
Version Date : 15th Dec 2021
Description : This script is to scrape the weather data from
https://www.wunderground.com/history/daily/us/ny/new-york-city/KJFK/date/2021-11-18
and load into postgres db
"""

import pandas as pd
import datetime
from datetime import date
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import logging

from selenium.webdriver.chrome.options import Options

HISTORICAL_DATA_FLG = True

def download_file(link):
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(options=options)
    # driver = webdriver.Firefox(firefox_binary='/usr/bin/firefox-esr')
    driver.get(link)
    time.sleep(3)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    driver.close()
    return soup

def scrape_file(soup):
    try:
        dateString_list = []
        temperature_list = []
        dewPoint_list = []
        humidity_list = []
        windcardinal_list = []
        windSpeed_list = []
        windGust_list = []
        pressure_list = []
        precipRate_list = []
        condition_list = []

        for row in soup.find_all("td", {'class':"mat-cell cdk-cell cdk-column-dateString mat-column-dateString ng-star-inserted"}):
            dateString_list.append(row.text.split()[0])

        for row in soup.find_all("td", {'class':"mat-cell cdk-cell cdk-column-temperature mat-column-temperature ng-star-inserted"}):
            temperature_list.append(row.text.split()[0])

        for row in soup.find_all("td", {'class':"mat-cell cdk-cell cdk-column-dewPoint mat-column-dewPoint ng-star-inserted"}):
            dewPoint_list.append(row.text.split()[0])

        for row in soup.find_all("td", {
            'class': "mat-cell cdk-cell cdk-column-humidity mat-column-humidity ng-star-inserted"}):
            humidity_list.append(row.text.split()[0])

        for row in soup.find_all("td", {
            'class': "mat-cell cdk-cell cdk-column-windcardinal mat-column-windcardinal ng-star-inserted"}):
            windcardinal_list.append(row.text.split()[0])

        for row in soup.find_all("td",
                                {'class': "mat-cell cdk-cell cdk-column-windSpeed mat-column-windSpeed ng-star-inserted"}):
            windSpeed_list.append(row.text.split()[0])
        for row in soup.find_all("td", {
            "mat-cell cdk-cell cdk-column-windGust mat-column-windGust ng-star-inserted"}):
            windGust_list.append(row.text.split()[0])

        for row in soup.find_all("td", {
            'class': "mat-cell cdk-cell cdk-column-pressure mat-column-pressure ng-star-inserted"}):
            pressure_list.append(row.text.split()[0])

        for row in soup.find_all("td",
                                {'class':  "mat-cell cdk-cell cdk-column-precipRate mat-column-precipRate ng-star-inserted"}):
            precipRate_list.append(row.text.split()[0])

        for row in soup.find_all("td",
                                {'class': "mat-cell cdk-cell cdk-column-condition mat-column-condition ng-star-inserted"}):
            condition_list.append(row.text.split()[0])

        d = {'Hour': dateString_list, 'Temp':temperature_list, 'Dew' : dewPoint_list, 'humidity' :humidity_list,
            'Wind Cardinal' :windcardinal_list, 'Wind Speed' : windSpeed_list, 'Wind Gust': windGust_list,
            'Pressure List': pressure_list, 'Precip Rate': precipRate_list, 'Condition':condition_list}
        df = pd.DataFrame(d)
        return df
    except IndexError:
        return None


def generate_links(yesterday_date):
    link_format = "https://www.wunderground.com/history/daily/us/ny/new-york-city/{}/date/{}-{}-{}"
    days_in_month = {1: 31, 2: 28, 3: 31, 4: 30,
                  5: 31, 6: 30, 7: 31, 8: 31,
                  9: 30, 10: 31, 11: 30, 12: 31}
    
    days_in_month_leap = {1: 31, 2: 29, 3: 31, 4: 30,
                  5: 31, 6: 30, 7: 31, 8: 31,
                  9: 30, 10: 31, 11: 30, 12: 31}

    links = []
    for year in range(2014, 2021):
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
                links.append(link_format.format('KJFK', year, month, day))
    return links


def generate_incremental_links(given_date):
    link_format = "https://www.wunderground.com/history/daily/us/ny/new-york-city/{}/date/{}"

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    today_obj = datetime.datetime.strptime(today_str, "%Y-%m-%d")

    links = []
    links.append(link_format.format('KJFK', given_date))
    date_obj = datetime.datetime.strptime(given_date, "%Y-%m-%d")
    while date_obj < today_obj:
        date_obj = date_obj + datetime.timedelta(days=1)
        date_str = date_obj.strftime("%Y-%m-%d")
        links.append(link_format.format('KJFK', date_str))

    return links



def get_date_max_date_in_db(engine, schema_name, table_name):
    #sql_statement = '''select max(Date + interval '1' day) from {}.{};'''.format(schema_name, table_name)
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
    weather_table_name = 'hourly_weather_data'
    engine = create_engine(database_url, echo=False)

    yesterday = date.today() - datetime.timedelta(days=1)
    yes_date_str = yesterday.strftime("%Y-%m-%d")
    yesterday_obj = datetime.datetime.strptime(yes_date_str, "%Y-%m-%d")
    
    #for station in ('KJFK'):
    if HISTORICAL_DATA_FLG: 
        #load data from 2015 to now
        links = generate_links(yesterday_obj)
    else:
        #load incremental data, compared to last date in db
        max_date = get_date_max_date_in_db(engine, schema_name, weather_table_name)
        links = generate_incremental_links(str(max_date))

    for i, link in enumerate(links):
        soup = download_file(link)
        df = scrape_file(soup)
        if isinstance(df, pd.core.frame.DataFrame):
            df['Date'] = link.split('/')[-1]
            df['station'] = 'KJFK'
            print("Storing values for date: %s", link.split('/')[-1])

            write_to_db(df, table_name= weather_table_name,schema_name = schema_name,engine= engine,date_list_str= link.split('/')[-1],
                        station = 'KJFK')


def write_to_db(df, table_name, schema_name, engine, date_list_str, station):
    # Remove duplicates
    sql_statement = '''delete from {}.{} where {}."Date" in ('{}')'''.format(schema_name, table_name, table_name,
                                                                          date_list_str)
    engine.execute(sql_statement)
    df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False, method='multi')


if __name__ == '__main__':
    main()
