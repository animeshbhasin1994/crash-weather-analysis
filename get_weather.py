"""
Author : Animesh Bhasin
Version : 1
Version Date : 16th Nov 2022
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

from selenium.webdriver.firefox.options import Options

def download_file(link):
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)
    # driver = webdriver.Firefox(firefox_binary='/usr/bin/firefox-esr')
    driver.get(link)
    time.sleep(3)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    driver.close()
    return soup

def scrape_file(soup):

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

def main():
    database_url = 'postgresql+psycopg2://postgres:postgres@34.69.230.53/bda'
    schema_name = 'weather'
    weather_table_name = 'hourly'
    engine = create_engine(database_url, echo=False)

    yesterday = date.today() - datetime.timedelta(days=1)
    yes_date_str = yesterday.strftime("%Y-%m-%d")

    link_format = "https://www.wunderground.com/history/daily/us/ny/new-york-city/{}/date/{}-{}-{}"
    for station in ('KJFK','KLGA'):

    # links = [link_format.format(station, year, month, day)
    #          for year in range(2021, 2022)
    #          for month in range(1, 2)
    #          for day in range(1, 3)]
        year = yes_date_str.split('-')[0]
        month = yes_date_str.split('-')[1]
        day = yes_date_str.split('-')[-1]
        links = [link_format.format(station, year, month, day)]
        for i, link in enumerate(links):
            soup = download_file(link)
            df = scrape_file(soup)
            df['Date'] = yes_date_str
            df['station'] = station

            write_to_db(df, table_name= weather_table_name,schema_name = schema_name,engine= engine,date_list_str= yes_date_str,
                        station = station)

def write_to_db(df, table_name, schema_name, engine, date_list_str, station):
    sql_statement = '''delete from {}.{} where "Date" in ('{}') and station = '{}';'''.format(schema_name, table_name,
                                                                          date_list_str, station)
    engine.execute(sql_statement)
    df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False)
if __name__ == '__main__':
    main()


