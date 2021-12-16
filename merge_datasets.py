"""
Author : Shivam Ojha
Version : 1
Version Date : 15th Dec 2021
Description : This script merges the two datasets, so that
analysis can be performed
"""
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def main():
    """
    Script to merge the scraped weather data and crash datasets
    """
    print ("Script started at " + str(datetime.now()))
    database_url = 'postgresql+psycopg2://postgres:postgres@34.69.230.53/bda'
    engine = create_engine(database_url, echo=False)
    
    # Weather data
    weather_schema_name = 'weather'
    weather_table_processed = 'weather_data_processed'

    sql_statement = '''select * from {}.{}'''.format(weather_schema_name, weather_table_processed)
    weather_df = pd.read_sql_query(sql_statement, database_url)
    weather_df['Datetime'] = pd.to_datetime(weather_df['Datetime'])

    # Crash Data
    crash_schema_name = 'crash'
    crash_table_processed = 'crashes_clean'
    sql_statement = '''select * from {}.{}'''.format(crash_schema_name, crash_table_processed)
    crash_df = pd.read_sql_query(sql_statement, database_url)
    #Creating crash_datetime column
    crash_df['crash_date'] = pd.to_datetime(crash_df['crash_date'])
    crash_df['crash_datetime'] = pd.to_datetime(crash_df['crash_date'].apply(str)+' '+crash_df['crash_time'])
    crash_df['crash_datetime'] = pd.to_datetime(crash_df['crash_datetime'])

    # Sort dataframes based on datetime values
    crash_df.sort_values('crash_datetime', inplace=True)
    weather_df.sort_values('Datetime', inplace=True)

    # Remove duplicate data columns
    del weather_df['Date']
    del weather_df['Hour']    

    # Appending tables
    merged_dataframe = pd.merge_asof(crash_df, weather_df, left_on="crash_datetime", 
                                 right_on="Datetime", direction='nearest')

    # Update merged table in db
    merged_dataframe.to_sql(name='merged_data_table', schema='merged_data', con=engine,
           if_exists='replace', index=False, method='multi')

    print ("Script completed at " + str(datetime.now()))

if __name__ == '__main__':
    main()
