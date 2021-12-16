"""
Author : Shivam Ojha
Version : 1
Version Date : 27th Nov 2021
Description : Airflow orchestration DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.operators.python_operator import PythonOperator

#seven_days_ago = datetime.combine(datetime.today() - timedelta(3),
#                                  datetime.min.time())

default_args = {
    'owner': 'Shivam',
    'depends_on_past': False,
    #'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'NYC_Vehicle_Crash_Analysis_and_Prediction',
    default_args=default_args,
    description='DAG for Big Data Analytics Final Project',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 11, 10, 7, 00, 00),
    catchup=False,
    tags=['Project']
) as dag:

    # t* examples of tasks created by instantiating operators

    t1 = BashOperator(
        task_id='fetch_crash_data',
        bash_command='python3 /home/so2639/crash-weather-analysis/get_crashes.py',
        dag=dag,
    )

    t2 = BashOperator(
        task_id='webscrape_weather_data',
        bash_command='python3 /home/so2639/crash-weather-analysis/get_weather.py',
        dag=dag
    )

    t3 = BashOperator(
        task_id='preprocess_crash_data',
        bash_command='python3 /home/so2639/crash-weather-analysis/preprocess_crash_data.py',
        dag=dag
    )

    t4 = BashOperator(
        task_id='preprocess_weather_data',
        bash_command='python3 /home/so2639/crash-weather-analysis/preprocess_weather_data.py',
        dag=dag
    )

    t5 = BashOperator(
        task_id='merge_datasets',
        bash_command= 'python3 /home/so2639/crash-weather-analysis/merge_datasets.py',
        dag=dag
    )

    t6 = BashOperator(
        task_id='temperature_prediction',
        bash_command='echo "Function to be added"',
        #bash_command= 'python3 /home/so2639/crash-weather-analysis/predict_weather.py',
        dag=dag
    )

    t7 = BashOperator(
        task_id='crash_analysis_and_prediction',
        bash_command='echo "Function to be added"',
        #bash_command='python3 /home/so2639/crash-weather-analysis/predict_crash.py',
        dag=dag
    )

    t8 = BashOperator(
        task_id='Visualization',
        bash_command='echo "Function to be added"',
        #bash_command= 'python3 /home/so2639/crash-weather-analysis/visualisation.py',
        dag=dag
    )

    # task dependencies
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5
    t5 >> t6
    t6 >> t7
    t7 >> t8
