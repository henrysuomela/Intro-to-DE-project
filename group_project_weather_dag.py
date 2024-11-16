import pandas as pd
import numpy as np
import sqlite3
import os
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi

# Values can be changed
default_args = {
    'owner': 'Team_6',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'group_project_weather_dag',
    default_args = default_args,
    # Runs daily
    schedule_interval = '@daily'
)






def determine_wind_strength(wind_speed):
    # Converts km/h to m/s and categorizes the wind strength 
    wind_speed_m_per_s = wind_speed / 3.6

    if wind_speed_m_per_s <= 1.5:
        return 'Calm'
    elif 1.6 <= wind_speed_m_per_s <= 3.3:
        return 'Light Air'
    elif 3.4 <= wind_speed_m_per_s <= 5.4:
        return 'Light Breeze'
    elif 5.5 <= wind_speed_m_per_s <= 7.9:
        return 'Gentle Breeze'
    elif 8.0 <= wind_speed_m_per_s <= 10.7:
        return 'Moderate Breeze'
    elif 10.8 <= wind_speed_m_per_s <= 13.8:
        return 'Fresh Breeze'
    elif 13.9 <= wind_speed_m_per_s <= 17.1:
        return 'Strong Breeze'
    elif 17.2 <= wind_speed_m_per_s <= 20.7:
        return 'Near Gale'
    elif 20.8 <= wind_speed_m_per_s <= 24.4:
        return 'Gale'
    elif 24.5 <= wind_speed_m_per_s <= 28.4:
        return 'Strong Gale'
    elif 28.5 <= wind_speed_m_per_s <= 32.6:
        return 'Storm'
    elif wind_speed_m_per_s >= 32.7:
        return 'Violent Storm'


def transform_data(**kwargs):
    csv = kwargs['ti'].xcom_pull(key = 'INSERT ACTUAL KEY HERE')
    df = pd.read_csv(csv)

    # Convert formatted date to datetime object, converting to utc and remove timezone info
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True)
    df['Formatted Date'] = df['Formatted Date'].dt.tz_localize(None)

    # Drop duplicate dates
    df.drop_duplicates(subset='Formatted Date', inplace=True)

    # Precip type has two different values, rain and snow
    # Fill nan values with snow if temperature is 0 or below, rain otherwise
    fill_values = np.where(df['Temperature (C)'] <= 0, 'snow', 'rain')
    df['Precip Type'] = df['Precip Type'].fillna(pd.Series(fill_values, index=df.index))

    # Column 'Loud Cover' probably means cloud cover, and only contains the value 0.0, let's remove it
    df = df.drop(columns=['Loud Cover'])


    """ Replacing possible negative values with '?' in columns where value can't be negative,
     then replacing all '?' values with NaN, then replacing NaN with mode in columns with numerical data """

    # Columns to check for negative values
    columns_to_check = ['Humidity', 'Wind Speed (km/h)', 'Wind Bearing (degrees)', 'Visibility (km)', 'Pressure (millibars)']

    # Replace negative values with '?'
    df[columns_to_check] = df[columns_to_check].where(df[columns_to_check] >= 0, '?')

    # Replace '?' with NaN in the whole dataframe
    df.replace('?', np.nan, inplace=True)

    # Columns where NaN should be replaced with mode
    columns_for_mode = columns_to_check + ['Temperature (C)', 'Apparent Temperature (C)']

    # Replace NaN with mode
    for column in columns_for_mode:
        mode_value = df[column].mode()[0]
        df[column] = df[column].fillna(mode_value)
