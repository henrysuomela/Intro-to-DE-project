import pandas as pd
import numpy as np
import sqlite3
import os
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi

# Path of kaggle.json:  /home/intro/.config/kaggle/kaggle.json

# Default arguments for the DAG
default_args = {
    'owner': 'Team_6',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'WeatherHistory_Project_incomplete',
    default_args=default_args,
    description='ETL pipeline for the weather history',
    schedule_interval='@daily',
)

# File paths
csv_file_path = '/home/intro/airflow/datasets/weatherHistory.csv'
db_path = '/home/intro/airflow/databases/weatherHistory_database.db'

# Task 1: Extract data
def extract_data(**kwargs):
    # Set up Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Download the dataset file
    api.dataset_download_file('muthuj7/weather-dataset', file_name='weatherHistory.csv', path='/home/intro/airflow/datasets')

    # Define file paths
    downloaded_file_path = '/home/intro/airflow/datasets/weatherHistory.csv'
    zip_file_path = downloaded_file_path + '.zip'

    # Check if the downloaded file is a ZIP file
    if zipfile.is_zipfile(zip_file_path):
        # If it's a ZIP file, unzip it
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall('/home/intro/airflow/datasets')
        # Update file path to extracted CSV
        os.remove(zip_file_path)  # Optionally delete the ZIP file after extraction
    else:
        print("Downloaded file is not a ZIP archive, skipping extraction.")

    # Push the CSV file path to XCom for use in the next steps
    kwargs['ti'].xcom_push(key='csv_file_path', value=downloaded_file_path)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Transform data
def transform_data(**kwargs):
    # Retrieve file path from XCom
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    df = pd.read_csv(file_path)

    # Convert formatted date to datetime object, converting to utc and remove timezone info
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True)
    df['Formatted Date'] = df['Formatted Date'].dt.tz_localize(None)

    # Drop duplicate dates (if date and time of day are same)
    df.drop_duplicates(subset='Formatted Date', inplace=True)

    # Precip type has two different values, rain and snow
    # Fill NaN values with snow if temperature is 0 or below, rain otherwise
    fill_values = np.where(df['Temperature (C)'] <= 0, 'snow', 'rain')
    df['Precip Type'] = df['Precip Type'].fillna(pd.Series(fill_values, index=df.index))



    """ Replacing possible negative values with '?' in columns where value can't be negative,
     then replacing all '?' values with NaN, then replacing NaN with mode in columns with numerical data """

    # Columns to check for negative values
    columns_to_check = ['Humidity', 'Wind Speed (km/h)', 'Wind Bearing (degrees)', 'Visibility (km)', 'Pressure (millibars)']

    # Replace negative values with '?'
    df[columns_to_check] = df[columns_to_check].where(df[columns_to_check] >= 0, '?')

    # Replace '?' with NaN in the whole dataframe
    df.replace('?', pd.NA, inplace=True)

    # Columns where NaN should be replaced with mode
    columns_for_mode = columns_to_check + ['Temperature (C)', 'Apparent Temperature (C)']

    # Replace NaN with mode
    for column in columns_for_mode:
        mode_value = df[column].mode()[0]
        df[column] = df[column].fillna(mode_value)


    """ Daily averages """

    # Create new column for date without time of day
    df['Date'] = df['Formatted Date'].dt.date

    # Create new dataframe for daily averages grouping by date
    daily_averages_df = df.groupby('Date')[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']].agg('mean').reset_index()


    """ Adding new columns for current month and monthly mode """

    # Create new column for month
    df['Month'] = df['Formatted Date'].dt.month

    # Return mode if there's only one mode, NaN if there are multiple
    def calculate_mode(precip):
        modes = precip.mode()
        if len(modes) == 1:
            return modes.iloc[0]
        else:
            return pd.NA

    # Determine mode for each month
    monthly_mode = df.groupby('Month')['Precip Type'].apply(calculate_mode)

    # Create Mode column to original dataframe
    df['Mode'] = df['Month'].map(monthly_mode)


    """ Adding new column for wind strength """

    def determine_wind_strength(wind_speed):
        # Converts km/h to m/s and categorizes the wind strength 
        wind_speed_m_per_s = round(wind_speed / 3.6, 1)

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

    df['wind_strength'] = df['Wind Speed (km/h)'].apply(determine_wind_strength)

    

    # Save the transformed data to a new CSV file and pass file path to XCom
    transformed_file_path = '/tmp/transformed_weatherHistory.csv'
    df.to_csv(transformed_file_path, index=False)
    kwargs['ti'].xcom_push(key='transformed_file_path', value=transformed_file_path)


transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)


# Task 3: Validate data
def validate_data(**kwargs):
    # Retrieve transformed file path from XCom
    transformed_file_path = kwargs['ti'].xcom_pull(key='transformed_file_path')
    df = pd.read_csv(transformed_file_path)

  

    # Pass the file path to XCom for the next task
    kwargs['ti'].xcom_push(key='validated_file_path', value=transformed_file_path)

validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)



# Task 4: Load data
def load_data(**kwargs):
    # Retrieve validated file path from XCom
    validated_file_path = kwargs['ti'].xcom_pull(key='validated_file_path')
    
    df = pd.read_csv(validated_file_path)

    # Load data into SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor() # A cursor is an object used to interact with a database. It acts as a control structure that enables traversal over the records in a database and execution of SQL commands. The cursor is part of the database connection and is used to execute SQL queries, fetch data, and manage database operations.

 
    # Insert data into the database
    df.to_sql('Weather History', conn, if_exists='append', index=False)
    conn.commit()
    conn.cursor()
    conn.close()

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    trigger_rule='all_success',  # Ensures load_task only runs if validate_task is successful
    dag=dag,
)

# Set task dependencies with trigger rules
extract_task >> transform_task >> validate_task >>  load_task