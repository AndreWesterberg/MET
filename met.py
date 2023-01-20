# Imports
import requests
import pandas as pd
import os 
import psycopg2
import json
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
"""

# Functions
# Takes the url for the API and returns the raw weather data
def request_weather_data(url):
    r = requests.get(url, headers={'User-Agent': 'Andrewest'})
    return r.json()

# Saves the raw data to a json file
def raw_json(raw_data, save_path):
    f = open(save_path, "w")
    json.dump(raw_data, f)
    f.close()

# Load the raw data and start excluding unnessesary data, and transforming the usfull data to a nicer format (harmonized data)
def raw_to_harmonized(load_path, save_path):
    f = open(load_path)
    raw = f.readline()
    f.close()
    raw_json = json.loads(raw)
    properties = raw_json['properties']
    timeseries = properties['timeseries']
    df = pd.json_normalize(timeseries)
    df.to_json(save_path)

# Take the harmonized data, removing the NaN values and saving the cleansed data to a json and returning the cleansed dataframe
def harmonized_to_cleansed(load_path, save_path):
    df = pd.read_json(load_path)
    df.dropna(inplace=True)
    df = df[:-1]
    df.to_json(save_path)
    return df

# Creates a connection to the database
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="weather_db",
        user="postgres",
        password="Andre9119")    # Change to your own pgAdmin postgres user password
    return conn

# Adds the weatherdata from the dataframe to the database
def add_weather_data(df, city):
    conn = get_db_connection()
    cur = conn.cursor()
    for i in range(len(df)):
        row = df.iloc[i]
        cur.execute(
            f"INSERT INTO cleansed.weather_{city} VALUES ( \
                '{row['time']}', \
                '{row['data.instant.details.air_pressure_at_sea_level']}', \
                '{row['data.instant.details.air_temperature']}', \
                '{row['data.instant.details.cloud_area_fraction']}', \
                '{row['data.instant.details.relative_humidity']}', \
                '{row['data.instant.details.wind_from_direction']}',\
                '{row['data.instant.details.wind_speed']}', \
                '{row['data.next_12_hours.summary.symbol_code']}', \
                '{row['data.next_1_hours.summary.symbol_code']}', \
                '{row['data.next_1_hours.details.precipitation_amount']}', \
                '{row['data.next_6_hours.summary.symbol_code']}', \
                '{row['data.next_6_hours.details.precipitation_amount']}');"
        )
    cur.execute("COMMIT;")
    cur.close()
    conn.close()

def request_w_d():
    city = ['Stockholm', 'Goteborg', 'Malmo', 'Bergen', 'Reykjavik']
    latitude = [59.34, 57.72, 55.61, 60.39, 64.13]
    longitude = [18.07, 11.99, 12.99, 5.32, 21.82]
    #raw_list = []
    for i in range(len(city)):
        url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={latitude[i]}&lon={longitude[i]}"
        raw_data = request_weather_data(url)
        current_path = os.path.dirname(os.path.realpath(__file__))
        data_path = current_path + "//data" 
        raw_file = f"//raw_{city[i]}.json"
        raw_json(raw_data, data_path + raw_file)
        #raw_list.append(raw_data)
#    return raw_list

def r_t_h():
    city = ['Stockholm', 'Goteborg', 'Malmo', 'Bergen', 'Reykjavik']
    for i in range(len(city)):
        current_path = os.path.dirname(os.path.realpath(__file__))
        data_path = current_path + "//data" 
        raw_file = f"//raw_{city[i]}.json"
        harmonized_file = f"//harmonized_{city[i]}.json"
        raw_to_harmonized(data_path + raw_file, data_path + harmonized_file)

def h_t_c():
    city = ['Stockholm', 'Goteborg', 'Malmo', 'Bergen', 'Reykjavik']
    for i in range(len(city)):
        current_path = os.path.dirname(os.path.realpath(__file__))
        data_path = current_path + "//data" 
        harmonized_file = f"//harmonized_{city[i]}.json"
        cleansed_file = f"//cleansed_{city[i]}.json"
        weather_df = harmonized_to_cleansed(data_path + harmonized_file, data_path + cleansed_file)
        add_weather_data(weather_df, city[i])



# Main program
request_w_d()
r_t_h()
h_t_c()


"""
# DAG
with DAG("met", start_date=datetime(2022, 2, 2),
    schedule_interval=None, catchup=False) as dag:

        task_1 = PythonOperator(
            task_id="request_weather_data",
            python_callable=request_w_d
        )
        task_2 = PythonOperator(
            task_id="raw_to_harmonized",
            python_callable=r_t_h
        )
        task_3 = PythonOperator(
            task_id="harmonized_to_cleansed",
            python_callable=h_t_c
        )
        read_OK = BashOperator(
            task_id="read_OK",
            bash_command="echo 'read_OK'"
        )
        read_failed = BashOperator(
            task_id="read_failed",
            bash_command="echo 'read_failed'"
        )
        task_1  >> task_2 >> task_3 """