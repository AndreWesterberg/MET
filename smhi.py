# Imports
import requests
import pandas as pd
import os 
import sqlalchemy
import psycopg2
import json


# Variables and constants
current_path = os.path.dirname(os.path.realpath(__file__))
data_path = current_path + "//data" 
url = "https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=60.10&lon=9.58"
raw_file = "//raw.json"
harmonized_file = "//harmonized.json"
cleansed_file = "//cleansed.json"
column_names = ['time', 'air_pressure_at_sea_level', 'air_temperature', 'cloud_area_fraction', 'relative_humidity', 'wind_from_direction', 'wind_speed', '12h_symbol_code', '1h_symbol_code', '1h_precipitation_amount', '6h_symbol_code', '6h_precipitation_amount']


# Functions

def request_weather_data(url):
    r = requests.get(url, headers={'User-Agent': 'Andrewest'})
    return r.json()

def raw_json(response_object, save_path):
    f = open(save_path, "w")
    json.dump(response_object, f)
    f.close()

def raw_to_harmonized(load_path, save_path):
    f = open(load_path)
    raw = f.readline()
    f.close()
    raw_json = json.loads(raw)
    properties = raw_json['properties']
    timeseries = properties['timeseries']
    df = pd.json_normalize(timeseries)
    #keys = df.keys()
    df.to_json(save_path)

# To do remove NaN values
def harmonized_to_cleansed(load_path, save_path, column_names):
    df = pd.read_json(load_path)
    #df2 = pd.DataFrame(df, columns=column_names)
    print(df.tail())

def connect_db():
    # db address + port + usr + pswd + schema
    pass

def clensed_to_db(load_path):
    pass


# Main program
response = request_weather_data(url)
raw_json(response, data_path + raw_file)
raw_to_harmonized(data_path + raw_file, data_path + harmonized_file)
harmonized_to_cleansed(data_path + harmonized_file, data_path + cleansed_file, column_names)