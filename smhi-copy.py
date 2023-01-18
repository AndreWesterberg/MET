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

url = 'https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=60.10&lon=9.58'

raw_file = "//raw.json"
harmonized_file = "//harmonized.json"
cleansed_file = "//cleansed.json"


# Functions

def request_weather_data(url):
    r = requests.get(url)
    return r.json()

def raw_json(response_object, save_path):
    # df = pd.json_normalize(response_object.json(), sep=',')
    # df.to_json(save_path)
    f = open(save_path, 'w')
    json.dump(response_object, f)
    f.close()


# deprecated
def raw_to_harmonized(load_path, save_path):
    df = pd.read_json(load_path, orient='records', typ='series')
    pd.set_option("expand_frame_repr", True)
    df_keys = df.keys()
    df_harmonize = pd.DataFrame.from_dict(df['properties,timeseries'])
    print(df_harmonize)
    

def raw_to_harmonized_2(load_path, save_path):
    f = open(load_path)
    b = f.readline()
    f.close()
    c = json.loads(b)
    # c = pd.json_normalize(b, sep='_')

    print(type(c))

def harmonized_to_cleansed(load_path, save_path):
    pass

def connect_db():
    # db address + port + usr + pswd + schema
    pass

def clensed_to_db(load_path):
    pass


# Main program
response = request_weather_data(url)
raw_json(response, data_path + raw_file)
#raw_to_harmonized(data_path + raw_file, data_path + harmonized_file)
raw_to_harmonized_2(data_path + raw_file, data_path + harmonized_file)