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
url = 'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/18/lat/59/data.json'
raw_file = "//raw.json"
harmonized_file = "//harmonized.json"
cleansed_file = "//cleansed.json"


# Functions

def request_weather_data(url):
    r = requests.get(url)
    return r

def raw_json(response_object, save_path):
    f = open(save_path, "w")
    f.write(json.dumps(response_object.json()))
    f.close()



def raw_to_harmonized(load_path, save_path):
    pass

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