# Imports
import requests
import pandas as pd
import os 
import sqlalchemy
import psycopg2


# Variables and constants
current_path = os.path.dirname(os.path.realpath(__file__))
data_path = current_path + "//data" 
url = None
raw_file = "//raw.json"
harmonized_file = "//harmonized.json"
cleansed_file = "//cleansed.json"

# Functions

def request_weather_data(url):
    r = requests.get(url)
    

def raw_json(request_object, save_path):
    pass

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
