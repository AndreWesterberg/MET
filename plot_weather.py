# Imoprts
import pandas as pd
import psycopg2
from plotly import express as px
from plotly.subplots import make_subplots
from plotly import graph_objects as go

column_names = [
    'time', 
    'pressure', 
    'temperature', 
    'cloud %', 
    'humidity', 
    'wind direction', 
    'wind speed', 
    '12h symbol code', 
    '1h symbol code', 
    '1h precipitation amount', 
    '6h symbol code', 
    '6h precipitation amount'
]

# Creates a connection to the database
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="weather_db",
        user="postgres",
        password="Andre9119")    # Change to your own pgAdmin postgres user password
    return conn

# Select weatherdata from the database
def add_weather_data():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM cleansed.weather;')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def create_df(rows, column_names):
    df = pd.DataFrame(rows, columns=column_names)
    return df

def temperature_plot(df):
    figure = px.line(df, x="time", y="temperature")
    figure.show()


# Main program
rows = add_weather_data()
df = create_df(rows, column_names)
temperature_plot(df)






