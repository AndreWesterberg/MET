# Imoprts
import pandas as pd
import psycopg2
from plotly import graph_objects as go

city_list = ['Stockholm', 'Goteborg', 'Malmo', 'Bergen', 'Reykjavik']

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
def add_weather_data(city):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM cleansed.weather_{city};')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def create_df(rows, column_names):
    df = pd.DataFrame(rows, columns=column_names)
    return df

def temperature_plot(df_list, city_list):
    figure = go.Figure()
    for i in range(len(df_list)):
        figure.add_trace(go.Scatter(x=df_list[i]["time"], y=df_list[i]["temperature"], name=city_list[i], mode='lines'))
    figure.update_layout(
        font_family = "Arial",
        font_color = "#000000",
        font_size = 20,
        title = "Temperature",
        title_x = 0.5,
        title_font_family = "Verdana",
        title_font_size = 30,
        title_font_color = "#ACACAC",
        legend_title = "City",
        xaxis_title = "Time",
        yaxis_title = "Temperature (°C)"
    )
    figure.show()

def precipitation_plot(df_list, city_list):
    figure = go.Figure()
    for i in range(len(df_list)):
        figure.add_trace(go.Bar(x=df_list[i]["time"], y=df_list[i]["1h precipitation amount"], name=city_list[i]))
    figure.update_layout(
        font_family = "Arial",
        font_color = "#000000",
        font_size = 20,
        title = "Precipitation",
        title_x = 0.5,
        title_font_family = "Verdana",
        title_font_size = 30,
        title_font_color = "#ACACAC",
        legend_title = "City",
        xaxis_title = "Time",
        yaxis_title = "Precipitation (mm)"
    )
    figure.show()

def wind_speed_plot(df_list, city_list):
    figure = go.Figure()
    for i in range(len(df_list)):
        figure.add_trace(go.Scatter(x=df_list[i]["time"], y=df_list[i]["wind speed"], name=city_list[i], mode='lines'))
    figure.update_layout(
        font_family = "Arial",
        font_color = "#000000",
        font_size = 20,
        title = "Wind speed",
        title_x = 0.5,
        title_font_family = "Verdana",
        title_font_size = 30,
        title_font_color = "#ACACAC",
        legend_title = "City",
        xaxis_title = "Time",
        yaxis_title = "Wind speed (m/s)"
    )
    figure.show()


# Main program
df_list = []
for city in city_list:
    rows = add_weather_data(city)
    df = create_df(rows, column_names)
    df_list.append(df)

temperature_plot(df_list, city_list)
precipitation_plot(df_list, city_list)
wind_speed_plot(df_list, city_list)






