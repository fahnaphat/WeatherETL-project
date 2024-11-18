from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import requests
import json
import os

POSTGRES_CONN_ID='postgres_db'
API_CONN_ID='open_weather_api'
CITY_NAME = 'Bangkok'
API_KEY = os.getenv('OPENWEATHER_API_KEY')

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


default_args = {
    'owner': 'naphat',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 17),
    catchup=False
) as dag:
    
    @task()
    def extract_weather_data():
        """Extract weather data from OpenWeather API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection
        http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')

        ## Build the API endpoint
        endpoint = f'/data/2.5/weather?q={CITY_NAME}&appid={API_KEY}'

        ## Make the request via the HTTP Hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
    
    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""

        time_of_record = datetime.utcfromtimestamp(weather_data['dt']).strftime('%Y-%m-%d %H:%M:%S')
        country = weather_data['sys']['country']
        city = weather_data['name'] # Bangkok
        latitude = weather_data['coord']['lat']
        longitude = weather_data['coord']['lon']
        weather = weather_data['weather'][0]['main'] # Cloud
        temp_farenheit = kelvin_to_fahrenheit(weather_data['main']['temp'])
        feels_like_farenheit= kelvin_to_fahrenheit(weather_data['main']['feels_like'])
        min_temp_farenheit = kelvin_to_fahrenheit(weather_data['main']['temp_min'])
        max_temp_farenheit = kelvin_to_fahrenheit(weather_data['main']['temp_max'])
        pressure = weather_data['main']['pressure']
        humidity = weather_data['main']['humidity']
        wind_speed = weather_data['wind']['speed']
        sunrise_time = datetime.utcfromtimestamp(weather_data['sys']['sunrise']).strftime('%Y-%m-%d %H:%M:%S')
        sunset_time = datetime.utcfromtimestamp(weather_data['sys']['sunset']).strftime('%Y-%m-%d %H:%M:%S')


        transformed_data = {
            "Time of Record": time_of_record,
            "Country": country,
            "City": city,
            "Latitude": latitude,
            "Longitude": longitude,
            "Current Weather": weather,
            "Temperature (F)": temp_farenheit,
            "Feels Like (F)": feels_like_farenheit,
            "Minimun Temp (F)":min_temp_farenheit,
            "Maximum Temp (F)": max_temp_farenheit,
            "Pressure": pressure,
            "Humidty": humidity,
            "Wind Speed": wind_speed,
            "Sunrise (Local Time)":sunrise_time,
            "Sunset (Local Time)": sunset_time
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into PostgreSQL"""

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            time_of_record TIMESTAMP,
            country VARCHAR(3),
            city VARCHAR(50),
            latitude FLOAT,
            longitude FLOAT,
            weather VARCHAR(50),
            temperature FLOAT,
            feels_like FLOAT,
            min_temp FLOAT,
            max_temp FLOAT,
            pressure INT,
            humidity INT,
            wind_speed FLOAT,
            sunrise_time TIMESTAMP,
            sunset_time TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
            INSERT INTO weather_data (
                time_of_record, country, city, latitude, longitude,
                weather, temperature, feels_like, min_temp,
                max_temp, pressure, humidity, wind_speed, 
                sunrise_time, sunset_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['Time of Record'],
            transformed_data['Country'],
            transformed_data['City'],
            transformed_data['Latitude'],
            transformed_data['Longitude'],
            transformed_data['Current Weather'],
            transformed_data['Temperature (F)'],
            transformed_data['Feels Like (F)'],
            transformed_data['Minimun Temp (F)'],
            transformed_data['Maximum Temp (F)'],
            transformed_data['Pressure'],
            transformed_data['Humidty'],
            transformed_data['Wind Speed'],
            transformed_data['Sunrise (Local Time)'],
            transformed_data['Sunset (Local Time)']
        ))

        conn.commit()
        cursor.close()

    ## DAG Workflow - ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)