import requests
import json
import os
import psycopg2
import psycopg2.extras
import time
from tqdm import tqdm

# OpenWeatherMap API key
api_key = os.environ.get('OPENWEATHER_API_KEY')

db_conn_params = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}

#fetch LocationID, Latitude, and Longitude from database and return as list of tuples
def get_locations(db_conn_params):
    conn = psycopg2.connect(**db_conn_params)
    cursor = conn.cursor()

    try:
        cursor.execute('SELECT LocationID, Latitude, Longitude FROM Location;')
        locations = cursor.fetchall()
        return locations
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error: {error}')
    finally:
        cursor.close()
        conn.close()


#fetch forecast data from OpenWeatherMap API
def get_forecast_data(lat,lon,api_key=api_key):
    url = f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=imperial'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')
        return None


#process forecast data from json response into list of tuples for bulk upsert
def extract_forecast_data(forecast_list):
    extracted_data = []
    for entry in forecast_list:
        temp = entry['main']['temp']
        pressure = entry['main']['pressure']
        sea_level = entry['main'].get('sea_level')
        grnd_level = entry['main'].get('grnd_level')
        humidity = entry['main']['humidity']
        
        weather_condition_id = entry['weather'][0]['id']
        
        cloudiness = entry['clouds']['all']
        wind_speed = entry['wind']['speed']
        wind_direction = entry['wind'].get('deg')
        wind_gust = entry['wind'].get('gust')
        visibility = entry.get('visibility')
        pop = entry.get('pop')
        
        rain = entry['rain']['3h'] if 'rain' in entry and '3h' in entry['rain'] else 0
        snow = entry['snow']['3h'] if 'snow' in entry and '3h' in entry['snow'] else 0

        dt_txt = entry['dt_txt']

        extracted_data.append((temp, pressure, sea_level, grnd_level, humidity,
                               weather_condition_id,
                               cloudiness, wind_speed, wind_direction, wind_gust,
                               visibility, pop, rain, snow, dt_txt))
    return extracted_data

#bulk upsert forecast data into database
def bulk_upsert_forecasts(db_conn_params, forecast_records):
    conn = psycopg2.connect(**db_conn_params)
    cursor = conn.cursor()

    upsert_query = """
    INSERT INTO Forecast (LocationID, Temperature, Pressure, SeaLevelPressure, GroundLevelPressure, Humidity,
                          WeatherConditionID, Cloudiness, WindSpeed, WindDirection,
                          WindGust, Visibility, PrecipitationChance, RainVolume, SnowVolume, TimestampISO)
    VALUES %s
    ON CONFLICT (LocationID, TimestampISO)
    DO UPDATE SET
        Temperature = EXCLUDED.Temperature,
        Pressure = EXCLUDED.Pressure,
        SeaLevelPressure = EXCLUDED.SeaLevelPressure,
        GroundLevelPressure = EXCLUDED.GroundLevelPressure,
        Humidity = EXCLUDED.Humidity,
        WeatherConditionID = EXCLUDED.WeatherConditionID,
        Cloudiness = EXCLUDED.Cloudiness,
        WindSpeed = EXCLUDED.WindSpeed,
        WindDirection = EXCLUDED.WindDirection,
        WindGust = EXCLUDED.WindGust,
        Visibility = EXCLUDED.Visibility,
        PrecipitationChance = EXCLUDED.PrecipitationChance,
        RainVolume = EXCLUDED.RainVolume,
        SnowVolume = EXCLUDED.SnowVolume;
    """

    try:
        psycopg2.extras.execute_values(cursor, upsert_query, forecast_records, template=None, page_size=100)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error in bulk_upsert_forecasts: {error}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

locations = get_locations(db_conn_params)
all_forecasts = []

for location in tqdm(locations, desc='Fetching forecasts', unit='location'):
    location_id, latitude, longitude = location
    forecast_data = get_forecast_data(latitude, longitude, api_key)

    #Sleep for 1 second to avoid exceeding API rate limit
    #time.sleep(1)
    
    #Check if forecast data exists for location before processing
    if forecast_data and 'list' in forecast_data:
        #Process forecast data
        processed_data = extract_forecast_data(forecast_data['list'])
        #Add LocationID to processed data
        upsert_data = [(location_id, *entry) for entry in processed_data]
        #Add processed data to list of all forecasts
        all_forecasts.extend(upsert_data)
    else:
        print(f'Process forecast data failed at {location}.')

if all_forecasts:
    bulk_upsert_forecasts(db_conn_params, upsert_data)
    print(f'Forecast data successfully upserted.')
else:
    print(f'Forecast data not upserted.')


'''
TODO: Remove rain and snow from main bulk upsert and create separate functions for bulk upsert of rain and snow.
Retrieve ForecastID based on LocationID and TimestampISO and use to insert into Rain and Snow tables.
Insert into Rain and Snow tables should be separate from main bulk upsert to avoid duplicate entries in Forecast table.
'''