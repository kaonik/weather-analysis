import requests
import json
import os
import psycopg2
import psycopg2.extras
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

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
def get_forecast_data(location, api_key=api_key):
    location_id, latitude, longitude = location
    url = f'https://api.openweathermap.org/data/2.5/forecast?lat={latitude}&lon={longitude}&appid={api_key}&units=imperial'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error fetching forecast data for {location_id}: {response.status_code}')
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
                               visibility, pop, dt_txt, rain, snow))
    return extracted_data

#bulk upsert forecast data into database
def bulk_upsert_forecasts(db_conn_params, forecast_records):
    conn = psycopg2.connect(**db_conn_params)
    cursor = conn.cursor()

    #Remove rain and snow from forecast_records
    forecast_records = [record[:-2] for record in forecast_records]

    upsert_query = """
    INSERT INTO Forecast (LocationID, Temperature, Pressure, SeaLevelPressure, GroundLevelPressure, Humidity,
                          WeatherConditionID, Cloudiness, WindSpeed, WindDirection,
                          WindGust, Visibility, PrecipitationChance, TimestampISO)
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
        PrecipitationChance = EXCLUDED.PrecipitationChance;
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

#fetch forecastid from database based on locationid and timestampiso
def get_forecast_ids(db_conn_params, forecast_records):
    conn = psycopg2.connect(**db_conn_params)
    cursor = conn.cursor()
    forecast_ids = []

    try:
        # Create a list of tuples containing the location ID and timestamp ISO for each forecast record
        forecast_params = [(record[0], record[-3]) for record in forecast_records]

        # Use executemany to execute the SELECT query for all forecast records at once
        cursor.executemany('SELECT ForecastID FROM Forecast WHERE LocationID = %s AND TimestampISO = %s;', forecast_params)
        results = cursor.fetchall()

        # Append the forecast IDs to the forecast_ids list
        forecast_ids.extend([result[0] for result in results])

        return forecast_ids
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error: {error}')
    finally:
        cursor.close()
        conn.close()

#insert rain and snow data into database
def bulk_upsert_rain_snow(db_conn_params, forecast_ids, forecast_data):
    try:
        conn = psycopg2.connect(**db_conn_params)
        cursor = conn.cursor()

        # Create a list of tuples containing the forecast ID, rain volume, and snow volume for each forecast record
        bulk_data = [(forecast_id, record[-2], record[-1]) for forecast_id, record in zip(forecast_ids, forecast_data)]

        # Use executemany to execute the INSERT query for all forecast records at once
        cursor.executemany('INSERT INTO Rain (ForecastID, Volume3h) VALUES (%s, %s);', [(forecast_id, rain) for forecast_id, rain, snow in bulk_data if rain != 0])
        cursor.executemany('INSERT INTO Snow (ForecastID, Volume3h) VALUES (%s, %s);', [(forecast_id, snow) for forecast_id, rain, snow in bulk_data if snow != 0])
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error: {error}')
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

locations = get_locations(db_conn_params)
all_forecasts = []

# Fetch forecast data for all locations in parallel
with ThreadPoolExecutor() as executor:
    forecast_data = list(tqdm(executor.map(get_forecast_data, locations[:1]), 
                              total=len(locations[:1]), desc='Fetching forecast data', unit='location'))



# Iterate over locations and forecast data
for location, forecast_data in zip(locations, forecast_data):
    location_id, latitude, longitude = location
    
    # Check if forecast data exists for location before processing
    if forecast_data and 'list' in forecast_data:
        # Process forecast data
        processed_data = extract_forecast_data(forecast_data['list'])
        upsert_data = [(location_id, *entry) for entry in processed_data]
        all_forecasts.extend(upsert_data)
    else:
        print(f'Process forecast data failed at {location}.')



if all_forecasts:
    bulk_upsert_forecasts(db_conn_params, all_forecasts)
    print(f'Forecast data successfully upserted.')
else:
    print(f'Forecast data not upserted.')

#Retrieve ForecastID based on LocationID and TimestampISO and use to insert into Rain and Snow tables.
forecast_ids = get_forecast_ids(db_conn_params, all_forecasts)
print(forecast_ids)
bulk_upsert_rain_snow(db_conn_params, forecast_ids, all_forecasts)