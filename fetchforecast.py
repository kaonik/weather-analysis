import requests
import json
import os
import psycopg2
import psycopg2.extras
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import asyncio
from tqdm.asyncio import tqdm as async_tqdm

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

async def get_forecast_data(session, location, api_key):
    location_id, latitude, longitude = location
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={latitude}&lon={longitude}&appid={api_key}&units=imperial"
    
    max_retries = 3
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Error fetching forecast data for {location_id}: {response.status}")
                    return None
        except:
            if attempt < max_retries - 1:
                print(f"Server disconnected for Location{location_id}, retrying in {retry_delay} seconds.")
                await asyncio.sleep(retry_delay)
            else:
                print(f"Server disconnected, max retries exceeded.")
                return None

async def main(api_key, locations):
    async with aiohttp.ClientSession() as session:
        forecast_data_list = []
        batch_size = 3000
        batches = [locations[i:i + batch_size] for i in range(0, len(locations), batch_size)]

        # Manual tqdm progress bar for total number of locations
        pbar = async_tqdm(total=len(locations), desc='Fetching forecast data', unit='location')

        for batch in batches:
            tasks = [asyncio.ensure_future(get_forecast_data(session, location, api_key)) for location in batch]
            
            # Wait for all tasks in batch to complete
            for task in asyncio.as_completed(tasks):
                data = await task
                forecast_data_list.append(data)
                pbar.update(1) # Update progress bar

            #Sleep for 60 seconds to avoid rate limit
            await asyncio.sleep(60)
        
        pbar.close()

        return forecast_data_list


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
        visibility = entry.get('visibility')
        pop = entry.get('pop')
        
        rain = entry['rain']['3h'] if 'rain' in entry and '3h' in entry['rain'] else 0
        snow = entry['snow']['3h'] if 'snow' in entry and '3h' in entry['snow'] else 0

        dt_txt = entry['dt_txt']

        extracted_data.append((temp, pressure, sea_level, grnd_level, humidity,
                               weather_condition_id,
                               cloudiness, wind_speed, wind_direction,
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
                            Visibility, PrecipitationChance, TimestampISO)
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
def get_forecast_ids(db_conn_params, forecast_records, batch_size=100):
    conn = psycopg2.connect(**db_conn_params)
    forecast_ids = []

    try:
        # Only include records where rain or snow is not zero
        forecast_params = [
            (record[0], record[-3]) for record in forecast_records 
            if record[-2] != 0 or record[-1] != 0
        ]

        # Function to divide the forecast_params into smaller batches
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        # Process each batch of parameters
        with conn.cursor() as cursor:
            for params_chunk in chunks(forecast_params, batch_size):
                placeholders = ','.join(cursor.mogrify('(%s,%s)', p).decode('utf-8') for p in params_chunk)
                cursor.execute(f"""
                    SELECT ForecastID, LocationID, TimestampISO
                    FROM Forecast
                    WHERE (LocationID, TimestampISO) IN ({placeholders})
                """)
                forecast_ids.extend([row[0] for row in cursor.fetchall()])

        return forecast_ids
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error: {error}')
        conn.rollback()
    finally:
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


if __name__ == '__main__':
    locations = get_locations(db_conn_params)
    all_forecasts = []


    # Run the event loop
    loop = asyncio.get_event_loop()
    forecast_data = loop.run_until_complete(main(api_key, locations))



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
    bulk_upsert_rain_snow(db_conn_params, forecast_ids, all_forecasts)

# # Fetch forecast data for all locations in parallel
# with ThreadPoolExecutor() as executor:
#     forecast_data = list(tqdm(executor.map(get_forecast_data, locations), 
#                               total=len(locations), desc='Fetching forecast data', unit='location'))
