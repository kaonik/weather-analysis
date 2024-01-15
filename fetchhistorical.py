import requests
import json
import os
import psycopg2
import psycopg2.extras
import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import asyncio
from tqdm.asyncio import tqdm as async_tqdm

from fetchforecast import bulk_upsert_forecasts, get_forecast_ids, bulk_upsert_rain_snow

# OpenWeatherMap API key
api_key = os.environ.get('OPENWEATHER_API_KEY')

db_conn_params = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}

#Get oldest forecast date per location from database
def get_locations_time(db_conn_params):
    try:
        with psycopg2.connect(**db_conn_params) as conn:
            with conn.cursor() as cur:
                # Get location_id, latitude, longitude, and oldest forecast date
                cur.execute("""
                    SELECT l.locationid, l.latitude, l.longitude, MIN(f.timestampiso)
                    FROM location l
                    JOIN forecast f ON l.locationid = f.locationid
                    WHERE l.data_available = TRUE
                    GROUP BY l.locationid, l.latitude, l.longitude
                """)
                results = cur.fetchall()
                return results
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None



#fetch forecast data from OpenWeatherMap API
async def get_historical_data(session, location, api_key=api_key):
    location_id, latitude, longitude, end = location

    #Convert to unix timestamp
    end = int(end.timestamp())

    #Subtract 1 day from end
    end = datetime.datetime.utcfromtimestamp(end) - datetime.timedelta(days=1)
    #Subtract 31 weeks from end - 50k limit on API calls, 1593 locations * 31 weeks = 49383
    start = end - datetime.timedelta(weeks=1)
    
    #Convert to unix timestamp
    start = int(start.timestamp())
    end = int(end.timestamp())
    
    url = f"https://history.openweathermap.org/data/2.5/history/city?lat={latitude}&lon={longitude}&type=hour&start={start}&end={end}&appid={api_key}&units=imperial"

    async with session.get(url) as response:
        if response.status == 200:
            return await response.json()
        elif response.status == 404:
            print(f"No data for location {location_id}. Marking as unavailable.")
            mark_data_available_false(db_conn_params, location_id)
            return None
        else:
            print(f"Error fetching forecast data for {location_id}: {response.status}")
            return None
        
#Mark data_available as FALSE for location_id with no historical data
def mark_data_available_false(db_conn_params, location_id):
    try:
        with psycopg2.connect(**db_conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE location
                    SET data_available = FALSE
                    WHERE locationid = %s
                """, (location_id,))
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error updating location availability: {error}")
        

async def main(api_key, locations, batch_size=100):
    async with aiohttp.ClientSession() as session:
        # Split locations into batches
        batches = [locations[i:i + batch_size] for i in range(0, len(locations), batch_size)]
        
        for batch in batches:
            tasks = []
            for location in batch:
                task = asyncio.ensure_future(get_historical_data(session, location, api_key))
                tasks.append(task)
        
            forecast_data_list = []
            for task in async_tqdm(asyncio.as_completed(tasks), total=len(batch), desc='Fetching historical data', unit='location'):
                data = await task
                if data is not None:
                    forecast_data_list.append(data)

            all_forecasts = []
            # Process and upsert the data for each batch
            for location, data in zip(batch, forecast_data_list):
                if data is not None:
                    location_id, latitude, longitude = location[:3]

                    # Process forecast data
                    processed_data = extract_forecast_data(data['list'])
                    upsert_data = [(location_id, *record) for record in processed_data]
                    all_forecasts.extend(upsert_data)
            
            # Bulk upsert forecast data
            if all_forecasts:
                bulk_upsert_forecasts(db_conn_params, all_forecasts)

                # Get forecast ids
                forecast_ids = get_forecast_ids(db_conn_params, all_forecasts)
                # Upsert rain and snow data
                bulk_upsert_rain_snow(db_conn_params, forecast_ids, all_forecasts)

    return None

    
#Extract forecast data for bulk upsert
def extract_forecast_data(forecast_list):
    extracted_data = []

    for entry in forecast_list:

        #Convert dt to datetime oblect object in utc
        dt_utc = datetime.datetime.utcfromtimestamp(entry['dt'])

        #Check 3 hour interval
        if dt_utc.hour % 3 == 0:

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

            dt_txt = dt_utc.isoformat()

            extracted_data.append((temp, pressure, sea_level, grnd_level, humidity,
                               weather_condition_id,
                               cloudiness, wind_speed, wind_direction, 
                               visibility, pop, dt_txt, rain, snow))
    return extracted_data


locations = get_locations_time(db_conn_params)


loop = asyncio.get_event_loop()
loop.run_until_complete(main(api_key, locations))



