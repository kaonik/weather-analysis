import psycopg2
import os

def create_location_points(lat_start=24,lat_end=50,long_start=-125,long_end=-67, step=1.0):
    '''
    Generate a grid of latitude and longitude points. Default is contiguous US with a 1 degree step.
    '''
    locations = []
    for lat in range(int(lat_start), int(lat_end)+1, int(step)):
        for long in range(int(long_start), int(long_end)+1, int(step)):
            locations.append((lat,long))
    return locations

def bulk_insert_locations(db_conn_params,locations):
    '''
    Bulk insert locations into database.
    '''

db_conn_params = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}