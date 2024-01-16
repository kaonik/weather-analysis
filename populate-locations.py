import psycopg2
import psycopg2.extras
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
    conn = psycopg2.connect(**db_conn_params)
    cursor = conn.cursor()

    try:
        #Use execute_values for bulk insert
        psycopg2.extras.execute_values(
            cursor,
            "INSERT INTO Location (Latitude, Longitude) VALUES %s ON CONFLICT (Latitude, Longitude) DO NOTHING;",
            locations,
            template=None,
            page_size=100
        )
        conn.commit()
        print('Locations successfully inserted.')
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error: {error}')
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


db_conn_params = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}

if __name__ == "__main__":
    #Grid for world
    locations = create_location_points(-90,90,-180,180)

    #Grid for contiguous US
    #locations = create_location_points() 
    bulk_insert_locations(db_conn_params,locations)