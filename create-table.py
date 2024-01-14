import psycopg2
import os

db_conn_params = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}

# Connect to the database
conn = psycopg2.connect(**db_conn_params)

# Create a cursor object to execute SQL statements
cursor = conn.cursor()

# SQL statements
sql_statements = '''
DROP TABLE IF EXISTS Rain CASCADE;
DROP TABLE IF EXISTS Snow CASCADE;
DROP TABLE IF EXISTS Forecast CASCADE;
DROP TABLE IF EXISTS Location CASCADE;
DROP TABLE IF EXISTS WeatherConditionTypes CASCADE;

create table WeatherConditionTypes (
    WeatherConditionID int primary key,
    Main varchar(50),
    Description varchar(50)
);

create table Location (
    LocationID serial primary key,
    Latitude float,
    Longitude float
);

create table Forecast (
    ForecastID serial primary key,
    LocationID int references Location(LocationID),
    Temperature float,
    Pressure int,
    SeaLevelPressure int,
    GroundLevelPressure int,
    Humidity int,
    WeatherConditionID int references WeatherConditionTypes(WeatherConditionID),
    Cloudiness int,
    WindSpeed float,
    WindDirection int,
    WindGust float,
    Visibility int,
    PrecipitationChance float,
    TimestampISO timestamp,
    constraint unique_location_timestamp unique (LocationID, TimestampISO)
);

create table Rain (
    RainID serial primary key,
    ForecastID int references Forecast(ForecastID),
    Volume3h float
);

create table Snow (
    SnowID serial primary key,
    ForecastID int references Forecast(ForecastID),
    Volume3h float
);
'''

# Execute the SQL statements
cursor.execute(sql_statements)

# Commit the changes and close the connection
conn.commit()
print('Tables successfully created.')
conn.close()
