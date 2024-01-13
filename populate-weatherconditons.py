from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
import os

# Read the HTML file
with open("projects/weather/weather-conditions.html", "r") as file:
    html_content = file.read()

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, "html.parser")

# Find the starting point for search
start_point = soup.find("a", id="Weather-Condition-Codes-2").find_next("h2")

# Find all the tables after the starting point
tables = start_point.find_all_next("table")

weather_conditions = []

# Iterate over each table
for table in tables:
    # Find all the rows in the table
    rows = table.find_all("tr")

    # Skip the header row
    for row in rows[1:]:
        # Extract the ID, Main, and Description from each row
        cells = row.find_all("td")
        condition_id = cells[0].text.strip()
        condition_main = cells[1].text.strip()
        condition_description = cells[2].text.strip()

        # Store in list of tuples
        weather_conditions.append((condition_id, condition_main, condition_description))

db_conn_params = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}

# Connect to database
conn = psycopg2.connect(**db_conn_params)
cursor = conn.cursor()
#Insert into database
try:
    cursor.executemany(
        "INSERT INTO WeatherConditionTypes (WeatherConditionID, Main, Description) VALUES (%s, %s, %s);",
        weather_conditions
    )
    conn.commit()
    print('Weather conditions successfully inserted.')
except (Exception, psycopg2.DatabaseError) as error:
    print(f'Error: {error}')
    conn.rollback()
finally:
    cursor.close()
    conn.close()

