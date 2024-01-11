import requests
import json
import os

# OpenWeatherMap API key
api_key = os.environ.get('OPENWEATHER_API_KEY')
# Example for a specific city
city_name = 'New York'
# Alternatively, use latitude and longitude
# lat, lon = 40.7128, -74.0060

# API endpoint URL
url = f"http://api.openweathermap.org/data/2.5/forecast?q={city_name}&appid={api_key}"

# Make the request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Convert the response to JSON
    data = response.json()

    # Save the data to a JSON file
    with open('weather_data.json', 'w') as file:
        json.dump(data, file)

    print("Data saved to weather_data.json")
else:
    print("Failed to retrieve data: ", response.status_code)
