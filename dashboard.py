import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.express as px
import psycopg2
import os
import pandas as pd

db_conn_params = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}

# Fetch data from database
def fetch_data(query,db_conn_params=db_conn_params):
    with psycopg2.connect(**db_conn_params) as conn:
        return pd.read_sql(query,conn)

# Get latitude, longitude, and forecast data with locationid
query ="""
    SELECT DATE(f.timestampiso) AS day,
           AVG(f.temperature) AS avg_temperature,
           AVG(f.humidity) AS avg_humidity,
           AVG(f.windspeed) AS avg_wind_speed,
           AVG(f.pressure) AS avg_pressure,
           l.latitude, l.longitude
    FROM location l
    JOIN forecast f ON l.locationid = f.locationid
    GROUP BY day, l.latitude, l.longitude
    LIMIT 1000;
"""

df = fetch_data(query)

# Create plotly scatter mapbox figure
fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", color="avg_temperature", color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=3, hover_data=["day","avg_temperature","avg_humidity","avg_wind_speed","avg_pressure"])
fig.update_layout(mapbox_style="open-street-map")

# Create dash app
app = dash.Dash(__name__)

# Create app layout
app.layout = html.Div([
    html.H1("Weather Dashboard"),
    dcc.Graph(
        id="scatter-map", 
        figure=fig,
        # Autosize height
        style={'height': '90vh'}
        )
])

# Run app
if __name__ == "__main__":
    app.run_server(debug=True)