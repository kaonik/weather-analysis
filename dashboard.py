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
    SELECT l.*, f.*
    FROM location l
    JOIN forecast f ON l.locationid = f.locationid
    LIMIT 10000;
                """

df = fetch_data(query)

# Create plotly scatter mapbox figure
fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", color="temperature", color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=3, hover_data=["timestampiso"])
fig.update_layout(mapbox_style="open-street-map")

# Create dash app
app = dash.Dash(__name__)

# Create app layout
app.layout = html.Div([
    html.H1("Weather Dashboard"),
    dcc.Graph(id="scatter-map", figure=fig)
])

# Run app
if __name__ == "__main__":
    app.run_server(debug=True)