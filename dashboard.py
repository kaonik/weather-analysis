import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.express as px
from sqlalchemy import create_engine
import os
import pandas as pd

db_conn_params = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}

#Mapbox access token
px.set_mapbox_access_token(os.environ.get('MAPBOX_API_KEY'))

#Create SQLAlchemy engine
engine = create_engine(f"postgresql://{db_conn_params['user']}:{db_conn_params['password']}@{db_conn_params['host']}/{db_conn_params['dbname']}")

# Get latitude, longitude, and forecast data with locationid
query ="""
    SELECT DATE(f.timestampiso) AS date,
    AVG(f.temperature) AS avg_temperature,
    AVG(f.humidity) AS avg_humidity,
    AVG(f.windspeed) AS avg_wind_speed,
    AVG(f.pressure) AS avg_pressure,
    AVG(f.cloudiness) AS avg_cloudiness,
    AVG(f.visibility) AS avg_visibility,
    AVG(f.precipitationchance) AS avg_precipitation_chance,
    l.latitude, l.longitude
    FROM location l
    JOIN forecast f ON l.locationid = f.locationid
    WHERE EXTRACT(HOUR FROM f.timestampiso) = 12 and DATE(f.timestampiso) >= '2024-01-16'
    GROUP BY date, l.latitude, l.longitude
    limit 1000000;
"""
# Read query results into pandas dataframe
df = pd.read_sql(query,engine)

#Convert dates to string for slider
df['date_str'] = df['date'].astype(str)
# Create date slider options
date_options = [{'label': d, 'value': d} for d in df['date_str'].unique()]

# Create plotly scatter mapbox figure
fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", color="avg_temperature", 
                        color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=3, hover_data=["date","avg_temperature","avg_humidity","avg_wind_speed","avg_pressure"])

# Set Mapbox access token
fig.update_layout(
    mapbox=dict(
        accesstoken=os.environ.get('MAPBOX_API_KEY'),
        style="streets",
        # Set map bounds to the world bounds to prevent zooming out past world bounds
        bounds = {"west": -180, "east": 180, "south": -90, "north": 90},
    )
)
fig.update_layout(uirevision=True)
# Create dash app
app = dash.Dash(__name__)


# Create app layout
app.layout = html.Div([
    html.H1("Weather Dashboard"),
    dcc.Graph(
        id="scatter-map", 
        figure=fig,
        # Autosize height
        style={'height': '80vh'}
    ),
    # Create date slider
    dcc.Slider(
        id='date-slider',
        min=0,
        max=len(date_options)-1,
        value=0,
        marks={i: date_options[i]['label'] for i in range(len(date_options))},
    ),
])

# Create callback to update map figure when date slider changes
@app.callback(
    Output('scatter-map', 'figure'),
    [Input('date-slider', 'value')]
)
def update_map(selected_date_index):
    # Get selected date from slider
    selected_date = date_options[selected_date_index]['value']
    # Filter dataframe to only include selected date
    filtered_df = df[df['date_str'] == selected_date]
    
    # Create plotly scatter mapbox figure
    fig = px.scatter_mapbox(filtered_df, lat="latitude", lon="longitude", color="avg_temperature", 
                            color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=3, hover_data=["date","avg_temperature","avg_humidity","avg_wind_speed","avg_pressure"])
    
    # Update the figure attribute of the dcc.Graph component
    return {'data': fig.data, 'layout': fig.layout}
# Run app
if __name__ == "__main__":
    app.run_server(debug=True)
