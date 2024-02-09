import os
import logging
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objs as go
from cassandra.cluster import Cluster
from dotenv import load_dotenv
from utils.configure_logging import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


KAFKA_TOPIC = os.getenv('TOPICS_ICE_DATA_NAME')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST')
CASSANDRA_PORT = os.getenv('CASSANDRA_PORT')
KEYSPACE = os.getenv('KEYSPACE')
TABLE_NAME = os.getenv('TABLE_NAME')
UPDATE_SECONDS = float(os.environ['UPDATE_SECONDS'])

# Establish connection to Cassandra outside the function
cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT))
session = cluster.connect(KEYSPACE)

# Initialize the Dash app
dash_app = Dash(__name__)
server = dash_app.server  # Expose Flask server for Gunicorn or other WSGI servers

# Define the layout of the app
dash_app.layout = html.Div([
    dcc.Graph(id='real-time-graph'),
    dcc.Interval(
        id='interval-component',
        interval=UPDATE_SECONDS * 1000,  # in milliseconds
        n_intervals=0
    )
])


def get_cassandra_data():
    """
    Retrieves the latest market data from Cassandra.
    Returns a list of dictionaries with 'timestamp' and 'close' keys.
    """
    logger.debug("Get data")

    query = "SELECT timestamp, close FROM {} WHERE ticker = '%TFM 1!-ICN' ORDER BY timestamp DESC LIMIT 20;".format(TABLE_NAME)
    try:
        rows = session.execute(query)

        data = [{'timestamp': row.timestamp, 'close': str(row.close)} for row in rows]
        logger.debug(f"Fetched {len(data)} records from Cassandra")
        return data
    except Exception as e:
        logger.error(f"Error fetching data from Cassandra: {e}")
        raise


@dash_app.callback(
    Output('real-time-graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_graph(n):
    """
    Callback function to update the graph with data from Cassandra.
    """
    logger.info("Updating graph")
    data_storage = get_cassandra_data()

    if not data_storage:
        logger.info("Data storage is empty.")
        return {'data': [], 'layout': go.Layout(title="No data available")}

    timestamps = [data['timestamp'] for data in data_storage]
    values = [data['close'] for data in data_storage]

    trace = go.Scatter(x=timestamps, y=values, name='Market Close', mode='lines+markers')
    layout = go.Layout(title='Real-time Market Data from Cassandra', xaxis=dict(title='Timestamp'), yaxis=dict(title='Close Price'))

    return {'data': [trace], 'layout': layout}


if __name__ == '__main__':
    logger.info('Dash app started')
    dash_app.run_server(debug=True, host="0.0.0.0", port=8050)
