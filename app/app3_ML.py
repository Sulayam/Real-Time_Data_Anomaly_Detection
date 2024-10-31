import os
import threading
import pandas as pd
import numpy as np
import logging
import time
from collections import deque

import dash
from dash.dependencies import Output, Input
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from sklearn.ensemble import IsolationForest

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

# Thread-safe data storage
data_lock = threading.Lock()

# Data structures for each power plant type
plant_types = ['Gas Plant', 'Wind Farm', 'Solar Farm', 'Hydroelectric Plant']

# Define features for each plant type
plant_features = {
    'Gas Plant': ['power_output', 'demand', 'fuel_consumption', 'emissions'],
    'Wind Farm': ['power_output', 'demand', 'wind_speed', 'turbine_efficiency'],
    'Solar Farm': ['power_output', 'demand', 'solar_radiation', 'panel_temperature'],
    'Hydroelectric Plant': ['power_output', 'demand', 'water_flow_rate', 'turbine_rotation_speed']
}

# Initialize data storage with a sliding window
window_size = 500  # Adjust based on memory and performance
data_store = {
    plant_type: {
        'data': deque(maxlen=window_size),
        'outliers': pd.DataFrame()
    } for plant_type in plant_types
}

def start_spark_streaming():
    """
    Start Spark Structured Streaming to read data from Kafka and process it in real-time.
    """
    # Create Spark Session
    spark = SparkSession \
        .builder \
        .appName("EnergyStreamOutlierDetection") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "app") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    # Define schema for incoming data
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("plant_type", StringType()),
        StructField("region", StringType()),
        StructField("power_output", DoubleType()),
        StructField("demand", DoubleType()),
        StructField("grid_frequency", DoubleType()),
        StructField("fuel_consumption", DoubleType()),
        StructField("emissions", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("turbine_efficiency", DoubleType()),
        StructField("solar_radiation", DoubleType()),
        StructField("panel_temperature", DoubleType()),
        StructField("water_flow_rate", DoubleType()),
        StructField("turbine_rotation_speed", DoubleType())
    ])

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')) \
        .option("subscribe", "energy_stream") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

    # Parse the JSON data
    df = df.selectExpr("CAST(value AS STRING)")
    df_parsed = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Convert timestamp to proper format
    df_parsed = df_parsed.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Function to process each batch
    def process_batch(batch_df, batch_id):
        """
        Process each micro-batch of data from Spark Streaming.

        Args:
            batch_df (DataFrame): The batch data as a Spark DataFrame.
            batch_id (int): The batch ID.
        """
        try:
            pandas_df = batch_df.toPandas()
            logging.info(f"Processing batch {batch_id} with {len(pandas_df)} records")

            with data_lock:
                for plant_type in plant_types:
                    # Filter data for the current plant type
                    plant_df = pandas_df[pandas_df['plant_type'] == plant_type]
                    if plant_df.empty:
                        continue

                    # Select relevant features
                    features = ['timestamp'] + plant_features[plant_type]
                    plant_df = plant_df[features].dropna()

                    if plant_df.empty:
                        continue

                    # Data Validation: Check data types and ranges
                    for feature in plant_features[plant_type]:
                        if not pd.api.types.is_numeric_dtype(plant_df[feature]):
                            plant_df = plant_df.drop(columns=[feature])
                            logging.warning(f"Dropped non-numeric feature {feature} in {plant_type}")

                    # Append new data to the deque (sliding window)
                    data_entries = data_store[plant_type]['data']
                    data_entries.extend(plant_df.to_dict('records'))

        except Exception as e:
            logging.error(f"Error processing batch {batch_id}: {e}")

    # Apply processing to each micro-batch
    query = df_parsed.writeStream \
        .trigger(processingTime='1 second') \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()

def perform_outlier_detection():
    """
    Perform outlier detection using Isolation Forest on a sliding window.
    """
    while True:
        with data_lock:
            for plant_type in plant_types:
                data_entries = data_store[plant_type]['data']
                if len(data_entries) < 50:
                    continue  # Need sufficient data

                data_df = pd.DataFrame(list(data_entries))
                features = plant_features[plant_type]
                data_features = data_df[features].astype(float)

                # Algorithm Explanation:
                # Isolation Forest is an unsupervised learning algorithm for anomaly detection
                # that isolates anomalies instead of profiling normal data points. It works well
                # with high-dimensional data and is effective in detecting anomalies in the presence
                # of concept drift and seasonal variations due to its tree-based structure.

                # Fit Isolation Forest on sliding window
                isolation_forest = IsolationForest(contamination=0.05, random_state=42)
                outlier_labels = isolation_forest.fit_predict(data_features)

                # Identify outliers
                outlier_indices = np.where(outlier_labels == -1)[0]
                if len(outlier_indices) > 0:
                    outliers_df = data_df.iloc[outlier_indices]
                    data_store[plant_type]['outliers'] = outliers_df
                    logging.info(f"Detected {len(outlier_indices)} outliers for {plant_type}")
                else:
                    data_store[plant_type]['outliers'] = pd.DataFrame()

                # Limit outliers DataFrame size
                max_outliers = 100
                if len(data_store[plant_type]['outliers']) > max_outliers:
                    data_store[plant_type]['outliers'] = data_store[plant_type]['outliers'].iloc[-max_outliers:]

        time.sleep(5)  # Wait before next detection cycle

# Initialize Dash app
external_stylesheets = [dbc.themes.FLATLY]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Cobblestone Energy Efficient Data Stream Anomaly Detection"

# Custom theme colors
theme_color_1 = '#000000'  # Black
theme_color_2 = '#94c8d0'  # Light blue
theme_color_3 = '#8bbec2'  # Light blue
theme_color_4 = '#7eb0b2'  # Light blue
theme_color_5 = '#94c5ce'  # Light blue

# Define app layout
app.layout = dbc.Container(
    [
        dbc.Navbar(
            dbc.Container(
                dbc.NavbarBrand(
                    "Cobblestone Energy Efficient Data Stream Anomaly Detection",
                    className="ms-2",
                    style={'color': theme_color_1, 'fontSize': '24px', 'fontWeight': 'bold'}
                ),
            ),
            color=theme_color_2,
            dark=False,
            sticky="top",
            style={'marginBottom': '20px'}
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H4("Gas Plant Operations", className="text-center", style={'color': theme_color_1}),
                        dcc.Graph(id='gas-plant-graph', animate=False),
                        html.H5("Detected Anomalies", className="text-center", style={'color': theme_color_1}),
                        dash_table.DataTable(
                            id='gas-plant-table',
                            columns=[{'name': i.replace('_', ' ').title(), 'id': i} for i in plant_features['Gas Plant']],
                            style_table={'overflowX': 'auto'},
                            style_cell={'textAlign': 'left', 'minWidth': '100px'},
                            style_header={'backgroundColor': theme_color_3, 'fontWeight': 'bold'}
                        ),
                    ],
                    width=12,
                    style={'marginBottom': '20px'}
                ),
                dbc.Col(
                    [
                        html.H4("Wind Farm Operations", className="text-center", style={'color': theme_color_1}),
                        dcc.Graph(id='wind-farm-graph', animate=False),
                        html.H5("Detected Anomalies", className="text-center", style={'color': theme_color_1}),
                        dash_table.DataTable(
                            id='wind-farm-table',
                            columns=[{'name': i.replace('_', ' ').title(), 'id': i} for i in plant_features['Wind Farm']],
                            style_table={'overflowX': 'auto'},
                            style_cell={'textAlign': 'left', 'minWidth': '100px'},
                            style_header={'backgroundColor': theme_color_3, 'fontWeight': 'bold'}
                        ),
                    ],
                    width=12,
                    style={'marginBottom': '20px'}
                ),
                dbc.Col(
                    [
                        html.H4("Solar Farm Operations", className="text-center", style={'color': theme_color_1}),
                        dcc.Graph(id='solar-farm-graph', animate=False),
                        html.H5("Detected Anomalies", className="text-center", style={'color': theme_color_1}),
                        dash_table.DataTable(
                            id='solar-farm-table',
                            columns=[{'name': i.replace('_', ' ').title(), 'id': i} for i in plant_features['Solar Farm']],
                            style_table={'overflowX': 'auto'},
                            style_cell={'textAlign': 'left', 'minWidth': '100px'},
                            style_header={'backgroundColor': theme_color_3, 'fontWeight': 'bold'}
                        ),
                    ],
                    width=12,
                    style={'marginBottom': '20px'}
                ),
                dbc.Col(
                    [
                        html.H4("Hydroelectric Plant Operations", className="text-center", style={'color': theme_color_1}),
                        dcc.Graph(id='hydroelectric-graph', animate=False),
                        html.H5("Detected Anomalies", className="text-center", style={'color': theme_color_1}),
                        dash_table.DataTable(
                            id='hydroelectric-table',
                            columns=[{'name': i.replace('_', ' ').title(), 'id': i} for i in plant_features['Hydroelectric Plant']],
                            style_table={'overflowX': 'auto'},
                            style_cell={'textAlign': 'left', 'minWidth': '100px'},
                            style_header={'backgroundColor': theme_color_3, 'fontWeight': 'bold'}
                        ),
                    ],
                    width=12,
                    style={'marginBottom': '20px'}
                ),
            ]
        ),
        dcc.Interval(
            id='graph-update',
            interval=1 * 1000,  # Update every second
            n_intervals=0
        ),
        html.Footer(
            dbc.Container(
                html.P(
                    "Cobblestone Energy Efficient Data Stream Anomaly Detection by Sulayam",
                    className="text-center",
                    style={'color': theme_color_1, 'padding': '10px'}
                )
            ),
            style={'backgroundColor': theme_color_4, 'marginTop': '20px'}
        )
    ],
    fluid=True,
    style={'backgroundColor': theme_color_4}
)

@app.callback(
    [
        Output('gas-plant-graph', 'figure'),
        Output('gas-plant-table', 'data'),
        Output('wind-farm-graph', 'figure'),
        Output('wind-farm-table', 'data'),
        Output('solar-farm-graph', 'figure'),
        Output('solar-farm-table', 'data'),
        Output('hydroelectric-graph', 'figure'),
        Output('hydroelectric-table', 'data'),
    ],
    [Input('graph-update', 'n_intervals')]
)
def update_graphs(n):
    """
    Update the graphs and tables in the Dash app with the latest data.

    Args:
        n (int): The number of intervals passed.

    Returns:
        tuple: Figures and table data for each power plant type.
    """
    try:
        with data_lock:
            figures = []
            table_data = []
            for plant_type in plant_types:
                data_entries = data_store[plant_type]['data']
                if not data_entries:
                    figures.append(go.Figure())
                    table_data.append([])
                    continue

                data_df = pd.DataFrame(list(data_entries))
                outliers_df = data_store[plant_type]['outliers']

                # Plot time series
                features = plant_features[plant_type]
                data_traces = []
                # Line colors for better visibility
                line_colors = ['#FF5733', '#33A1FF', '#28B463', '#AF7AC5']  # Red, Blue, Green, Purple

                for i, feature in enumerate(features):
                    data_traces.append(
                        go.Scatter(
                            x=data_df['timestamp'],
                            y=data_df[feature],
                            mode='lines',
                            name=f'{feature.replace("_", " ").title()}',
                            line=dict(width=2, color=line_colors[i % len(line_colors)]),
                            hoverinfo='text',
                            hovertext=[
                                f'Time: {t}<br>{feature.replace("_", " ").title()}: {v:.2f}'
                                for t, v in zip(data_df['timestamp'], data_df[feature])
                            ]
                        )
                    )

                # Add outlier markers
                if not outliers_df.empty:
                    for i, feature in enumerate(features):
                        data_traces.append(
                            go.Scatter(
                                x=outliers_df['timestamp'],
                                y=outliers_df[feature],
                                mode='markers',
                                name=f'Outliers ({feature.replace("_", " ").title()})',
                                marker=dict(color='red', size=8, symbol='x'),
                                hoverinfo='text',
                                hovertext=[
                                    f'Time: {t}<br>{feature.replace("_", " ").title()}: {v:.2f}'
                                    for t, v in zip(outliers_df['timestamp'], outliers_df[feature])
                                ]
                            )
                        )

                layout = go.Layout(
                    xaxis=dict(title='Time'),
                    yaxis=dict(title='Values'),
                    legend=dict(x=0, y=1),
                    hovermode='closest',
                    plot_bgcolor='white',  # Set chart background to white
                    paper_bgcolor='white',  # Set chart background to white
                    font=dict(color=theme_color_1),
                    title=f"{plant_type} Operations"
                )

                figures.append({'data': data_traces, 'layout': layout})

                # Prepare table data
                if not outliers_df.empty:
                    table_entries = outliers_df[features + ['timestamp']].to_dict('records')
                else:
                    table_entries = []

                table_data.append(table_entries)

        return (
            figures[0], table_data[0],
            figures[1], table_data[1],
            figures[2], table_data[2],
            figures[3], table_data[3],
        )
    except Exception as e:
        logging.error(f"Error in update_graphs: {e}")
        empty_figure = go.Figure()
        return empty_figure, [], empty_figure, [], empty_figure, [], empty_figure, []

if __name__ == '__main__':
    # Start Spark Streaming in a separate thread
    streaming_thread = threading.Thread(target=start_spark_streaming, daemon=True)
    streaming_thread.start()

    # Start Outlier Detection in a separate thread
    outlier_thread = threading.Thread(target=perform_outlier_detection, daemon=True)
    outlier_thread.start()

    # Run the Dash app
    app.run_server(debug=False, host='0.0.0.0', port=8050)