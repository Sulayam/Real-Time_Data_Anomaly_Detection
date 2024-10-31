import os
import time
import random
import math
from datetime import datetime, timezone
from confluent_kafka import Producer
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def delivery_report(err, msg):
    """
    Callback function for delivery reports from the producer.
    Logs the status of message delivery.

    Args:
        err (KafkaError): Error information, if any.
        msg (Message): The message that was produced.
    """
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def create_producer():
    """
    Create and return a Kafka producer with specified configuration.

    Returns:
        Producer: ---> A Kafka producer instance. <---
    """
    conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        'linger.ms': 5,
        'batch.num.messages': 1000
    }
    return Producer(conf)

# List of power plant types and regions
power_plant_types = ['Gas Plant', 'Wind Farm', 'Solar Farm', 'Hydroelectric Plant']
regions = ['North', 'South', 'East', 'West']

# Initialize the producer
producer = create_producer()

def generate_data(counter):
    """
    Generate a data point with seasonal patterns, concept drift, and potential anomalies.

    Args:
        counter (int): Counter to simulate time progression for seasonal patterns.

    Returns:
        dict: Simulated data point with various metrics.
    """
    # Randomly choose power plant type and region
    plant_type = random.choice(power_plant_types)
    region = random.choice(regions)

    # Simulate seasonal variation using sinusoidal functions
    time_factor = counter / 100.0  # Adjust to control frequency

    # Base values for different metrics
    power_output_base = 100  # in MW (megawatts)
    demand_base = 200  # in MW
    grid_frequency_base = 50  # in Hz
    fuel_consumption_base = 300  # in kg/hr (for Gas Plant)

    # Seasonal variations
    power_output_seasonal = 30 * math.sin(2 * math.pi * time_factor / 24)  # 24-hour cycle
    demand_seasonal = 50 * math.sin(2 * math.pi * time_factor / 24 + math.pi / 4)
    grid_frequency_seasonal = 0.1 * math.sin(2 * math.pi * time_factor / 12)  # 12-hour cycle
    fuel_consumption_seasonal = 20 * math.sin(2 * math.pi * time_factor / 24)  # For Gas Plant

    # Concept drift (e.g., gradual increase in power demand)
    demand_drift = 0.05 * counter

    # Generate data point
    data = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'plant_type': plant_type,
        'region': region,
        'power_output': power_output_base + power_output_seasonal + random.uniform(-5, 5),
        'demand': demand_base + demand_seasonal + demand_drift + random.uniform(-10, 10),
        'grid_frequency': grid_frequency_base + grid_frequency_seasonal + random.uniform(-0.05, 0.05)
    }

    # Add plant-specific fields
    if plant_type == 'Gas Plant':
        data.update({
            'fuel_consumption': fuel_consumption_base + fuel_consumption_seasonal + random.uniform(-10, 10),
            'emissions': random.uniform(100, 300),  # CO2 emissions in kg/hr
        })
    elif plant_type == 'Wind Farm':
        data.update({
            'wind_speed': random.uniform(3, 25),  # in m/s
            'turbine_efficiency': random.uniform(80, 95),  # in percentage
        })
    elif plant_type == 'Solar Farm':
        data.update({
            'solar_radiation': random.uniform(200, 1000),  # in W/m²
            'panel_temperature': random.uniform(20, 80),  # in °C
        })
    elif plant_type == 'Hydroelectric Plant':
        data.update({
            'water_flow_rate': random.uniform(50, 300),  # in cubic meters per second
            'turbine_rotation_speed': random.uniform(100, 500),  # in RPM
        })

    # Inject anomalies with a 10% chance
    if random.random() < 0.1:
        if plant_type == 'Gas Plant':
            data['fuel_consumption'] *= random.uniform(1.5, 2.0)
            data['emissions'] *= random.uniform(1.2, 1.5)
        elif plant_type == 'Wind Farm':
            data['wind_speed'] *= random.uniform(0.5, 0.7)  # Extreme low wind speed
            data['turbine_efficiency'] *= random.uniform(0.5, 0.8)  # Drop in efficiency
        elif plant_type == 'Solar Farm':
            data['solar_radiation'] *= random.uniform(1.5, 2.0)  # Extreme high radiation
            data['panel_temperature'] *= random.uniform(1.2, 1.5)  # High temperature
        elif plant_type == 'Hydroelectric Plant':
            data['water_flow_rate'] *= random.uniform(1.5, 2.0)  # High flow rate
            data['turbine_rotation_speed'] *= random.uniform(0.5, 0.7)  # Drop in speed

    # Data Validation: Ensure no negative values
    for key, value in data.items():
        if isinstance(value, (int, float)) and value < 0:
            data[key] = 0

    return data

def main():
    """Main function to produce data continuously."""
    counter = 0  # Counter to simulate time progression
    try:
        while True:
            # Generate data
            data = generate_data(counter)
            counter += 1

            # Serialize to JSON
            data_json = json.dumps(data)

            # Produce message to Kafka
            producer.produce(
                topic='energy_stream',
                value=data_json,
                callback=delivery_report
            )

            # Poll for delivery reports
            producer.poll(0)

            logging.debug(f'Produced: {data}')

            # Simulate real-time data generation
            time.sleep(0.125)

    except KeyboardInterrupt:
        logging.info("Data generation stopped by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()