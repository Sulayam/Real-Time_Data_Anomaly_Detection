# Real-Time Energy Data Anomaly Detection

This project implements a real-time energy data pipeline that simulates data from various power plants, detects anomalies using machine learning algorithms, and visualizes the results in a web dashboard. The system uses **Apache Kafka** for data streaming, **Apache Spark** for data processing and anomaly detection, and **Dash** for real-time visualization. All components are containerized with **Docker** and orchestrated using **Docker Compose**.

---

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Prerequisites](#prerequisites)
4. [Setup Instructions](#setup-instructions)
   - [Clone the Repository](#clone-the-repository)
   - [Directory Structure](#directory-structure)
5. [Building and Running the Docker Containers](#building-and-running-the-docker-containers)
   - [Building the Docker Images](#building-the-docker-images)
   - [Starting the Containers](#starting-the-containers)
6. [Accessing the Dash Application](#accessing-the-dash-application)
7. [System Flow Explanation](#system-flow-explanation)
8. [User Interface (UI) Screenshots](#user-interface-ui-screenshots)
9. [Kafka CLI Commands](#kafka-cli-commands)
10. [Monitoring the Spark Application](#monitoring-the-spark-application)
11. [Stopping and Cleaning Up](#stopping-and-cleaning-up)
12. [Troubleshooting](#troubleshooting)
13. [Why Use Isolation Forest for Anomaly Detection](#why-use-isolation-forest-for-anomaly-detection)
14. [Acknowledgments](#acknowledgments)

---

## Introduction

This project simulates real-time data from different types of power plants, including **Gas Plants**, **Wind Farms**, **Solar Farms**, and **Hydroelectric Plants**. The data includes seasonal patterns, concept drift, and potential anomalies. The data is streamed to **Apache Kafka**, processed in real-time using **Apache Spark Structured Streaming**, and anomalies are detected using the **Isolation Forest** algorithm. A **Dash web application** visualizes the real-time data and detected anomalies.

## Architecture Overview

The system architecture includes the following components:

- **Data Producer**: A Python script that generates synthetic energy data and publishes it to a Kafka topic.
- **Kafka**: Serves as the data streaming platform for real-time data ingestion.
- **Spark Streaming**: Consumes data from Kafka, performs data processing and anomaly detection.
- **Anomaly Detection**: Uses the Isolation Forest algorithm from scikit-learn to detect anomalies.
- **Dash Application**: Visualizes the real-time data and anomalies using interactive graphs and tables.
- **Docker Compose**: Orchestrates all services in a multi-container Docker application.

## Prerequisites

- **Docker** and **Docker Compose** installed on your machine.
- Allocate at least **4GB of RAM** to Docker for smooth operation.
- Internet connection to pull Docker images and download dependencies.

## Setup Instructions

### Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/yourusername/energy-anomaly-detection.git
cd energy-anomaly-detection
```

### Directory Structure
The repository has the following structure:
```
.
├── producer
│   ├── producer.py
│   └── Dockerfile
├── app
│   ├── app.py
│   └── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```
* producer: Contains the data producer script and its Dockerfile.
* app: Contains the Dash application script and its Dockerfile.
* docker-compose.yml: Defines all the services and their configurations.
* requirements.txt: Lists the Python dependencies for the project.
* README.md: Documentation of the project.

### Building and Running the Docker Containers
#### Building the Docker Images
From the root directory of the repository, build the Docker images for the producer and the app:
```bash
docker-compose build
```

This command builds the Docker images as specified in the docker-compose.yml file and the respective Dockerfiles in producer and app directories.

### Starting the Containers
#### Start all the services using Docker Compose:

```bash
docker-compose up
```
This will start the following services:

* zookeeper: Required for Kafka.
* kafka: The message broker for streaming data.
* spark-master: Spark master node for processing data.
* spark-worker: Spark worker node.
* producer: The data producer that generates and sends data to Kafka.
* app: The Dash web application for visualization.
**Note:** The first time you run docker-compose up, it may take some time to download the required Docker images and install dependencies.

### Accessing the Dash Application
Once all services are up and running, you can access the Dash web application in your web browser:

```
http://localhost:8050
```
The dashboard provides real-time visualization of the energy data and detected anomalies for each power plant type.

**Important:** It may take a few minutes for the data to start flowing and the anomalies to be detected as the services initialize and start processing data.

## System Flow Explanation

The system flow can be summarized in the following steps:

#### Data Generation
- The data producer (`producer.py`) simulates energy data from various power plant types.
- Data includes seasonal patterns, random noise, concept drift, and injected anomalies.
- Data is serialized to JSON and sent to a Kafka topic named `energy_stream`.

#### Data Streaming
- Apache Kafka acts as the data pipeline, enabling real-time data streaming between the producer and consumers.
- Data is stored temporarily in Kafka topics, awaiting consumption.

#### Data Processing and Anomaly Detection
- Apache Spark Structured Streaming consumes data from the Kafka topic.
- Data is parsed and transformed into a structured format.
- The Isolation Forest algorithm is applied to detect anomalies in real-time.
- The model is retrained on a sliding window of recent data to adapt to concept drift and seasonal variations.
- Detected anomalies are stored separately for visualization.

#### Data Visualization
- The Dash application (`app.py`) retrieves processed data and detected anomalies.
- Real-time graphs and tables are generated for each power plant type.
- Users can interact with the dashboard to monitor operations and anomalies.

#### Containerization and Orchestration
- All components are containerized using Docker, ensuring consistent environments and dependencies.
- Docker Compose orchestrates the services, defining networks and service dependencies.


## User Interface (UI) Screenshots

Below are screenshots of the Dash application showcasing the real-time visualization of energy data and detected anomalies:

- **Gas Plant Operations**
- **Wind Farm Operations**
- **Solar Farm Operations**
- **Hydroelectric Plant Operations**

## Monitoring the Spark Application
You can monitor the Spark application using the Spark Master web UI:

```
http://localhost:8080
```
This interface provides information about the Spark jobs, stages, tasks, storage, environment, and executors.

## Stopping and Cleaning Up
To stop all running containers:

```bash
docker-compose down
```

To remove all containers, networks, volumes, and images created by docker-compose up:

```bash
docker-compose down --rmi all --volumes --remove-orphans
```
Alternatively, to stop and remove all Docker containers and images (use with caution as this affects all Docker containers and images on your system):

```bash
docker stop $(docker ps -q)
docker rm $(docker ps -aq)
docker system prune -a
```

- **Logs:** Check the logs of individual services for errors:
```bash
docker-compose logs kafka
docker-compose logs producer
docker-compose logs app
```

## Why Use Isolation Forest for Anomaly Detection
- **Unsupervised Learning Approach:** Isolation Forest doesn't require labeled data, making it ideal for real-time data streams.
- **Efficient for High-Dimensional Data:** Suitable for multi-feature data like power plant metrics.
- **Concept Drift Adaptability:** Adapts to changes in data distribution over time with a sliding window retraining approach.
- **Fast and Scalable:** Linear time complexity makes it efficient for real-time processing.
- **Robustness to Anomalies:** Detects anomalies effectively by isolating them rather than profiling normal data points.
- **Minimal Parameter Tuning:** Simple setup makes it ideal for real-time systems where quick deployment is essential.