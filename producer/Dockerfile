FROM python:3.9-slim-bullseye

# Install required libraries
RUN pip install --no-cache-dir confluent-kafka

# Copy your producer code into the container
COPY producer-3.py /app/producer-3.py

# Set the working directory
WORKDIR /app

# Run your producer script
CMD ["python", "producer-3.py"]