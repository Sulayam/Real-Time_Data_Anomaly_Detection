FROM python:3.9-slim-bullseye

# Install Java and procps (for 'ps' command)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    procps && \
    rm -rf /var/lib/apt/lists/*

# Determine architecture and set JAVA_HOME accordingly
RUN arch=$(dpkg --print-architecture) && \
    if [ "$arch" = "amd64" ]; then \
        JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"; \
    elif [ "$arch" = "arm64" ]; then \
        JAVA_HOME="/usr/lib/jvm/java-11-openjdk-arm64"; \
    else \
        echo "Unsupported architecture: $arch"; exit 1; \
    fi && \
    echo "JAVA_HOME is set to $JAVA_HOME" && \
    ln -s $JAVA_HOME /usr/lib/jvm/default-java

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$PATH:$JAVA_HOME/bin"

# Install Python dependencies including scikit-learn
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    dash \
    dash-bootstrap-components \
    pandas \
    plotly \
    confluent-kafka \
    scikit-learn  # Adding scikit-learn for Isolation Forest

# Copy your application code into the container
COPY app3_ML.py /app/app3_ML.py

# Set the working directory
WORKDIR /app

# Expose the port that Dash uses
EXPOSE 8050

# Run your application
CMD ["python", "app3_ML.py"]
