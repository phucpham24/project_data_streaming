# Base image: Flink 1.17.0
FROM flink:1.17.0

# Update and install Python 3, pip3, and development tools
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Create a symbolic link for python3 to be used as 'python'
RUN ln -s /usr/bin/python3 /usr/bin/python

# Download Flink Kafka connector, JDBC connector, and PostgreSQL JDBC driver
RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar && \
    wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.0.0-1.16/flink-connector-jdbc-3.0.0-1.16.jar && \
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Create directories for checkpoints and savepoints
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints

# Configure Flink to export Prometheus metrics
RUN echo "metrics.reporters: prom" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
    echo "metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
    echo "metrics.reporter.prometheus.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
    echo "metrics.reporter.prom.port: 9249" >> "$FLINK_HOME/conf/flink-conf.yaml";

# Copy Python dependencies file (requirements.txt)
COPY requirements.txt /opt/flink/

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /opt/flink/requirements.txt
