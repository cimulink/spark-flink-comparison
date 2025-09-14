FROM bitnami/spark:3.4

USER root

COPY requirements.txt /app/

# Install Python dependencies
RUN pip install -r requirements.txt

# Create app directory
WORKDIR /app

# Copy application files
COPY order_processor.py /app/
COPY start.sh /app/

# Make start script executable
RUN chmod +x /app/start.sh

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3



ENTRYPOINT ["/app/start.sh"]