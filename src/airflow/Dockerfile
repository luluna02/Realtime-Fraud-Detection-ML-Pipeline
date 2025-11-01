FROM apache/airflow:3.1.0

# Create models directory and set ownership
USER root
RUN mkdir -p /app/models

USER airflow
# Copy requirements file
COPY requirements.txt /tmp/requirements.txt


# Install Python packages as root (system-wide)
RUN pip install --default-timeout=1000 --retries=10 -r /tmp/requirements.txt


# Switch back to airflow user for safety
USER airflow