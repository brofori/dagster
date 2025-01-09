FROM python:3.9-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy your code
COPY cloud_run_example.py /app/
WORKDIR /app

# The entrypoint will be provided by Dagster 