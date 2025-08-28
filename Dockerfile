# Use the official Airflow image as the base
FROM apache/airflow:2.8.2-python3.11

# Copy the requirements.txt file into the container's temporary directory
COPY requirements.txt .

# Install the Python packages from the requirements.txt file
RUN pip install --no-cache-dir -r requirements.txt