# Use official Python image
FROM python:3.14-slim

# Set working directory inside container
WORKDIR /app

# Copy project files into container
COPY . /app

# Install dependencies (if requirements.txt exists)
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the Python script
ENTRYPOINT ["python", "nc_lightsource_asyncua_server.py", "--backoff-interval", "10", "--auto-reconnect"]
CMD ["--opcua-endpoint", "opc.tcp://0.0.0.0:48099",  "--address=10.11.4.69", "--product",  "FF"]