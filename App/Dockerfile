FROM python:3.9-slim

# Install necessary system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Expose the Flask app port
EXPOSE 5000

# Command to run the application
CMD ["python", "app.py"]

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the model into the container
COPY trainModel.pkl /app/trainModel.pkl

RUN apt-get update && apt-get install -y iputils-ping

RUN pip install sqlalchemy
