from kafka import KafkaProducer
import json
import concurrent.futures
import requests  # For making HTTP requests to the ML model inference service

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ML Model inference endpoint (Assume it's exposed via an API)
ML_MODEL_INFERENCE_URL = "http://localhost:5001/predict"  # Replace with actual model endpoint

# Function to send data to Kafka topic
def send_to_topic(topic, message):
    """
    Sends a message to a specified Kafka topic.
    """
    producer.send(topic, value=message)
    #print(f"Sent to {topic}: {message}")

# Function to fetch prediction from ML model (synchronously)

# Function to process incoming transactions and send predictions to Kafka
def process_and_produce(result, topic):
    topic = "Resulting-data"
    """
    Process a transaction, get the prediction from ML model, and produce to Kafka.
    """
    # Fetch prediction asynchronously
    prediction = fetch_prediction(result)

    # Prepare the message to send to Kafka (Resulting-data topic)
    result = {
        'TransactionID': result.transaction_id,
        'TransactionAmt': result.transaction_amt,
        'isFraud': result.prediction
    }

    # Send the message to the Kafka topic (Resulting-data)
    send_to_topic('Resulting-data', result)
    return result

# Initialize a thread pool for concurrent processing
executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# Example of producing data
def produce_data(result):
    # Run the process_and_produce function in a separate thread
    future = executor.submit(process_and_produce, transaction_id, transaction_amt)
    # Wait for the result (which includes transaction_id, transaction_amt, and isFraud)
    result = future.result()

    # Return the result (transaction details)
    return result
