from kafka import KafkaProducer
import json
import logging
from config import KAFKA_BOOTSTRAP_SERVERS

producer = None

def get_kafka_producer():
    """
    Initialize and return a Kafka producer instance.
    """
    global producer
    if not producer:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return producer

def send_to_kafka(topic, message):
    """
    Send a message to a Kafka topic.
    """
    try:
        get_kafka_producer().send(topic, value=message)
    except Exception as e:
        logging.error(f"Error sending message to Kafka topic '{topic}': {e}")
