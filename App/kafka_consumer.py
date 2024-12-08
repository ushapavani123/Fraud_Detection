from kafka import KafkaConsumer
import json
import logging
from db_utilities import get_mysql_connection

# Configure logging
logging.basicConfig(level=logging.INFO)

def save_to_database(data):
    """
    Save data to the 'modelling_data' table in MySQL.
    """
    connection = get_mysql_connection()
    cursor = connection.cursor()

    try:
        # Define the SQL query to insert data into the table
        query = """
            INSERT INTO modelling_data (TransactionID, TransactionAmt, C1, C11, C13, C14, V187, V189, V244, V258, isFraud)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                TransactionAmt=VALUES(TransactionAmt),
                C1=VALUES(C1),
                C11=VALUES(C11),
                C13=VALUES(C13),
                C14=VALUES(C14),
                V187=VALUES(V187),
                V189=VALUES(V189),
                V244=VALUES(V244),
                V258=VALUES(V258),
                isFraud=VALUES(isFraud);
        """

        # Insert data into the table
        cursor.execute(query, (
            data['TransactionID'],
            data['TransactionAmt'],
            data['C1'],
            data['C11'],
            data['C13'],
            data['C14'],
            data['V187'],
            data['V189'],
            data['V244'],
            data['V258'],
            data['isFraud']
        ))
        connection.commit()
        logging.info(f"Inserted data: {data}")
    except Exception as e:
        logging.error(f"Error inserting data into database: {e}")
    finally:
        cursor.close()
        connection.close()

def consume_modelling_data():
    """
    Consume messages from the 'modelling-data' Kafka topic and save them to the database.
    """
    consumer = KafkaConsumer(
        'modelling-data',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logging.info("Started consuming messages from 'modelling-data' topic.")
    for message in consumer:
        try:
            # Save the consumed data to the database
            save_to_database(message.value)
        except Exception as e:
            logging.error(f"Error processing message: {e}")

if __name__ == '__main__':
    consume_modelling_data()
