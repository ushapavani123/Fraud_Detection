import pickle

import mysql.connector
from mysql.connector import Error
import logging
import pandas as pd  # For handling CSV data
import joblib  # For loading the trained ML model
import os

# MySQL Configuration
MYSQL_CONFIG = {
    'host': 'mysql',  # Use Docker Compose service name or IP address
    'user': 'app_user',
    'password': 'app_password',
    'database': 'fraud_detection',
    'port': 3306
}

# Database Initialization
def initialize_database():
    """
    Initializes the database by creating required tables.
    """
    logging.info(f"Attempting to connect to MySQL at {MYSQL_CONFIG['host']} as user {MYSQL_CONFIG['user']}.")
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        logging.info("Database connection successful.")
        cursor = connection.cursor()

        # Table creation query
        table_query = """
        CREATE TABLE IF NOT EXISTS modelling_data (
            TransactionID INT NOT NULL PRIMARY KEY,
            TransactionAmt DECIMAL(10, 2),
            C1 VARCHAR(255),
            C11 VARCHAR(255),
            C13 VARCHAR(255),
            C14 VARCHAR(255),
            V187 DECIMAL(10, 2),
            V189 DECIMAL(10, 2),
            V244 DECIMAL(10, 2),
            V258 DECIMAL(10, 2),
            isFraud BOOLEAN
        );
        """
        try:
            cursor.execute(table_query)
            logging.info("Table 'modelling_data' created successfully.")
        except Error as err:
            logging.error(f"Error creating table 'modelling_data': {err}")

        # Commit and close connection
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Database initialization completed successfully.")

    except Error as err:
        logging.error(f"Error initializing database: {err}")

# Insert Data into Table
def insert_data_to_table():
    """
    Insert data from a dataset into the modelling_data table only if the column names match.
    If TransactionID already exists, update the existing row.
    """
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        logging.info("Inserting data into the database...")
        cursor = connection.cursor()

        # Load the dataset
        dataset_path = '/app/cleaned_df.csv'  # Ensure the file is present in the container
        logging.info(f"Loading dataset from {dataset_path}")
        df = pd.read_csv(dataset_path)

        # Define the table columns as a list (ensure they match the actual table columns)
        table_columns = [
            'TransactionID', 'TransactionAmt', 'C1', 'C11', 'C13', 'C14',
            'V187', 'V189', 'V244', 'V258', 'isFraud'
        ]

        # Check if dataset columns match the table columns
        matching_columns = [col for col in df.columns if col in table_columns]
        logging.info(f"Matching columns: {matching_columns}")

        # Ensure the DataFrame has only the matching columns
        df = df[matching_columns]

        # Insert data into the table
        for _, row in df.iterrows():
            logging.info(f"Inserting row: {row.tolist()}")
            insert_query = """
            INSERT INTO modelling_data (
                TransactionID, TransactionAmt, C1, C11, C13, C14, V187, V189, V244, V258, isFraud
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                TransactionAmt = VALUES(TransactionAmt),
                C1 = VALUES(C1),
                C11 = VALUES(C11),
                C13 = VALUES(C13),
                C14 = VALUES(C14),
                V187 = VALUES(V187),
                V189 = VALUES(V189),
                V244 = VALUES(V244),
                V258 = VALUES(V258),
                isFraud = VALUES(isFraud)
            """
            try:
                cursor.execute(insert_query, tuple(row))
            except Error as err:
                logging.error(f"Error inserting row {row['TransactionID']}: {err}")

        connection.commit()
        logging.info("Data successfully inserted into the modelling_data table.")

        cursor.close()
        connection.close()

    except Error as err:
        logging.error(f"Error inserting data: {err}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

# Fetch Data for ML Model
def fetch_data_for_ml():
    """
    Fetches data from the 'modelling_data' table and prepares it for the ML model.
    """
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        logging.info("Fetching data from MySQL...")
        query = "SELECT * FROM modelling_data"
        df = pd.read_sql(query, connection)
        connection.close()
        logging.info("Data fetched successfully.")
        return df
    except Error as err:
        logging.error(f"Error fetching data from MySQL: {err}")
        return None

# Prepare Data for the ML Model
def prepare_data_for_model(df):
    """
    Preprocess and prepare data for the ML model (example steps).
    """
    # For example, let's assume you're predicting 'isFraud'
    X = df.drop(columns=['isFraud'])  # Drop non-relevant columns
    y = df['isFraud']  # Target variable
    return X, y

# Load the Trained ML Model
def load_model():
    model_filename = '/app/trainModel.pkl'  # Path to the model file
    try:
        with open(model_filename, 'rb') as file:
            file.seek(0)
            model = pickle.load(file)
        logging.info(f'Model loaded successfully: {model}')
        return model
    except pickle.UnpicklingError as e:
        logging.error(f"Error unpickling the model: {e}")
    except FileNotFoundError as e:
        logging.error(f"Model file not found: {e}")
    except Exception as e:
        logging.error(f"Error loading model: {e}")
    return None

# Make Predictions Using the ML Model
def make_predictions(model, X):
    """
    Make predictions using the trained ML model.
    """
    try:
        predictions = model.predict(X)
        logging.info("Predictions made successfully.")
        return predictions
    except Exception as e:
        logging.error(f"Error making predictions: {e}")
        return None

# Flask Application
from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return "Welcome to the Fraud Detection Application!"

@app.route('/predict')
def predict():
    # Fetch data and prepare it for the model
    df = fetch_data_for_ml()
    if df is not None:
        X, y = prepare_data_for_model(df)

        # Load the trained model
        model = load_model()
        if model:
            # Make predictions
            predictions = make_predictions(model, X)
            return f"Predictions: {predictions}"
        else:
            return "Model could not be loaded."

    return "Error fetching data from MySQL."

# Entry Point
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # Initialize the database
    logging.info("Initializing the database...")
    initialize_database()

    # Insert data into the table
    logging.info("Inserting data into the modelling_data table...")
    insert_data_to_table()

    # Run Flask app
    app.run(host='0.0.0.0', port=5000, debug=True)
