import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import errorcode  # Import errorcode
import logging
from config import MYSQL_CONFIG

# Connection Pool
try:
    pool = MySQLConnectionPool(pool_name="mypool", pool_size=5, **MYSQL_CONFIG)
    logging.info("MySQL connection pool created successfully.")
except mysql.connector.Error as err:
    logging.error(f"Error creating connection pool: {err}")
    pool = None

def get_mysql_connection():
    """
    Get a MySQL connection from the pool.
    """
    try:
        return pool.get_connection()
    except mysql.connector.Error as err:
        logging.error(f"Error getting connection from pool: {err}")
        if err.errno == errorcode.CR_CONNECTION_ERROR:
            logging.error("Unable to connect to MySQL server. Check host and port settings.")
        elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Invalid credentials. Check username and password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Database does not exist.")
        return None
