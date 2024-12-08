from db_utils import get_mysql_connection

connection = get_mysql_connection()
if connection:
    print("Connection successful!")
    connection.close()
else:
    print("Failed to connect to MySQL.")
