import pymysql
import json

# Database connection details
db_config = {
    'host': 'ancalagon2.mysql.database.azure.com',  # Replace with your host
    'user': 'ancalagon',  # Replace with your username
    'password': 'Annex9@call',  # Replace with your password
    'db': 'datasciencese',  # The database to connect to
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# File path to the JSON file
file_path = '/Users/joyjitchoudhury/Downloads/address_country_map_final.json.json'  # Replace with the actual file path

# Function to load JSON data from a file
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

# Function to insert data in batches of 100
def insert_data_in_batches(conn, data, batch_size=100):
    batch = []
    for location, country_code in data.items():
        batch.append((location, country_code))

        if len(batch) == batch_size:
            with conn.cursor() as cursor:
                cursor.executemany('''
                    INSERT INTO location_country_mapping (location, country_code)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                    location=VALUES(location), country_code=VALUES(country_code)
                ''', batch)
            conn.commit()
            batch = []

    # Insert remaining records
    if batch:
        with conn.cursor() as cursor:
            cursor.executemany('''
                INSERT INTO location_country_mapping (location, country_code)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                location=VALUES(location), country_code=VALUES(country_code)
            ''', batch)
        conn.commit()

# Main function to connect to DB and execute the tasks
def main():
    # Connect to the database
    print("Connecting to the database...")
    connection = pymysql.connect(**db_config)

    try:
        print("Loading data from JSON file...")
        data = load_json_data(file_path)

        print("Inserting data into the database...")
        insert_data_in_batches(connection, data)
        print("Data inserted successfully.")

    finally:
        connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
