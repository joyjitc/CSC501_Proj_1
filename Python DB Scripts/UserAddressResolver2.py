import pymysql
import requests

# Database connection details
db_config = {
    'host': 'ancalagon2.mysql.database.azure.com',  # Replace with your host
    'user': 'ancalagon',  # Replace with your username
    'password': 'Annex9@call',  # Replace with your password
    'db': 'datasciencese',  # The database to connect to
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# Function to update the usersv2 table with the location details
def update_location_data(conn, user_id, lat, lon, addresstype, display_name):
    with conn.cursor() as cursor:
        query = """
        UPDATE usersv2
        SET lat = %s, `long` = %s, location_addresstype = %s, location_display_name = %s
        WHERE Id = %s
        """
        cursor.execute(query, (lat, lon, addresstype, display_name, user_id))

# Function to set a boolean field to TRUE when there's no data from the API
def set_field_to_true_when_no_data(conn, user_id):
    with conn.cursor() as cursor:
        query = """
        UPDATE usersv2
        SET loc_geoloc_na = 1
        WHERE Id = %s
        """
        cursor.execute(query, (user_id,))

# Function to make API call to Nominatim OpenStreetMap
def fetch_location_data(location):
    url = 'https://nominatim.openstreetmap.org/search.php'
    params = {
        'q': location,
        'polygon_geo': 0,
        'format': 'jsonv2'
    }
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,hi;q=0.8,en-IN;q=0.7,bn;q=0.6,lb;q=0.5,fi;q=0.4",
        "priority": "u=1, i",
        "referer": "https://nominatim.openstreetmap.org/ui/search.html?q=cordova+bay",
        "sec-ch-ua": "\"Chromium\";v=\"128\", \"Not;A=Brand\";v=\"24\", \"Google Chrome\";v=\"128\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    }
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if len(data) > 0:
            return data[0]  # Return the first result
    return None

# Main function to connect to DB, fetch users, and update the table
def main():
    # Connect to the database
    print("Connecting to the database...")
    connection = pymysql.connect(**db_config)

    try:
        print("Logged into the database.")
        with connection.cursor() as cursor:
            # Fetch rows where location is non-null
            cursor.execute("SELECT Id, Location FROM usersv2 WHERE Location IS NOT NULL AND lat IS NULL AND `long` IS NULL AND loc_geoloc_na = 0")

            rows = cursor.fetchall()

            print(f"Found {len(rows)} rows with non-null location.")

            # Initialize counter
            row_count = 0

            for row in rows:
                user_id = row['Id']
                location = row['Location']

                # Fetch data from Nominatim API
                location_data = fetch_location_data(location)

                if location_data:
                    lat = location_data.get('lat')
                    lon = location_data.get('lon')
                    addresstype = location_data.get('addresstype')
                    display_name = location_data.get('display_name')
                    display_name = display_name[:200]

                    if lat and lon:
                        # Update the table with the fetched data
                        update_location_data(connection, user_id, lat, lon, addresstype, display_name)
                    else:
                        set_field_to_true_when_no_data(connection, user_id)
                        print(f"Useless response fromm API, processed {row_count} number of rows")
                else:
                    set_field_to_true_when_no_data(connection, user_id)
                    print(f"No response fromm API, processed {row_count} number of rows")

                # Increment counter and print every 1000 rows
                row_count += 1
                if row_count % 100 == 0:
                    print(f"Processed {row_count} rows...")
                    connection.commit()

    finally:
        connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
