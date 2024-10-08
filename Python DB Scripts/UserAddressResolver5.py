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

# Function to set loc_geoloc_na to TRUE when there's no data from the API
def set_loc_geoloc_na(conn, user_id):
    with conn.cursor() as cursor:
        query = """
        UPDATE usersv2
        SET loc_geoloc_na = 1
        WHERE Id = %s
        """
        cursor.execute(query, (user_id,))

# Function to insert the API response into usersLocation table
def insert_location_data_bulk(conn, data):
    if not data:
        return
    with conn.cursor() as cursor:
        query = """
        INSERT INTO usersLocation (user_id, lat, lon, suburb, city, county, state, ISO3166_2_lvl4, country, country_code, loc_revgeoloc_na)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
        """
        cursor.executemany(query, data)

# Function to update usersv2 table with the location details
def update_usersv2_bulk(conn, data):
    if not data:
        return
    with conn.cursor() as cursor:
        query = """
        UPDATE usersv2
        SET lat = %s, `long` = %s, location_display_name = %s
        WHERE Id = %s
        """
        cursor.executemany(query, data)

# Function to make API call to Nominatim OpenStreetMap Search endpoint
def fetch_location_data(location_query):
    url = 'https://nominatim.openstreetmap.org/search.php'
    params = {
        'q': location_query,
        'format': 'jsonv2',
        'addressdetails': 1  # Include additional address details in the response
    }
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,hi;q=0.8,en-IN;q=0.7,bn;q=0.6,lb;q=0.5,fi;q=0.4',
        'priority': 'u=1, i',
        'referer': 'https://nominatim.openstreetmap.org/ui/search.html',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if len(data) > 0:
            return data[0]  # Return the first result
    return None

# Main function to connect to DB, fetch users, and update the tables
def main():
    # Connect to the database
    print("Connecting to the database...")
    connection = pymysql.connect(**db_config)

    try:
        print("Logged into the database.")
        with connection.cursor() as cursor:
            # Fetch user_id and Location from usersv2 where lat/long is NULL
            cursor.execute("""
            SELECT u.Id, u.Location 
            FROM usersv2 u
            WHERE u.Location IS NOT NULL
              AND u.lat IS NULL
              AND u.long IS NULL
              AND u.loc_geoloc_na = 0
            """)

            rows = cursor.fetchall()

            print(f"Found {len(rows)} rows with non-null location and no existing lat/long.")

            # Initialize counter and data lists
            row_count = 0
            data_to_insert_location = []
            data_to_update_usersv2 = []

            for row in rows:
                user_id = row['Id']
                location_query = row['Location']

                # Fetch data from Nominatim API
                location_data = fetch_location_data(location_query)

                if location_data:
                    api_lat = location_data.get('lat')
                    api_lon = location_data.get('lon')
                    display_name = location_data.get('display_name', '')
                    display_name = display_name[:200]
                    address = location_data.get('address', {})

                    # Prepare data for usersLocation table
                    data_to_insert_location.append((
                        user_id,
                        api_lat,
                        api_lon,
                        address.get('suburb'),
                        address.get('city'),
                        address.get('county'),
                        address.get('state'),
                        address.get('ISO3166-2-lvl4'),
                        address.get('country'),
                        address.get('country_code')
                    ))

                    # Prepare data for usersv2 table
                    data_to_update_usersv2.append((
                        api_lat,
                        api_lon,
                        display_name,
                        user_id
                    ))

                else:
                    set_loc_geoloc_na(connection, user_id)
                    print(f"No response from API, set loc_geoloc_na for user_id {user_id}.")

                # Increment counter and bulk insert/update every 100 rows
                row_count += 1
                if row_count % 100 == 0:
                    insert_location_data_bulk(connection, data_to_insert_location)
                    update_usersv2_bulk(connection, data_to_update_usersv2)
                    data_to_insert_location = []  # Clear the list after bulk insert
                    data_to_update_usersv2 = []  # Clear the list after bulk update
                    print(f"Processed and inserted/updated {row_count} rows... committing changes.")
                    connection.commit()

            # Final bulk insert/update and commit after processing all rows
            if data_to_insert_location:
                insert_location_data_bulk(connection, data_to_insert_location)
            if data_to_update_usersv2:
                update_usersv2_bulk(connection, data_to_update_usersv2)
            connection.commit()
            print(f"Final commit after processing all rows.")

    finally:
        connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
