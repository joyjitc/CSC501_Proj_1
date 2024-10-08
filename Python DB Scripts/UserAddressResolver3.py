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

# Function to set loc_revgeoloc_na to TRUE when there's no data from the API
def set_loc_revgeoloc_na(conn, user_id):
    with conn.cursor() as cursor:
        query = """
        UPDATE usersv2
        SET loc_geoloc_na = 1
        WHERE Id = %s
        """
        cursor.execute(query, (user_id,))

# Function to insert the API response into usersLocation table
def insert_location_data(conn, user_id, lat, lon, address):
    with conn.cursor() as cursor:
        query = """
        INSERT INTO usersLocation (user_id, lat, lon, suburb, city, county, state, ISO3166_2_lvl4, country, country_code, loc_revgeoloc_na)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
        """
        cursor.execute(query, (
            user_id,
            lat,
            lon,
            address.get('suburb'),
            address.get('city'),
            address.get('county'),
            address.get('state'),
            address.get('ISO3166-2-lvl4'),
            address.get('country'),
            address.get('country_code')
        ))

# Function to make API call to Nominatim OpenStreetMap
def fetch_location_data(lat, lon):
    url = 'https://nominatim.openstreetmap.org/reverse.php'
    params = {
        'lat': lat,
        'lon': lon,
        'zoom': 14,
        'format': 'jsonv2'
    }
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,hi;q=0.8,en-IN;q=0.7,bn;q=0.6,lb;q=0.5,fi;q=0.4',
        'priority': 'u=1, i',
        'referer': 'https://nominatim.openstreetmap.org/ui/reverse.html?lat=31.26389050&lon=-98.54561160&zoom=15',
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
        if data and 'address' in data:
            return data
    return None

# Main function to connect to DB, fetch users, and update the table
def main():
    # Connect to the database
    print("Connecting to the database...")
    connection = pymysql.connect(**db_config)

    try:
        print("Logged into the database.")
        with connection.cursor() as cursor:
            # Fetch user_id, lat, and long from usersv2 where corresponding entry is not in usersLocation
            cursor.execute("""
            SELECT u.Id, u.lat, u.long 
            FROM usersv2 u
            LEFT JOIN usersLocation ul ON u.Id = ul.user_id
            WHERE u.Location IS NOT NULL
              AND u.lat IS NOT NULL
              AND u.long IS NOT NULL
              AND ul.user_id IS NULL
            """)

            rows = cursor.fetchall()

            print(f"Found {len(rows)} rows with non-null lat/long and no existing entry in usersLocation.")

            # Initialize counter
            row_count = 0

            for row in rows:
                user_id = row['Id']
                lat = row['lat']
                lon = row['long']

                # Fetch data from Nominatim API
                location_data = fetch_location_data(lat, lon)

                if location_data:
                    api_lat = location_data.get('lat')
                    api_lon = location_data.get('lon')
                    address = location_data.get('address', {})
                    insert_location_data(connection, user_id, api_lat, api_lon, address)
                    print(f"Inserted location data for user_id {user_id} using API lat/long.")
                else:
                    set_loc_revgeoloc_na(connection, user_id)
                    print(f"No response from API, set loc_revgeoloc_na for user_id {user_id}.")

                # Increment counter and commit every 100 rows
                row_count += 1
                if row_count % 100 == 0:
                    print(f"Processed {row_count} rows... committing changes.")
                    connection.commit()

            # Final commit after processing all rows
            connection.commit()

    finally:
        connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
