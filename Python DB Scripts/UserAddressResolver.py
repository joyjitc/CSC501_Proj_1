import requests

ur = "https://nominatim.openstreetmap.org/search.php?q=victoria%20bc&polygon_geojson=0&format=jsonv2"

import httpx

# Define the base URL
base_url = 'https://nominatim.openstreetmap.org/search.php'

# Define the path variables
path_variables = {}

# Define the query parameters (optional)
params = {
    'q': 'victoria bc',
    'polygon_geo': 0,
    'format': 'jsonv2'
}

# Perform the GET request using path variables
url = base_url.format(**path_variables)

# Make the request with query parameters
response = httpx.get(url, params = params)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    print(data)
    data = data[0]
    print(data)
    # Fetch a specific field from the JSON response
    # Replace 'field_name' with the actual key you want to retrieve
    field_value = data.get('display_name')

    if field_value:
        print(f"Field value: {field_value}")
    else:
        print("Field not found in the response.")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
