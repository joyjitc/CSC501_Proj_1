import mysql.connector
import xml.etree.ElementTree as ET

# MySQL connection settings
username = 'ancalagon'
password = 'Annex9@call'
host = 'ancalagon2.mysql.database.azure.com'
database = 'datasciencese'

# Establish MySQL connection
cnx = mysql.connector.connect(
    user=username,
    password=password,
    host=host,
    database=database
)

# Load XML file
tree = ET.parse('/Users/joyjitchoudhury/Downloads/test/datascience.stackexchange.com/Users.xml')
root = tree.getroot()

# Insert data into MySQL table
cursor = cnx.cursor()

data = []

query = ("INSERT INTO users "
         "(Id, Reputation, CreationDate, DisplayName, LastAccessDate, WebsiteUrl, "
         "Location, AboutMe, Views, UpVotes, DownVotes, AccountId) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

counter = 0
batchSize = 1000

for row in root:
    try:
        # accountId = int(row.get('AccountId'))) if (row.get('AccountId') is not None else None
        temp_row = (int(row.get('Id')),
                    int(row.get('Reputation')),
                    row.get('CreationDate'),
                    row.get('DisplayName'),
                    row.get('LastAccessDate'),
                    row.get('WebsiteUrl'),
                    row.get('Location'),
                    row.get('AboutMe'),
                    int(row.get('Views')),
                    int(row.get('UpVotes')),
                    int(row.get('DownVotes')),
                    (int(row.get('AccountId')) if (row.get('AccountId') is not None) else None))
        data.append(temp_row)
        counter += 1
    except Exception as e:
        print(e)
        print("Skipping row " + str(row.get('Id')))

    if counter % batchSize == 0:
        cursor.executemany(query, data)
        data.clear()
        print(f'{counter} records inserted')
        cnx.commit()

# Insert any remaining data
if data:
    cursor.executemany(query, data)
    print(f'{counter} records inserted')
    cnx.commit()

# Close cursor and connection
cursor.close()
cnx.close()
