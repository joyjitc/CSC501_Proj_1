import mysql.connector
import xml.etree.ElementTree as ET

# MySQL connection settings
username = 'ancalagon'
password = ''
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
tree = ET.parse('/Users/joyjitchoudhury/Downloads/test/datascience.stackexchange.com/Badges.xml')
root = tree.getroot()

# Insert data into MySQL table
cursor = cnx.cursor()

data = []

query = ("INSERT INTO badges "
             "(Id, UserId, Name, Date, Class, TagBased) "
             "VALUES (%s, %s, %s, %s, %s, %s)")

counter = 0
batchSize = 1000


for row in root:
    temp_row = (
        row.get('Id'),
        row.get('UserId'),
        row.get('Name'),
        row.get('Date'),
        row.get('Class'),
        row.get('TagBased') == 'True'
    )
    data.append(temp_row)
    counter += 1

    if counter % batchSize == 0:
        cursor.executemany(query, data)
        data.clear()
        print(counter)
        cnx.commit()


cursor.executemany(query, data)
data.clear()
print(counter)

cnx.commit()
cursor.close()
cnx.close()