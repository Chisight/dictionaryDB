import os
import sqlite3
from dictionaryDB import *

# Define the test database file name and table name
test_db_file = "writeDBBasicTest.db"
test_table_name = "data"

# Delete the existing test database file if it exists
if os.path.exists(test_db_file):
    os.remove(test_db_file)

# Prepare test data
test_data_1 = {
    "temperature": 22.5,  # Float
    "humidity": 60,       # Integer
    "timestamp": 1739936040,  # String (timestamp)
}

test_data_2 = {
    "temperature": 23.0,
    "humidity": 65,
    "timestamp": 1739936040,
}

test_data_3 = {
    "steps": 1500,        # New field (Integer)
    "calories": 200.0,   # Float
    "timestamp": 1739936040,
}

# Call writeDB multiple times with different data
writeDB(test_db_file, test_table_name, test_data_1)
writeDB(test_db_file, test_table_name, test_data_2)
writeDB(test_db_file, test_table_name, test_data_3)  # This introduces a new field

print("Test data written to the database successfully.")

# Function to read all records from the database
def read_all_records(db_file: str, table_name: str):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

# Function to check the structure of the table
def check_table_structure(db_file: str, table_name: str):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    structure = cursor.fetchall()
    cursor.close()
    conn.close()
    return structure

# Read all records and check them
records = read_all_records(test_db_file, test_table_name)
expected_records = [
    (1, 22.5, 60, 1739936040, None, None),  # First record with temperature and humidity
    (2, 23.0, 65, 1739936040, None, None),  # Second record with temperature and humidity
    (3, None, None, 1739936040, 1500, 200.0),  # Third record with steps and calories
]

# Check if the records match expectations
if records == expected_records:
    print("Records match expected values.")
else:
    print("Records do not match expected values.")
    print("Expected:", expected_records)
    print("Actual  :", records)

# Check the structure of the table
structure = check_table_structure(test_db_file, test_table_name)
expected_structure = [
    (0, 'id', 'INTEGER', 0, None, 1), 
    (1, 'temperature', 'REAL', 0, 'NULL', 0),  # Updated to expect 'NULL' as a string
    (2, 'humidity', 'INTEGER', 0, 'NULL', 0),   # Updated to expect 'NULL' as a string
    (3, 'timestamp', 'INTEGER', 0, 'NULL', 0),   # Updated to expect 'NULL' as a string
    (4, 'steps', 'INTEGER', 0, 'NULL', 0),       # Updated to expect 'NULL' as a string
    (5, 'calories', 'REAL', 0, 'NULL', 0),       # Updated to expect 'NULL' as a string
]

# Check if the structure matches expectations
if structure == expected_structure:
    print("Table structure matches expected values.")
else:
    print("Table structure does not match expected values.")
    print("Expected:", expected_structure)
    print("Actual  :", structure)

