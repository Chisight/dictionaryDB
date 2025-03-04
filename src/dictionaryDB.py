'''
dictionaryDB
Features
    ✔️Timestamp Handling: Optional flag to add timestamps to incoming data.
    Field Types: Allow for named parameters to specify field types (e.g., numeric fields saved as strings).
    Improved Error Logging and Returns.
    ✔️Data Retention Policies: Currently, all data is kept indefinitely; future consideration possible.
    ✔️Handle Resetting Data Sources: Functionality to track data from sources that reset (e.g., network counters).
    ✔️Handle Non-Resetting Data Sources: Capability to manage data from analog or temperature loggers.
    ✔️Dynamic Dictionary Handling: Ability to handle changing dictionaries and add fields on the fly.
    ✔️Filename and Table Name Acceptance: Accept a filename, table name, and data, along with optional flags (e.g., cumulative data).
'''

import sqlite3
import traceback
import time
from typing import Callable, Dict, Any, Tuple

__all__ = ['writeDB', 'readDB', 'archiveDB']  # Specify the functions to be exported

# Cache for database connections
db_connection_cache: Dict[str, sqlite3.Connection] = {}
# Cache for table schemas
schema_cache: Dict[str, Dict[str, list]] = {}


def readDB(dbFileName: str, dbTable: str, limit: int = 1, orderBy: str = None, whereClause: str = None, fields: list = None, include_id: bool = False) -> list:
    """
    Read records from dbTable of dbFileName.
    
    Parameters:
        dbFileName: The name of the database file.
        dbTable: The name of the table to read from.
        limit: The maximum number of records to return (default is 1).
        orderBy: Optional ORDER BY clause (e.g., "id DESC").
        fields: Optional list of fields to return; if None, return all fields.
        include_id: Optional boolean to specify if the 'id' field should be included (default is False).
    
    Returns:
        A list of dictionaries representing the records.
    """
    # Check if the database connection is already cached
    conn = get_conn(dbFileName)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Get the schema for the specified table
        if fields is None:
            fields = get_table_schema(dbFileName, dbTable)

        # If include_id is False, remove 'id' from the fields if it exists
        if not include_id and 'id' in fields:
            fields.remove('id')

        fields_str = ', '.join(fields)

        query = f"SELECT {fields_str} FROM {dbTable}"
        
        if orderBy:
            query += f" ORDER BY {orderBy}"

        if whereClause:
            query += f" WHERE {whereClause}"
        
        if limit:
            query += f" LIMIT {limit}"

        # Execute the query
        cursor.execute(query)
        
        # Fetch the results
        records = cursor.fetchall()
        
        # Directly use the Row objects to create a list of dictionaries
        if records:
            result = [dict(record) for record in records]
            return result
        else:
            return None

    except Exception as e:
        print(f"Error occurred while reading data: {e}")
        traceback.print_exc()
        return []

    finally:
        # Optionally close the cursor if needed
        cursor.close()


def writeDB(dbFileName: str, dbTable: str, data: Any, timestamp_field: str = None, cumulative_fields: list = None) -> None:
    """
    Write data as a new record in dbTable of dbFileName.
        cumulative_fields means those values accumulate regardless of resets.
        timestamp_field: Optional field name for storing the timestamp.
        cumulative_fields: List of fields to accumulate; defaults to None.
    """
    # Add the current Unix timestamp to the data dictionary
    if timestamp_field is not None:
        data[timestamp_field] = int(time.time())  # Store timestamp as Unix time

    # Get database connection
    conn = get_conn(dbFileName)
    cursor = conn.cursor()

    try:
        # Initialize the database and create the main table if it doesn't exist
        initialize_database(dbFileName, dbTable, data)

        # Insert the data into the database
        if cumulative_fields is not None:
            # Initialize/create the offsets table
            offsets_table = dbTable + "offsets"
            initialize_database(dbFileName, offsets_table, data)

            # Fetch the last values from the data table (old totals)
            cursor.execute(f"SELECT * FROM {dbTable} WHERE id = (SELECT MAX(id) FROM {dbTable})")
            old_row = cursor.fetchone()
            if old_row is None:
                old_totals = {key: 0 for key in data.keys()}  # Default to 0 for all keys
            else:
                old_totals = dict(old_row)

            # Fetch the last values from the offsets table
            cursor.execute(f"SELECT * FROM {offsets_table} WHERE id = (SELECT MAX(id) FROM {offsets_table})")
            offset_row = cursor.fetchone()
            if offset_row is None:
                offsets = {key: 0 for key in data.keys()}  # Default to 0 for all keys
            else:
                offsets = dict(offset_row)

            # Call the cumulative calculation function
            new_totals, reset_detected, offsets = calculate_cumulative_data(data, offsets, old_totals, 
                                                                           timestamp_field, cumulative_fields)

            # Start a transaction if cumulative is True
            cursor.execute("BEGIN TRANSACTION;")
            if reset_detected:
                # Prepare the SQL for inserting a new row
                columns = ', '.join(offsets.keys())
                placeholders = ', '.join('?' for _ in offsets)

                # Attempt to insert a new row, ignoring if it already exists
                cursor.execute(f"INSERT OR IGNORE INTO {offsets_table} ({columns}) VALUES ({placeholders})", tuple(offsets.values()))

                # Now update the existing row with the new values
                cursor.execute(f"UPDATE {offsets_table} SET " + ', '.join(f"{key} = ?" for key in offsets.keys()), tuple(offsets.values()))

            # Prepare the data to insert
            data.update(new_totals)

        # Insert the main data into the database
        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        values = tuple(data.values())
        
        cursor.execute(f"INSERT INTO {dbTable} ({columns}) VALUES ({placeholders})", values)

        if cumulative_fields is not None:
            cursor.connection.commit()
        else:
            conn.commit()
    except Exception as e:
        if cumulative_fields is not None:
            # Rollback in case of error if cumulative is True
            cursor.connection.rollback()
        print(f"Error occurred while inserting data: {e}")
        traceback.print_exc()

    finally:
        # Optionally close the cursor if needed
        cursor.close()

def calculate_cumulative_data(
    current_data: Dict[str, Any], 
    offsets: Dict[str, Any], 
    old_totals: Dict[str, Any],
    timestamp_field: str = None,
    cumulative_fields: list = None
) -> Tuple[Dict[str, Any], bool, Dict[str, Any]]:
    """
    Calculate cumulative data and detect resets.

    Args:
        current_data (Dict[str, Any]): The current data from the payload.
        offsets (Dict[str, Any]): The current offsets for the interface.
        old_totals (Dict[str, Any]): The last cumulative values for the interface.
        timestamp_field (str): The field name to ignore for reset calculations.
        cumulative_fields (list): List of fields to accumulate; defaults to None (all fields).

    Returns:
        Tuple[Dict[str, Any], bool, Dict[str, Any]]: A tuple containing the updated data,
        a boolean indicating if a reset was detected, and the updated offsets.
    """

    # If cumulative_fields is None, consider all keys
    if cumulative_fields is None:
        cumulative_fields = current_data.keys()

    # Check for potential reset by comparing current values with previous running totals
    reset_detected = False
#    print(f"old_totals:{old_totals}")
#    print(f"cumulative_fields:{cumulative_fields}")
    for key in cumulative_fields:
#        print(f"key:{key}")
        if key in old_totals:
#            print(f"key:{key}  current_data:{current_data[key]} offsets:{offsets[key]} (sum:{current_data[key] + offsets[key]}) old_totals:{old_totals[key]}")
            if current_data[key] + offsets[key] < old_totals[key]:
#                print(f"key:{key}  current_data:{current_data[key]} offsets:{offsets[key]} (sum:{current_data[key] + offsets[key]}) old_totals:{old_totals[key]}")
                reset_detected = True
                break
    
    if reset_detected:
        # If a reset is detected, we treat the current running totals as the new baseline
#        print(f"Detected reset. Updating baseline values.  old_totals:{old_totals}")
        for key in cumulative_fields:
            if key in old_totals:
                offsets[key] = old_totals[key]

    # Calculate new running totals
    new_totals = {}
    for key in cumulative_fields:
        if key != timestamp_field:
            new_totals[key] = current_data[key] + offsets[key]

    return new_totals, reset_detected, offsets


def archiveDB(
    dbFileName: str,
    dbTable: str,
    archiveFileName: str,
    before_date: int = None,
    condition: str = None,
    records_to_keep: int = None,
    custom_logic: Callable[[Dict[str, Any]], bool] = None
) -> None:
    if records_to_keep is None:
        return  # Exit if no records_to_keep specified

    # Connect to the main and archive databases
    conn_main = get_conn(dbFileName)
    conn_archive = get_conn(archiveFileName)
    
    cursor_main = conn_main.cursor()
    cursor_archive = conn_archive.cursor()

    # Start a transaction
    try:
        conn_main.execute("BEGIN TRANSACTION;")
        conn_archive.execute("BEGIN TRANSACTION;")

        # Get count of records in the main database
        cursor_main.execute(f'SELECT COUNT(*) FROM {dbTable}')
        record_count = cursor_main.fetchone()[0]

        # If record count exceeds the specified limit, move excess records to archive
        if record_count > records_to_keep:
            print(f"Archiving old records: Moving {record_count - records_to_keep} records to archive. Total records before: {record_count}")

            # Select records to archive in ID order
            cursor_main.execute(f'SELECT * FROM {dbTable} ORDER BY id ASC LIMIT ?', (record_count - records_to_keep,))
            records = cursor_main.fetchall()
            old_records = [dict(record) for record in records]

            # Fetch column names from the cursor description
            column_names = [description[0] for description in cursor_main.description]

            # Create a schema dictionary based on the column names
            schema = {column_name: None for column_name in column_names}  # You can specify actual types if known

            # Initialize the database and create the table if it doesn't exist
            initialize_database(archiveFileName, dbTable, schema)  # Ensure archive table is also initialized

            for record in old_records:
                placeholders = ', '.join(['?'] * len(column_names[1:]))  # Exclude 'id' column
                cursor_archive.execute(f'INSERT INTO {dbTable} ({", ".join(column_names[1:])}) VALUES ({placeholders})', record[1:])
                cursor_main.execute(f'DELETE FROM {dbTable} WHERE id = ?', (record[0],))

            # Commit the changes to both databases
            conn_main.commit()
            conn_archive.commit()
        else:
            print("No records to archive.")

    except Exception as e:
        # Rollback in case of error
        conn_main.rollback()
        conn_archive.rollback()
        print(f"Error processing old records: {e}")

    finally:
        # Close the cursors
        cursor_main.close()
        cursor_archive.close()

def initialize_database(dbFileName: str, dbTable: str, data: Dict[str, Any]) -> None:
    """Initialize the database and create a table if it doesn't exist, updating the schema based on the provided data."""
    conn = get_conn(dbFileName)
    cursor = conn.cursor()
    
    # Create a table with an auto-incrementing ID if it doesn't exist
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {dbTable} (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        )
    ''')

    # Update the schema based on the incoming data
    update_schema(dbFileName, dbTable, data)

def get_conn(dbFileName: str) -> sqlite3.Connection:
    """Get cached connection from databese filename, opens db if needed."""
    conn = db_connection_cache.get(dbFileName)
    if conn is None:
        conn = sqlite3.connect(dbFileName)
        conn.row_factory = sqlite3.Row
        db_connection_cache[dbFileName] = conn
    return conn

def get_table_schema(dbFileName: str, dbTable: str) -> list:
    """
    Get the schema of the specified table from the database.
    Caches the schema for future use.
    
    Parameters:
        dbFileName: The name of the database file.
        dbTable: The name of the table to get the schema for.
    
    Returns:
        A list of column names for the specified table.
    """
    cache_key = (dbFileName, dbTable)
    
    # Check if the schema is already cached
    if cache_key in schema_cache:
        return schema_cache[cache_key]

    # If not cached, retrieve the schema from the database
    conn = get_conn(dbFileName)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({dbTable})")
    schema = [row[1] for row in cursor.fetchall()]  # Get column names

    # Cache the schema
    schema_cache[cache_key] = schema
    cursor.close()
    return schema

def update_schema(dbFileName: str, dbTable: str, data: Dict[str, Any]) -> None:
    """Update the database schema based on the provided data."""
    conn = get_conn(dbFileName)
    cursor = conn.cursor()
    # Get the schema for the specified table
    columns = get_table_schema(dbFileName, dbTable)

    for key, value in data.items():
        if key not in columns:
            if isinstance(value, int):
                column_type = 'INTEGER'
            elif isinstance(value, float):
                column_type = 'REAL'
            else:
                column_type = 'TEXT'
            cursor.execute(f"ALTER TABLE {dbTable} ADD COLUMN {key} {column_type} DEFAULT NULL")
            #invalidate cached schema
            cache_key = (dbFileName, dbTable)
            #print(f"cache_key: {cache_key}")
            if cache_key in schema_cache:
                schema_cache.pop(cache_key)
    cursor.close()
    # TODO: Account for user data type overrides


