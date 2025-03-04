Python Library for persistent data
Sqlite3 backend
Minimal knowledge of SQL required.


def readDB(dbFileName: str, 
    dbTable: str,
    limit: int = 1,
    orderBy: str = None,
    whereClause: str = None,
    fields: list = None,
    include_id: bool = False
) -> list:
    Read records from dbTable of dbFileName.
    
    Parameters:
        dbFileName: The name of the database file.
        dbTable: The name of the table to read from.
        limit: The maximum number of records to return (default is 1).
        orderBy: Optional ORDER BY clause (e.g., "id DESC").
        whereClause: Optional WHERE clause (e.g., "FIELDNAME = 'value'").
        fields: Optional list of fields to return; if None, return all fields.
        include_id: Optional boolean to specify if the 'id' field should be included (default is False).
    
    Returns:
        A list of dictionaries representing the records.

def writeDB(dbFileName: str,
    dbTable: str,
    data: Any,
    timestamp_field: str = None,
    cumulative_fields: list = None
) -> None:
    Write data as a new record in dbTable of dbFileName.

    Parameters:
        dbFileName: The name of the database file.
        dbTable: The name of the table to read from.
        data: list of dictionaries representing the records.
        timestamp_field: Optional field name for storing the timestamp.
        cumulative_fields: List of fields to accumulate regardless of resets; defaults to None.

def archiveDB(
    dbFileName: str,
    dbTable: str,
    archiveFileName: str,
    before_date: int = None,
    condition: str = None,
    records_to_keep: int = None,
    custom_logic: Callable[[Dict[str, Any]], bool] = None
) -> None:
