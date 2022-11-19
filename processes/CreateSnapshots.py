# pip install pyodbc
from logging import NullHandler
from tkinter import FIRST

import APISupport

missingTablesQuery = """SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = ?
                        EXCEPT
                        SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = ?"""

missingColumnsQuery = """SELECT i.COLUMN_NAME FROM (
                            SELECT c.TABLE_NAME, c.COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
                            EXCEPT
                            SELECT c.TABLE_NAME, c.COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
                        ) i
                        JOIN INFORMATION_SCHEMA.COLUMNS c ON c.TABLE_SCHEMA = ? AND c.TABLE_NAME = i.TABLE_NAME AND c.COLUMN_NAME = i.COLUMN_NAME
                        ORDER BY c.ORDINAL_POSITION"""

tablesAndViewsQuery = """   SELECT i.table_name, SUBSTRING (i.table_name, 0, 1 + LEN(i.table_name) - i.last_u) AS view_name
                            FROM (
                                SELECT i.table_name, i.name_r, CHARINDEX ('_', i.name_r, 0) AS last_u
                                FROM (
                                    SELECT t.table_name, REVERSE (t.table_name) AS name_r FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE'
                                ) i
                            ) i
                            ORDER BY i.TABLE_NAME DESC"""
            
columnListQuery = """   SELECT STRING_AGG (c.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY column_name) AS column_list 
                        FROM INFORMATION_SCHEMA.COLUMNS c 
                        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?"""

def get_all_source_tables(conn, source_schema):
    return conn.cursor().execute("SELECT t.TABLE_NAME AS Name FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = ? ORDER BY t.TABLE_NAME", source_schema).fetchall()

def get_first_column(conn, source_schema, source_table):
    firstColumn = conn.cursor().execute("SELECT c.COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.ORDINAL_POSITION = 1", source_schema, source_table).fetchval()
    APISupport.print_v(f"First column for {source_schema}.{source_table} is {firstColumn}")
    return firstColumn

def get_ordered_column_list(conn, source_schema, source_table):
    return conn.cursor().execute(columnListQuery, source_schema, source_table).fetchval()

def create_missing_schema(conn, snapshot_schema):
    erTil = conn.cursor().execute('SELECT COUNT(1) AS er_til FROM sys.schemas s WHERE s.name = ?', snapshot_schema).fetchval()
    if ( erTil == 1):
        print(f'Target schema ({snapshot_schema}) already exists')
    else:
        print(f'Snapshot schema ({snapshot_schema}) missing, creating it')
        conn.cursor().execute(f'CREATE SCHEMA {snapshot_schema}')
        print('Snapshot schema created')

def create_missing_tables(conn, source_schema, snapshot_schema, snapshotDateColumnName):
    missingTables = conn.cursor().execute(missingTablesQuery, source_schema, snapshot_schema).fetchall()
    numberMissing = len(missingTables)
    print(f"Number of missing snapshot tables: {numberMissing}")
    if (numberMissing == 0):
        return
    for row in missingTables:
        print(f"\tCreating missing snapshot table: {row.TABLE_NAME}", end="")
        # Our API tables always contain a first column, single column unique id
        firstColumn = get_first_column(conn, source_schema, row.TABLE_NAME)
        conn.cursor().execute(f"SELECT CAST (NULL AS DATE) AS {snapshotDateColumnName}, s.* INTO {snapshot_schema}.{row.TABLE_NAME} FROM {source_schema}.{row.TABLE_NAME} s WHERE s.{firstColumn} IS NULL")
        print(" - Done")

def create_or_alter_view(conn, history_schema, view_name, snapshot_schema, table_name):
    #CREATE OR ALTER VIEW Private_Snapshot.postnumer_midja AS SELECT * FROM Private_Snapshot.postnumer_midja_v1
    conn.cursor().execute(f"CREATE OR ALTER VIEW {history_schema}.{view_name} AS SELECT * FROM {snapshot_schema}.{table_name}")

def create_missing_views(conn, snapshot_schema, history_schema):
    APISupport.print_v(f"create_missing_views -> snapshot_schema: {snapshot_schema}, history_schema: {history_schema}")
    print("Creating initial views. Those need to be updated by hand when new versions are released!")
    allTables = conn.cursor().execute(tablesAndViewsQuery, snapshot_schema).fetchall()
    print(f"Checking for views for {len(allTables)} snapshot tables in {snapshot_schema}")
    APISupport.print_v(allTables)
    for row in allTables:
        APISupport.print_v(f"\t\ttable: {row.table_name}, view: {row.view_name}, schema: {snapshot_schema}")
        print(f"\tChecking for table {row.table_name}", end="")        
        if conn.cursor().execute("SELECT COUNT(1) FROM INFORMATION_SCHEMA.VIEWS v WHERE v.TABLE_SCHEMA = ? AND v.TABLE_NAME = ?", history_schema, row.view_name).fetchval() == 0:
            print(f" - Creating view {row.view_name}", end="")
            create_or_alter_view(conn, history_schema, row.view_name, snapshot_schema, row.table_name)
            print(" - Done")
        else:
            print(f" - View existed")
    return

def add_missing_column(conn, source_schema, table_name, column_name, snapshot_schema):
    columnInfo = conn.cursor().execute("SELECT c.IS_NULLABLE, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, COALESCE (c.NUMERIC_SCALE, 0) AS NUMERIC_SCALE, c.DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?", source_schema, table_name, column_name).fetchone()
    print(f"\t\tAdding column: {column_name} to {snapshot_schema}.{table_name}")
    #
    alterCommand = f"ALTER TABLE {snapshot_schema}.{table_name} ADD {column_name} "
    if columnInfo.DATA_TYPE in ["date", "datetime", "int", "bigint", "tinyint", "smallint", "bit"]:
        alterCommand += str(columnInfo.DATA_TYPE)
    elif columnInfo.DATA_TYPE in ["char", "varchar", "nvarchar"]:
        alterCommand += f"{columnInfo.DATA_TYPE}({columnInfo.CHARACTER_MAXIMUM_LENGTH})"
    elif columnInfo.DATA_TYPE in ["decimal", "numeric"]:
        alterCommand += f"{columnInfo.DATA_TYPE}({columnInfo.NUMERIC_PRECISION}, {columnInfo.NUMERIC_SCALE})"
    elif columnInfo.DATA_TYPE == "float":
        alterCommand += f"{columnInfo.DATA_TYPE}({columnInfo.NUMERIC_PRECISION})"
    else: 
        raise Exception(f"\t\tUnsupported type {columnInfo.DATA_TYPE}! Add this column manually.")
    #
    if columnInfo.IS_NULLABLE == 'NO':
        alterCommand += " NOT NULL"
    #
    APISupport.print_v(f"\t\t\tAlter command: {alterCommand}")
    conn.cursor().execute(alterCommand)
    # We do not update views when snapshot tables are extended, they might be unions of multiple versions!

def add_missing_columns(conn, source_schema, snapshot_schema):
    sourceTables = get_all_source_tables(conn, source_schema)
    print(f"Checking for missing columns in {len(sourceTables)} source tables")
    for sourceTable in sourceTables:
        missingColumns = conn.cursor().execute(missingColumnsQuery, source_schema, sourceTable.Name, snapshot_schema, sourceTable.Name, source_schema).fetchall()
        print(f"\t{len(missingColumns)} missing columns in {snapshot_schema}.{sourceTable.Name}")
        for missingColumn in missingColumns:
            add_missing_column(conn, source_schema, sourceTable.Name, missingColumn.COLUMN_NAME, snapshot_schema)

def remove_data_current_date(conn, snapshot_schema, table_name, target_date, snapshotDateColumnName):
    startTime = APISupport.get_current_time()
    print(f"\tRemoving data from {snapshot_schema}.{table_name} for {snapshotDateColumnName} = {target_date}")
    deleteCursor = conn.cursor()
    deleteCursor.execute(f"DELETE {snapshot_schema}.{table_name} WHERE {snapshotDateColumnName} = ?", target_date)
    print(f"\t\t{deleteCursor.rowcount} rows deleted in {APISupport.get_execution_time_in_seconds(startTime)} seconds.")

def create_snapshot(conn, source_schema, table_name, snapshot_schema, target_date, snapshotDateColumnName):
    startTime = APISupport.get_current_time()
    print(f"\tTaking a snapshot of {source_schema}.{table_name} and adding it to {snapshot_schema}.{table_name} for {snapshotDateColumnName} = {target_date}")
    firstColumn = get_first_column(conn, source_schema, table_name)
    columnList = get_ordered_column_list(conn, source_schema, table_name)
    insertColumnList = f"{snapshotDateColumnName}, {columnList}"
    command = f"INSERT INTO {snapshot_schema}.{table_name} ({insertColumnList}) SELECT '{target_date}', {columnList} FROM {source_schema}.{table_name} WHERE {firstColumn} IS NOT NULL"
    APISupport.print_v(f"\t\tExecuting: {command}")
    insertCursor = conn.cursor()
    insertCursor.execute(command)
    print(f"\t\t{insertCursor.rowcount} rows inserted in {APISupport.get_execution_time_in_seconds(startTime)} seconds")

def create_snapshots(database_server, database_name, source_schema, snapshot_schema, history_schema, snapshotDateColumnName ):
    startTime = APISupport.get_current_time()
    print(f"\n{APISupport.separator}")
    conn = APISupport.get_database_connection(database_server, database_name)
    # Testing the connection
    targetDate = conn.cursor().execute("SELECT CAST (GETDATE() AS date)").fetchval() # Ensures we always remove and add the same date, even if we cross midnight, also a great connection test!
    print(f"Snapshot date - {snapshotDateColumnName}: {targetDate}")
    # Create the snapshot schema if it does not exist
    print(APISupport.separator)
    create_missing_schema(conn, snapshot_schema)
    print(APISupport.separator)
    create_missing_tables(conn, source_schema, snapshot_schema, snapshotDateColumnName)
    print(APISupport.separator)
    add_missing_columns(conn, source_schema, snapshot_schema)
    print(APISupport.separator)
    create_missing_views(conn, snapshot_schema, history_schema)
    for sourceTable in get_all_source_tables(conn, source_schema):
        print(APISupport.separator)
        remove_data_current_date(conn, snapshot_schema, sourceTable.Name, targetDate, snapshotDateColumnName)
        create_snapshot(conn, source_schema, sourceTable.Name, snapshot_schema, targetDate, snapshotDateColumnName)
    print(APISupport.separator)
    print(f"Total execution time for source schema {source_schema}: {APISupport.get_execution_time_in_seconds(startTime)} seconds\n")
    return 0

def run():
    startingTime = APISupport.get_current_time()
    databaseServer = APISupport.config["database"]["server"]
    databaseName = APISupport.config["database"]["name"]
    snapshotDateColumnName = APISupport.config["history"]["snapshot-date-column"]
    APISupport.print_v(f"Database server: {databaseServer} - Database name: {databaseName} - Snapshot date colum name: {snapshotDateColumnName}")
    for item in APISupport.config["history"]["projects"]:
        sourceSchema = item["project"]["source-schema"]
        snapshotSchema = item["project"]["snapshot-schema"]
        publicSchema = item["project"]["public-schema"]
        APISupport.print_v(f"sourceSchema: {sourceSchema} - snapshotSchema: {snapshotSchema} - publicSchema: {publicSchema}")
        create_snapshots(databaseServer, databaseName, sourceSchema, snapshotSchema, publicSchema, snapshotDateColumnName)
    print(f"\nTotal execution time for all snapshots: {APISupport.get_execution_time_in_seconds(startingTime)}")
    return

def main():
    return run()

if __name__ == '__main__':
    main()