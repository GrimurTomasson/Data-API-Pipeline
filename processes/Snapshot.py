# pip install pyodbc
from logging import NullHandler
from tkinter import FIRST

import Decorators
import APISupport

class Snapshot:
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

    def __init__ (self):
        APISupport.initialize ()
        self._databaseServer = APISupport.config["database"]["server"]
        self._databaseName = APISupport.config["database"]["name"]
        self._snapshotDateColumnName = APISupport.config["history"]["snapshot-date-column"]

        APISupport.print_v (f"Database server: {self._databaseServer} - Database name: {self._databaseName} - Snapshot date colum name: {self._snapshotDateColumnName}")
        self._databaseConnection = APISupport.get_database_connection (self._databaseServer, self._databaseName)
        return

    def __get_all_source_tables (self, source_schema):
        """Retrieves all relations in a schema"""
        return self._databaseConnection.cursor ().execute ("SELECT t.TABLE_NAME AS Name FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = ? ORDER BY t.TABLE_NAME", source_schema).fetchall ()

    def __get_first_column (self, source_schema, source_table):
        """Retrieves the name of the first column in a relation"""
        firstColumn = self._databaseConnection.cursor ().execute("SELECT c.COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.ORDINAL_POSITION = 1", source_schema, source_table).fetchval ()
        APISupport.print_v (f"First column for {source_schema}.{source_table} is {firstColumn}")
        return firstColumn

    def __get_ordered_column_list (self, source_schema, source_table):
        """Retrieves an ordered list of the columns of a relation"""
        return self._databaseConnection.cursor ().execute(Snapshot.columnListQuery, source_schema, source_table).fetchval ()

    def __create_schema_if_missing (self, target_schema):
        """Checks if a database schema exists, creates it if it does not"""
        erTil = self._databaseConnection.cursor ().execute ('SELECT COUNT(1) AS er_til FROM sys.schemas s WHERE s.name = ?', target_schema).fetchval ()
        if (erTil == 1):
            print (f'Target schema ({target_schema}) already exists')
        else:
            print (f'Target schema ({target_schema}) missing, creating it')
            self._databaseConnection.cursor ().execute (f'CREATE SCHEMA {target_schema}')
            print ('Target schema created')

    def __create_missing_tables (self, source_schema, snapshot_schema):
        """Creates empty snapshot tables if they are missing"""
        missingTables = self._databaseConnection.cursor ().execute (Snapshot.missingTablesQuery, source_schema, snapshot_schema).fetchall ()
        numberMissing = len (missingTables)
        print (f"Number of missing snapshot tables: {numberMissing}")
        if (numberMissing == 0):
            return
        for row in missingTables:
            print (f"\tCreating missing snapshot table: {row.TABLE_NAME}", end="")
            # Our API tables always contain a first column, single column unique id
            firstColumn = self.get_first_column (source_schema, row.TABLE_NAME)
            self._databaseConnection.cursor ().execute (f"SELECT CAST (NULL AS DATE) AS {self._snapshotDateColumnName}, s.* INTO {snapshot_schema}.{row.TABLE_NAME} FROM {source_schema}.{row.TABLE_NAME} s WHERE s.{firstColumn} IS NULL")
            print (" - Done")

    def __create_or_alter_view (self, history_schema, view_name, snapshot_schema, table_name):
        """Creates or updates a view for a single snapshot"""
        self._databaseConnection.cursor ().execute (f"CREATE OR ALTER VIEW {history_schema}.{view_name} AS SELECT * FROM {snapshot_schema}.{table_name}")

    def __create_missing_views (self, snapshot_schema, history_schema):
        """Creates missing views for snapshot tables"""
        APISupport.print_v (f"create_missing_views -> snapshot_schema: {snapshot_schema}, history_schema: {history_schema}")
        self.__create_schema_if_missing (history_schema)
        print ("Creating initial views. Those need to be updated by hand when new versions are released!")
        allTables = self._databaseConnection.cursor ().execute (Snapshot.tablesAndViewsQuery, snapshot_schema).fetchall ()
        print (f"Checking for views for {len (allTables)} snapshot tables in {snapshot_schema}")
        APISupport.print_v (allTables)
        for row in allTables:
            APISupport.print_v (f"\t\ttable: {row.table_name}, view: {row.view_name}, schema: {snapshot_schema}")
            print (f"\tChecking for table {row.table_name}", end="")        
            if self._databaseConnection.cursor ().execute("SELECT COUNT(1) FROM INFORMATION_SCHEMA.VIEWS v WHERE v.TABLE_SCHEMA = ? AND v.TABLE_NAME = ?", history_schema, row.view_name).fetchval () == 0:
                print (f" - Creating view {row.view_name}", end="")
                self.__create_or_alter_view (history_schema, row.view_name, snapshot_schema, row.table_name)
                print (" - Done")
            else:
                print (f" - View existed")
        return

    def __add_missing_column (self, source_schema, table_name, column_name, snapshot_schema):
        """Adds missing columns to a snapshot table"""
        columnInfo = self._databaseConnection.cursor ().execute ("SELECT c.IS_NULLABLE, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, COALESCE (c.NUMERIC_SCALE, 0) AS NUMERIC_SCALE, c.DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?", source_schema, table_name, column_name).fetchone()
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
        APISupport.print_v (f"\t\t\tAlter command: {alterCommand}")
        self._databaseConnection.cursor ().execute (alterCommand)
        # We do not update views when snapshot tables are extended, they might be unions of multiple versions!

    def __add_missing_columns (self, source_schema, snapshot_schema):
        sourceTables = self.__get_all_source_tables (source_schema)
        print (f"Checking for missing columns in {len (sourceTables)} source tables")
        for sourceTable in sourceTables:
            missingColumns = self._databaseConnection.cursor ().execute(Snapshot.missingColumnsQuery, source_schema, sourceTable.Name, snapshot_schema, sourceTable.Name, source_schema).fetchall ()
            print (f"\t{len(missingColumns)} missing columns in {snapshot_schema}.{sourceTable.Name}")
            for missingColumn in missingColumns:
                self.__add_missing_column (source_schema, sourceTable.Name, missingColumn.COLUMN_NAME, snapshot_schema)

    @Decorators.execution_time(tabCount=1)
    def __remove_data_current_date (self, snapshot_schema, table_name, target_date):
        print (f"\tRemoving data from {snapshot_schema}.{table_name} for {self._snapshotDateColumnName} = {target_date}")
        deleteCursor = self._databaseConnection.cursor ()
        deleteCursor.execute (f"DELETE {snapshot_schema}.{table_name} WHERE {self._snapshotDateColumnName} = ?", target_date)
        print (f"\t\t{deleteCursor.rowcount} rows deleted")

    @Decorators.output_headers
    @Decorators.execution_time(tabCount=1)
    def __create_snapshot (self, source_schema, table_name, snapshot_schema, target_date):
        print (f"\tTaking a snapshot of {source_schema}.{table_name} and adding it to {snapshot_schema}.{table_name} for {self._snapshotDateColumnName} = {target_date}")
        firstColumn = self.__get_first_column (source_schema, table_name)
        columnList = self.__get_ordered_column_list (source_schema, table_name)
        insertColumnList = f"{self._snapshotDateColumnName}, {columnList}"
        command = f"INSERT INTO {snapshot_schema}.{table_name} ({insertColumnList}) SELECT '{target_date}', {columnList} FROM {source_schema}.{table_name} WHERE {firstColumn} IS NOT NULL"
        APISupport.print_v (f"\t\tExecuting: {command}")
        insertCursor = self._databaseConnection.cursor ()
        insertCursor.execute (command)
        print (f"\t\t{insertCursor.rowcount} rows inserted")

    @Decorators.output_headers
    @Decorators.execution_time
    def __create_snapshots (self, source_schema, snapshot_schema, history_schema) -> None:
        # Testing the connection
        targetDate = self._databaseConnection.cursor ().execute("SELECT CAST (GETDATE() AS date)").fetchval () # Ensures we always remove and add the same date, even if we cross midnight, also a great connection test!
        print (f"Snapshot date - {self._snapshotDateColumnName}: {targetDate}")
        # Create the snapshot schema if it does not exist
        print (APISupport.separator)
        self.__create_schema_if_missing (snapshot_schema)
        print (APISupport.separator)
        self.__create_missing_tables (source_schema, snapshot_schema)
        print (APISupport.separator)
        self.__add_missing_columns (source_schema, snapshot_schema)
        print (APISupport.separator)
        self.__create_missing_views (snapshot_schema, history_schema)
        for sourceTable in self.__get_all_source_tables (source_schema):
            print (APISupport.separator)
            self.__remove_data_current_date (snapshot_schema, sourceTable.Name, targetDate)
            self.__create_snapshot (source_schema, sourceTable.Name, snapshot_schema, targetDate)
        return

    @Decorators.output_headers
    @Decorators.execution_time
    def create (self) -> None:
        """Taking snapshots for the Latest models"""
        for item in APISupport.config["history"]["projects"]:
            sourceSchema = item["project"]["source-schema"]
            snapshotSchema = item["project"]["snapshot-schema"]
            publicSchema = item["project"]["public-schema"]
            APISupport.print_v (f"sourceSchema: {sourceSchema} - snapshotSchema: {snapshotSchema} - publicSchema: {publicSchema}")
            self.__create_snapshots (sourceSchema, snapshotSchema, publicSchema)
        return

def main ():
    return Snapshot ().create ()

if __name__ == '__main__':
    main ()