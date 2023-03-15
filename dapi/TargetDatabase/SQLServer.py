import os
import pyodbc

from typing import List
from datetime import date
from colorama import Fore

from ..Shared.Environment import Environment
from ..Shared.Config import Config
from ..Shared.Utils import Utils
from ..Shared.Logger import Logger
from ..Shared.PrettyPrint import Pretty
from .TargetDatabase import TargetDatabase, Relation, Relations
from ..Shared.Decorators import execution_time, output_headers

class SQLServer (TargetDatabase):
    typeQuery = """
        SELECT
            TABLE_SCHEMA AS schema_name
            ,TABLE_NAME AS relation_name
            ,COLUMN_NAME AS column_name
            ,DATA_TYPE AS data_type
            ,COALESCE (CHARACTER_MAXIMUM_LENGTH, '') AS character_maximum_length
            ,COALESCE (NUMERIC_PRECISION, '') AS numeric_precision
            ,COALESCE (NUMERIC_PRECISION_RADIX, '') AS numeric_precision_radix
            ,COALESCE (NUMERIC_SCALE, '') AS numeric_scale
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            TABLE_CATALOG = ?
        ORDER BY 
            TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """

    def __init__(self):
        self._databaseServer = Utils.retrieve_variable ('Database server', Environment.databaseServer, Config['database'], 'server')
        self._databaseName = Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')

        if Utils.environment_variable_with_value (Environment.databasePort):
            self._databaseServer += "," + os.environ.get(Environment.databasePort)

        self._connectionString = Config['database']['connection-string-template']
        self._connectionString = self._connectionString.replace('{{database-server}}', self._databaseServer)
        self._connectionString = self._connectionString.replace('{{database-name}}', self._databaseName)

        # user/pass support - Only from environment variables.

        if Utils.environment_variable_with_value (Environment.databaseUser):
            self._connectionString = self._connectionString.replace('{{database-user}}', os.environ.get(Environment.databaseUser))

        self._loggableConnectionString = self._connectionString

        if Utils.environment_variable_with_value (Environment.databasePassword):
            self._connectionString = self._connectionString.replace('{{database-password}}', os.environ.get(Environment.databasePassword))
            self._loggableConnectionString = self._connectionString.replace('{{database-password}}', '**********')

        self._connection = self.get_connection ()
        return

    def get_connection (self) -> pyodbc.Connection:
        Logger.debug (f"\tConnection string: {self._loggableConnectionString}")
        Logger.debug (f"\tCreating a DB connection to: {self._databaseServer} - {self._databaseName}")
        conn = pyodbc.connect (self._connectionString)
        conn.autocommit = True # Þetta á við allar útfærslur, koma betur fyrir!
        return conn

    def get_date (self) -> date:
        return self._connection.cursor ().execute ("SELECT CAST (GETDATE() AS date)").fetchval ()

    @execution_time
    def load_data (self, query, enrichmentFunction, destination, what):
        rows = self._connection.cursor().execute(query, self._databaseName).fetchall()
        for row in rows:
            schemaName = row.schema_name
            tableName = row.relation_name
            columnName = row.column_name
            
            if not schemaName in destination:
                destination[schemaName] = { }
            if not tableName in destination[schemaName]:
                destination[schemaName][tableName] = { }
            info = enrichmentFunction(row)
            destination[schemaName][tableName][columnName] = info
        Logger.debug (f"\t\tNumber of items for {what}: {len (rows)}")
        return
        
    def retrieve_type_info (self, row): # ToDo: Búa til dataclass fyrir þetta, á heima í TargetDatabase (sjá ConceptGlossary útfærslu)
        column = {}
        column['type_name'] = row.data_type
        column['max_length'] = row.character_maximum_length
        column['numeric_precision'] = row.numeric_precision
        column['numeric_precision_radix'] = row.numeric_precision_radix
        column['numeric_scale'] = row.numeric_scale
        #APISupport.print_v(f"Column name: {columnName} - Values: {column}")
        return column

    # Public interface

    def get_type_length(self, columnData) -> str: 
        if columnData['database_info']['type_name'] in ['char', 'nchar', 'varchar' ,'nvarchar']: 
            return columnData['database_info']['max_length']
        if columnData['database_info']['type_name'] in ['int', 'bigint', 'tinyint', 'smallint', 'numeric', 'float', 'decimal']:
            precision = columnData['database_info']['numeric_precision']
            scale = columnData['database_info']['numeric_scale'] 
            return f"{int (precision) - int (scale)}, {scale}"
        return ""

    def get_type_info_column_data (self, schemaName:str, tableName:str, columnName:str) -> dict:
        if not hasattr (SQLServer, "_types"):
            SQLServer._types = {}
            self.load_data (SQLServer.typeQuery, self.retrieve_type_info, SQLServer._types, "Datatypes")
        return SQLServer._types[schemaName][tableName][columnName] 

    def retrieve_relations (self, schemaName:str) -> Relations:
        query = """
        SELECT 
            c.TABLE_SCHEMA, c.TABLE_NAME, t.TABLE_TYPE, c.COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.COLUMNS c 
            JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
        WHERE 
            c.TABLE_SCHEMA = ?
        ORDER BY 
            c.TABLE_NAME, c.ORDINAL_POSITION
        """
        results = self._connection.cursor ().execute (query, schemaName).fetchall()
        relationDict = {}
        for row in results:
            if row.TABLE_NAME in relationDict.keys():
                relationDict[row.TABLE_NAME].columnNames.append (row.COLUMN_NAME)
            else:
                relation = Relation(row.TABLE_SCHEMA, row.TABLE_NAME, row.TABLE_TYPE == "VIEW", [row.COLUMN_NAME])
                relationDict[relation.name] = relation

        relations = sorted (list(relationDict.values ()), key=lambda x: x.name)
        Logger.debug (f"Retrieved relations for schema: {schemaName} - list: {len (relations)} - dictionary: {len (relationDict.keys())}")
        return Relations (relations, relationDict)

    def clone_column (self, sourceSchema:str, sourceTable:str, targetSchema:str, targetTable:str, columnName:str) -> None:
        columnInfo = self._connection.cursor ().execute ("SELECT c.IS_NULLABLE, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, COALESCE (c.NUMERIC_SCALE, 0) AS NUMERIC_SCALE, c.DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?", sourceSchema, sourceTable, columnName).fetchone()
        Logger.info(f"\t\tAdding column: {columnName} to {targetSchema}.{targetTable}")
        
        alterCommand = f"ALTER TABLE {targetSchema}.{targetTable} ADD {columnName} "
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
        
        alterCommand += " NULL" # All new columns must be nullable!
        
        Logger.debug (f"\t\t\tAlter command: {alterCommand}")
        self._connection.cursor ().execute (alterCommand)
        return

    def drop_view (self, schemaName:str, viewName:str) -> None:
        self._connection.cursor ().execute (f"DROP VIEW {schemaName}.{viewName}")
        return

    def create_schema_if_missing (self, schemaName:str) -> None:
        erTil = self._connection.cursor ().execute ('SELECT COUNT(1) AS er_til FROM sys.schemas s WHERE s.name = ?', schemaName).fetchval ()
        if (erTil == 1):
            print (f'Schema ({schemaName}) already exists\n')
        else:
            print (f'Schema ({schemaName}) missing, creating it')
            self._connection.cursor ().execute (f'CREATE SCHEMA {schemaName}')
            print ('Schema created\n')
        return

    def create_or_alter_view (self, viewSchema:str, viewName:str, sourceSchema:str, sourceTable:str) -> None:
        Logger.debug (f"\t\tCreating view {viewSchema}.{viewName} - Selecting from: {sourceSchema}.{sourceTable}")
        self._connection.cursor ().execute (f"CREATE OR ALTER VIEW [{viewSchema}].[{viewName}] AS SELECT * FROM [{sourceSchema}].[{sourceTable}]")
        return

    def create_empty_target_table (self, sourceSchema:str, sourceTable:str, sourceKeyColumns:List[str], targetSchema:str, targetTable:str, dateColumnName:str) -> None:
        predicate = " IS NULL OR ".join (sourceKeyColumns) + " IS NULL"
        self._connection.cursor ().execute (f"SELECT CAST (NULL AS DATE) AS {dateColumnName}, s.* INTO [{targetSchema}].[{targetTable}] FROM [{sourceSchema}].[{sourceTable}] s WHERE {predicate}")
        return

    @output_headers
    @execution_time(tabCount=2)
    def delete_data (self, schemaName:str, tableName:str, comparisonColumn:str, columnValue:str) -> None:
        Logger.info (f"\tRemoving data from {schemaName}.{tableName} for {comparisonColumn} = {columnValue}")
        deleteCursor = self._connection.cursor()
        deleteCursor.execute (f"DELETE [{schemaName}].[{tableName}] WHERE {comparisonColumn} = ?", columnValue)
        Logger.info (f"\t\t{deleteCursor.rowcount} rows deleted")
        return

    @output_headers
    @execution_time(tabCount=2)
    def insert_data (self, sourceSchema:str, sourceTable:str, sourceColumns:List[str], sourceKeyColumns:List[str], targetSchema:str, targetTable:str, dateColumnName:str, runDate:date) -> None:
        # Command building
        selectColumnList = ", ".join (sourceColumns)
        insertColumnList = f"{dateColumnName}, {selectColumnList}"
        predicate = " IS NOT NULL AND ".join (sourceKeyColumns) + " IS NOT NULL"
        command = f"INSERT INTO [{targetSchema}].[{targetTable}] ({insertColumnList}) SELECT '{runDate}', {selectColumnList} FROM [{sourceSchema}].[{sourceTable}] WHERE {predicate}"
        Logger.debug (Pretty.assemble ("Executing: ", False, False, Fore.LIGHTMAGENTA_EX, 0, 2))
        Logger.debug (command)
        # Execution
        insertCursor = self._connection.cursor ()
        insertCursor.execute (command)
        Logger.info (Pretty.assemble (f"{insertCursor.rowcount} rows inserted", False, False, Fore.WHITE, 0 ,3))
        return