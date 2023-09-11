import os
import pyodbc

from typing import List
from datetime import date
from colorama import Fore
from dataclasses import dataclass
from typing import get_type_hints

from ..Shared.Environment import Environment
from ..Shared.Config import Config
from ..Shared.Utils import Utils
from ..Shared.Logger import Logger
from ..Shared.PrettyPrint import Pretty
from .TargetDatabase import TargetDatabase, Relation, Relations
from ..Shared.Decorators import post_execution_output

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
        ORDER BY 
            TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """

    @dataclass
    class ConnectionString:
        normal: str
        masked: str

    def __init__(self):
        self._connection = None
        self._databaseName = None
        return

    @post_execution_output
    def __load_data (self, query, enrichmentFunction, destination, what):
        rows = self.get_connection ().cursor().execute(query).fetchall()
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
        Logger.debug (Pretty.assemble_simple (f"Number of items for {what}: {len (rows)}"))
        return
        
    def __retrieve_type_info (self, row): # ToDo: Búa til dataclass fyrir þetta, á heima í TargetDatabase (sjá ConceptGlossary útfærslu)
        column = {}
        column['type_name'] = row.data_type
        column['max_length'] = row.character_maximum_length
        column['numeric_precision'] = row.numeric_precision
        column['numeric_precision_radix'] = row.numeric_precision_radix
        column['numeric_scale'] = row.numeric_scale
        #APISupport.print_v(f"Column name: {columnName} - Values: {column}")
        return column

    def __get_connection_string (self, databaseNameParam = None) -> ConnectionString: 
        # Environment variables trump config ones.
        databaseServer = Utils.retrieve_variable ('Database server', Environment.databaseServer, Config['database'], 'server')
        databasePort = Utils.retrieve_variable ('Database server port', Environment.databasePort, Config['database'], 'port', True) # Optional
        databaseName = databaseNameParam if databaseNameParam is not None else Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')

        if databasePort is not None and len (databasePort) > 0: # Þetta á ekki við um default port!
            databaseServer += "," + os.environ.get (Environment.databasePort)

        connectionString = Config['database']['connection-string-template']
        connectionString = connectionString.replace('{{database-server}}', databaseServer)
        connectionString = connectionString.replace('{{database-name}}', databaseName)

        # user/pass support - Only from environment variables.
        if Utils.environment_variable_with_value (Environment.databaseUser):
            connectionString = connectionString.replace('{{database-user}}', os.environ.get(Environment.databaseUser))

        loggableConnectionString = connectionString

        if Utils.environment_variable_with_value (Environment.databasePassword):
            parameterizedConnectionString = connectionString # For logging without passwords!
            connectionString = connectionString.replace('{{database-password}}', os.environ.get(Environment.databasePassword))
            loggableConnectionString = parameterizedConnectionString.replace('{{database-password}}', '**********')

        return SQLServer.ConnectionString (connectionString, loggableConnectionString)

    # Public interface
    
    def set_connection (self, databaseName:str):
        self._databaseName = databaseName
        connectionString = self.__get_connection_string (self._databaseName)
        Logger.debug (Pretty.assemble_simple (f"Connection set to: {connectionString.masked}"))
        self._connection = pyodbc.connect (connectionString.normal)
        self._connection.autocommit = True # Þetta á við allar útfærslur, koma betur fyrir!

    def get_connection (self) -> pyodbc.Connection:
        if self._connection is None:
            self.set_connection (Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name'))        
        return self._connection
    
    def get_database_name (self) -> str:
        return self._databaseName

    def get_date (self) -> date:
        return self.get_connection().cursor ().execute ("SELECT CAST (GETDATE() AS date)").fetchval () 

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
            self.__load_data (SQLServer.typeQuery, self.__retrieve_type_info, SQLServer._types, "Datatypes")
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
        results = self.get_connection().cursor ().execute (query, schemaName).fetchall()
        relationDict = {}
        for row in results:
            if row.TABLE_NAME in relationDict.keys():
                relationDict[row.TABLE_NAME].columnNames.append (row.COLUMN_NAME)
            else:
                relation = Relation(row.TABLE_SCHEMA, row.TABLE_NAME, row.TABLE_TYPE == "VIEW", [row.COLUMN_NAME])
                relationDict[relation.name] = relation

        relations = sorted (list(relationDict.values ()), key=lambda x: x.name)
        Logger.debug (Pretty.assemble_simple (f"Retrieved relations for {self._databaseName}.{schemaName} - list: {len (relations)} - dictionary: {len (relationDict.keys())}"))
        return Relations (relations, relationDict)

    def clone_column (self, sourceSchema:str, sourceTable:str, targetDatabase:str, targetSchema:str, targetTable:str, columnName:str) -> None:
        columnInfo = self.get_connection().cursor ().execute ("SELECT c.IS_NULLABLE, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, COALESCE (c.NUMERIC_SCALE, 0) AS NUMERIC_SCALE, c.DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?", sourceSchema, sourceTable, columnName).fetchone()
        Logger.info(Pretty.assemble_simple (f"Adding column: {columnName} to {self._databaseName}.{targetSchema}.{targetTable}"))
        
        alterCommand = f"ALTER TABLE {targetDatabase}.{targetSchema}.{targetTable} ADD {columnName} "
        if columnInfo.DATA_TYPE in ["date", "datetime", "datetime2", "int", "bigint", "tinyint", "smallint", "bit"]:
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
        
        Logger.debug (Pretty.assemble_simple (f"Alter command: {alterCommand}"))
        self.get_connection().cursor ().execute (alterCommand)
        return

    def drop_view (self, schemaName:str, viewName:str) -> None:
        self.get_connection().cursor ().execute (f"DROP VIEW {schemaName}.{viewName}")
        return

    def create_schema_if_missing (self, schemaName:str) -> None:
        erTil = self.get_connection().cursor ().execute ('SELECT COUNT(1) AS er_til FROM sys.schemas s WHERE s.name = ?', schemaName).fetchval ()
        if (erTil == 1):
            Logger.info (Pretty.assemble_simple (f'Schema ({self._databaseName}.{schemaName}) already exists'))
        else:
            Logger.info (Pretty.assemble_simple (f'Schema ({self._databaseName}.{schemaName}) missing, creating it'))
            self.get_connection().cursor ().execute (f'CREATE SCHEMA {schemaName}')
            Logger.debug (Pretty.assemble_simple ('Schema created\n'))
        return
    
    
    def relation_exists (self, schemaName:str, tableName:str) -> bool:
        ret_val = self.get_connection().cursor ().execute ('SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?', schemaName, tableName).fetchval() == 1
        Logger.debug (Pretty.assemble_simple (f"Table {schemaName}.{tableName} found = {ret_val}"))
        return ret_val
    
    def get_database_type (self, pythonType) -> str:
        if pythonType == "str":
            return "nvarchar(4000)"
        if pythonType == "int":
            return "bigint"
        if pythonType == "float":
            return "float"
        if pythonType == "date":
            return "date"
        if pythonType == "datetime":
            return "datetime"
        #log, default type!
        return "nvarchar(2000)"
    
    def create_table_if_missing (self, schemaName:str, tableName:str, data_class) -> None:
        if self.relation_exists (schemaName, tableName) == True:
            Logger.info (Pretty.assemble_simple (f'Table ({self._databaseName}.{schemaName}.{tableName}) already exists'))
            return
        Logger.info (Pretty.assemble_simple (f'Creating table ({self._databaseName}.{schemaName}.{tableName})'))
        
        command = f"CREATE TABLE [{schemaName}].[{tableName}] ("
        for att_name, att_type in get_type_hints (data_class).items():
            sql_type = self.get_database_type (att_type.__name__)
            command += f"\n\t[{att_name}] {sql_type} NULL, "
            
        command = command[:-2] # Remove the last comma
        command += "\n)"
        Logger.debug (Pretty.assemble_simple (f"Creating table: \n{command}"))
        self.get_connection().cursor ().execute (command)
        return    
    
    @staticmethod
    def __create_insert_command_from_dataclass_instance (database:str, schema:str, table:str, data_class) -> str:
        command = f"INSERT INTO [{database}].[{schema}].[{table}]\n"
        columns = ""
        values = ""
        for att_name, att_type in get_type_hints (data_class).items():
            columns += f"[{att_name}], "
            values += "?, "
            
        columns = columns[:-2] # fjarlægjum auka kommur
        values = values[:-2]
    
        command += f" ({columns}) VALUES ({values})"
        return command
    
    @staticmethod
    def __dataclass_to_parameter_list (data_class) -> list:
        params = []
        for att_name, att_type in get_type_hints (data_class).items():
            params.append (getattr (data_class, att_name))
        return params
    
    def insert_dataclass (self, database:str, schema:str, table:str, data_class) -> None:
        command = SQLServer.__create_insert_command_from_dataclass_instance (database, schema, table, data_class)
        params = SQLServer.__dataclass_to_parameter_list (data_class)
        self.get_connection().cursor ().execute (command, params)    
        return
    
    def insert_dataclasses (self, database:str, schema:str, table:str, data_classes:list) -> None:
        if data_classes is None or len (data_classes) == 0:
            return
        
        command = SQLServer.__create_insert_command_from_dataclass_instance (database, schema, table, data_classes[0])
        
        param_list = []
        for dc in data_classes:
            param_list.append (SQLServer.__dataclass_to_parameter_list (dc))
        
        cursor = self.get_connection().cursor ()
        cursor.fast_executemany = True
        cursor.executemany (command, param_list)
        return

    def create_or_alter_view (self, viewSchema:str, viewName:str, sourceDatabase:str, sourceSchema:str, sourceTable:str) -> None:
        Logger.debug (Pretty.assemble_simple (f"Creating view {self._databaseName}.{viewSchema}.{viewName} - Selecting from: {sourceSchema}.{sourceTable}"))
        self.get_connection().cursor ().execute (f"CREATE OR ALTER VIEW [{viewSchema}].[{viewName}] AS SELECT * FROM [{sourceDatabase}].[{sourceSchema}].[{sourceTable}]")
        return

    def create_empty_target_table (self, sourceDatabase:str, sourceSchema:str, sourceTable:str, sourceKeyColumns:List[str], targetSchema:str, targetTable:str, dateColumnName:str) -> None:
        predicate = " IS NULL OR ".join (sourceKeyColumns) + " IS NULL"
        self.get_connection().cursor ().execute (f"SELECT CAST (NULL AS DATE) AS {dateColumnName}, s.* INTO [{targetSchema}].[{targetTable}] FROM [{sourceDatabase}].[{sourceSchema}].[{sourceTable}] s WHERE {predicate}")
        self.get_connection().cursor ().execute (f"CREATE CLUSTERED COLUMNSTORE INDEX {targetSchema.lower()}_{targetTable.lower()}_cci ON [{targetSchema}].[{targetTable}]")
        return

    @post_execution_output
    def delete_data (self, schemaName:str, tableName:str, comparisonColumn:str, columnValue:str) -> None:
        Logger.debug (Pretty.assemble_simple (f"Removing data from {self._databaseName}.{schemaName}.{tableName} for {comparisonColumn} = {columnValue}"))
        deleteCursor = self.get_connection().cursor()
        deleteCursor.execute (f"DELETE [{schemaName}].[{tableName}] WHERE {comparisonColumn} = ?", columnValue)
        Logger.debug (Pretty.assemble_simple (f"{deleteCursor.rowcount} rows deleted"))
        return

    @post_execution_output
    def insert_data (self, sourceDatabase:str, sourceSchema:str, sourceTable:str, sourceColumns:List[str], sourceKeyColumns:List[str], targetSchema:str, targetTable:str, dateColumnName:str, runDate:date) -> None:
        # Command building
        selectColumnList = "[" + "], [".join (sourceColumns) + "]" 
        insertColumnList = f"{dateColumnName}, {selectColumnList}"
        predicate = " IS NOT NULL AND ".join (sourceKeyColumns) + " IS NOT NULL"
        command = f"INSERT INTO [{targetSchema}].[{targetTable}] ({insertColumnList}) SELECT '{runDate}', {selectColumnList} FROM [{sourceDatabase}].[{sourceSchema}].[{sourceTable}] WHERE {predicate}"
        Logger.debug (Pretty.assemble_simple (f"Adding data to {self._databaseName}.{targetSchema}.{targetTable}"))
        Logger.debug (Pretty.assemble (value="Executing:", color=Fore.LIGHTMAGENTA_EX, tabCount=Pretty.Indent))
        Logger.debug (Pretty.assemble_simple (command))
        # Execution
        insertCursor = self.get_connection().cursor ()
        insertCursor.execute (command)
        Logger.debug (Pretty.assemble (value=f"{insertCursor.rowcount} rows inserted", color=Fore.WHITE, tabCount=Pretty.Indent))
        return
    
    @post_execution_output
    def retrieve_cardinality (self, schemaName:str, tableName:str) -> int:
        query = f"SELECT COUNT(1) AS fjoldi FROM [{schemaName}].[{tableName}]"
        rows = self.get_connection().cursor ().execute (query).fetchval ()
        Logger.debug (Pretty.assemble_simple (f"Cardinality for table {self._databaseName}.{schemaName}.{tableName} retrieved - cardinality: {rows}"))
        return rows