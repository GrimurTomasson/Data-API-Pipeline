import pyodbc

from Shared.Config import Config
from Shared.Utils import Utils
from TargetDatabase.TargetDatabase import TargetDatabase

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
    glossaryQuery = """
        SELECT
            TABLE_SCHEMA AS schema_name
            ,TABLE_NAME AS relation_name
            ,COLUMN_NAME AS column_name
            ,COALESCE (bg.heiti, '') AS concept_name
            ,COALESCE (bg.lysing, '') AS description
            ,LOWER (COALESCE (dd.typu_id, '')) AS data_type
            ,COALESCE (dd.hamarkslengd, -1) AS max_length
        FROM
            INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN [RVK-DATA-MASTER-API].Nustada.hugtak_utfaersla_V1 dd ON dd.gagnaform_id = 'SQL_SERVER' AND dd.heiti = c.COLUMN_NAME
            LEFT JOIN [RVK-DATA-MASTER-API].Nustada.hugtak_V1 bg ON bg.id = dd.hugtak_id
        WHERE
            c.TABLE_CATALOG = ?
        ORDER BY 
            c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
    """

    def __init__(self):
        self._config = Config()
        self._utils = Utils()
        self._databaseServer = self._config['database']['server']
        self._databaseName = self._config['database']['name']
        self._types = {}
        self._glossary = {}

    def get_connection (self) -> pyodbc.Connection:
        self._utils.print_v (f"Creating a DB connection to: {self._databaseServer} - {self._databaseName}")
        conn = pyodbc.connect ('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self._databaseServer+';DATABASE='+self._databaseName+';Trusted_Connection=yes;')
        conn.autocommit = True # Þetta á við allar útfærslur, koma betur fyrir!
        return conn

    def load_data (self, query, enrichmentFunction, destination):
        conn = self.get_connection ()
        rows = conn.cursor().execute(query, self._databaseName).fetchall()
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
        #APISupport.print_v(f"Allt: {destination}")
        return
        
    def retrieve_glossary_info (self, row):
        column = {}
        column['concept_name'] = row.concept_name
        column['description'] = row.description
        column['data_type'] = row.data_type
        column['max_length'] = row.max_length
        return column

    def retrieve_type_info (self, row): 
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

    def get_glossary_column_data (self, schemaName, tableName, columnName) -> dict:
        if len (self.glossary) == 0:
            self.load_data (SQLServer.glossaryQuery, self.retrieve_glossary_info, self.glossary)
        return self.glossary[schemaName][tableName][columnName] 

    def get_type_info_column_data (self, schemaName, tableName, columnName) -> dict:
        if len (self.types) == 0:
            self.load_data (SQLServer.typeQuery, self.retrieve_type_info, self.types)
        return self.types[schemaName][tableName][columnName] 