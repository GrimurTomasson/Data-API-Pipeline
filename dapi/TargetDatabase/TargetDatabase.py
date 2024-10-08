from abc import ABC, abstractmethod
from typing import List
from dataclasses import dataclass
from datetime import date, datetime
import pyodbc

from ..Shared.Utils import Utils

@dataclass
class Relation:
    schema:str
    name:str
    isView:bool
    columnNames:List[str] = Utils.default_field ([])

@dataclass
class Relations:
    list:List[Relation]
    dictionary:dict

    def __getitem__(self, key):
        return self.dictionary[key]
    
# Auditing 
@dataclass
class dapi_invocation:
    id:str
    database:str
    operation:str
    status:str
    parameters:str
    execution_time_in_seconds:float
    version:str
    user:str
    host:str
    start_time:datetime
    op_number_starts:int
    op_number_ends:int
    stack_depth:int
    stack_depth_indicator:str

@dataclass
class dbt_invocation:
    id:str
    dapi_invocation_id:str
    operation:str
    status:str
    execution_time_in_seconds:float
    parameters:str
    version:str
    start_time:datetime
    
@dataclass
class dbt_action:
    invocation_id:str
    unique_id:str
    relation_name:str
    status:str
    execution_time_in_seconds:float
    rows_affected:int   
    thread_id:str

class TargetDatabase (ABC):

    @abstractmethod
    def set_connection (self, databaseName):
        """Sets a connection, using a connection string. Make it auto-committing."""
        raise NotImplementedError

    @abstractmethod
    def get_connection (self) -> pyodbc.Connection: 
        """Returns an open database connection."""
        raise NotImplementedError
    
    @abstractmethod
    def get_database_type (self, pythonType) -> str:
        """Returns a database type for a Python type."""
        raise NotImplementedError
    
    @abstractmethod
    def get_database_name (self) -> str:
        """Returns the name of the database, can change due to set_connection."""
        raise NotImplementedError

    @abstractmethod
    def get_date (self) -> date:
        """Returns the current date of the database, without time."""
        raise NotImplementedError

    @abstractmethod
    def get_type_length (self, columnData) -> str: 
        """This function returns the length information for a column. It uses DB specific column data, returned by get_type_info_column_date (), as input."""
        raise NotImplementedError

    @abstractmethod
    def get_type_info_column_data (self, schemaName:str, tableName:str, columnName:str) -> dict: 
        """This function returns DB specific information about column type in a dictionary. This dictionary is used by get_type_length ()."""
        raise NotImplementedError

    @abstractmethod
    def retrieve_relations (self, schemaName:str) -> Relations:
        """Returns an alphabetically ordered list of relations, including column name in declaration order."""
        raise NotImplementedError
    
    @abstractmethod
    def retrieve_columns (self, schemaName:str, relationName:str) -> List[str]:
        """Returns the column names of a relation, in order."""
        raise NotImplementedError

    @abstractmethod
    def clone_column (self, sourceSchema:str, sourceTable:str, targetDatabase:str, targetSchema:str, targetTable:str, columnName:str) -> None:
        """Clones a column from the source table to the target table."""
        raise NotImplementedError

    @abstractmethod
    def drop_view (self, schemaName:str, viewName:str) -> None:
        """Drops a view."""
        raise NotImplementedError

    @abstractmethod
    def create_schema_if_missing (self, schemaName:str) -> None:
        """Creates schema if it does not exist."""
        raise NotImplementedError
    
    @abstractmethod
    def create_table_if_missing (self, schemaName:str, tableName:str, data_class) -> None:
        """Creates a table using dataclass variables for column names. This does not work for nested dataclasses!"""
        raise NotImplementedError

    @abstractmethod
    def create_empty_target_table (self, sourceDatabase:str, sourceSchema:str, sourceTable:str, sourceKeyColumns:List[str], targetSchema:str, targetTable:str, dateColumnName:str) -> None:
        """Creates an empty snapshot table which prefixes a date column to the column list of the source table but is otherwise type identical."""
        raise NotImplementedError

    @abstractmethod
    def create_or_alter_view (self, viewSchema:str, viewName:str, sourceDatabase:str, sourceSchema:str, sourceTable:str, viewColumns:str = None, selectColumns:str = None) -> None:
        """Adds or replaces a view which selects all columns from a source table."""
        raise NotImplementedError
    
    @abstractmethod
    def create_or_alter_view (self, viewSchema:str, viewName:str, sourceDatabase:str, sourceSchema:str, sourceTable:str) -> None:
        """Adds or replaces a view which selects all columns from a source table."""
        raise NotImplementedError

    @abstractmethod
    def delete_data (self, schemaName:str, tableName:str, comparisonColumn:str, columnValue:any) -> None:
        """Deletes all rows in a table where the comparison column has a particular value."""
        raise NotImplementedError

    @abstractmethod
    def insert_data (self, sourceDatabase:str, sourceSchema:str, sourceTable:str, sourceColumns:List[str], sourceKeyColumns:List[str], targetSchema:str, targetTable:str, dateColumnName:str, runDate:date) -> None:
        """Inserts all rows in the source table into the target table and adds a load-date to it."""
        raise NotImplementedError
    
    @abstractmethod
    def insert_dataclass (self, database:str, schema:str, table:str, data_class) -> None:
        """Inserts the contents of a single data class instance."""
        raise NotImplementedError
    
    @abstractmethod
    def insert_dataclasses (self, database:str, schema:str, table:str, data_classes:list) -> None:
        """Inserts the contents of a list of data class instances."""
        raise NotImplementedError
    
    @abstractmethod
    def retrieve_cardinality (self, schemaName:str, tableName:str) -> int:
        """Retrieves the cardinality of a relation."""
        raise NotImplementedError