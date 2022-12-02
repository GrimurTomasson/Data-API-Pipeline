from abc import ABC, abstractmethod
import pyodbc

class TargetDatabase (ABC):

    @abstractmethod
    def get_connection (self) -> pyodbc.Connection: # All parameters should come from config, makes this flexible
        raise NotImplementedError

    @abstractmethod
    def get_type_length (self, columnData) -> str: 
        raise NotImplementedError

    @abstractmethod
    def get_type_info_column_data (self, schemaName, tableName, columnName) -> dict:
        raise NotImplementedError