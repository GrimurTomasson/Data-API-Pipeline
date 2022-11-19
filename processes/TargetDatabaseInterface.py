from abc import ABC,abstractmethod

class TargetDatabaseInterface(ABC):

    @abstractmethod
    def get_type_length(self, columnData) -> str: 
        raise NotImplementedError

    @abstractmethod
    def get_glossary_column_data (self, schemaName, tableName, columnName) -> dict:
        raise NotImplementedError

    @abstractmethod
    def get_type_info_column_data (self, schemaName, tableName, columnName) -> dict:
        raise NotImplementedError