from ..Shared.Config import Config
from .TargetDatabase import TargetDatabase
from .SQLServer import SQLServer

class TargetDatabaseFactory:
    _supportedDatabases = ['SQL-Server']

    def __init__(self):
        self._targetDatabase = self.__generate_target_database ()

    def get_target_database (self) -> TargetDatabase:
        return self._targetDatabase

    def __generate_target_database (self) -> TargetDatabase:
        self._targetDatabaseName = Config['database']['type']
        if len (self._targetDatabaseName) and self._targetDatabaseName not in TargetDatabaseFactory._supportedDatabases:
            print(f"Database in config is not support by pipeline. Config: {Config['database']['type']}. Supported databases: {TargetDatabaseFactory._supportedDatabases}")
            raise
        
        if self._targetDatabaseName == 'SQL-Server':    
            return SQLServer ()