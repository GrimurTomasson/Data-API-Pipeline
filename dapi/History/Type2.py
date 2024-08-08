import re
from logging import NullHandler
from tkinter import FIRST
from colorama import Fore
from typing import List
from collections import namedtuple

from ..Shared.Decorators import post_execution_output
from ..Shared.Config import Config
from ..Shared.Utils import Utils
from ..Shared.LogLevel import LogLevel
from ..Shared.Logger import Logger
from ..Shared.PrettyPrint import Pretty
from ..Shared.Environment import Environment
from ..TargetDatabase.TargetDatabase import Relations, Relation
from ..TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory, TargetDatabase
from ..Shared.AuditDecorators import audit

ColumnRename = namedtuple ('ColumnRename', ['rFrom', 'rTo'])

class Type2:

    @post_execution_output (logLevel=LogLevel.DEBUG)
    def __init__ (self):
        if not 'history' in Config._config or not 'type-2' in Config['history']:
            self._enabled = False
            return
        
        self._enabled = Config['history']['enabled'] if 'enabled' in Config['history'] else True
        if self._enabled == False:
            return
        
        self._publicHistoryDatabaseName = Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')
        self._type2HistoryDatabaseName = Config['history']['type-2']['history-database'] if 'history-database' in Config['history']['type-2'] else Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')
        
        self._type2Db = TargetDatabaseFactory ().get_target_database ()
        self._type2Db.set_connection (self._type2HistoryDatabaseName)

        self._publicHistoryDb = TargetDatabaseFactory ().get_target_database ()
        self._publicHistoryDb.set_connection (self._publicHistoryDatabaseName)

        self._columnsToIgnore = []
        self._columnsToRename = []
        if 'target-columns' in Config['history']['type-2']:
            if 'ignore' in Config['history']['type-2']['target-columns']:
                for columnName in Config["history"]['type-2']['target-columns']['ignore']:
                    self._columnsToIgnore.append (columnName)
            if 'rename' in Config['history']['type-2']['target-columns']:
                for item in Config['history']['type-2']['target-columns']['rename']:
                    self._columnsToRename.append (ColumnRename (item['pair']['from'], item['pair']['to']))
        return
    
    def __get_view_name (self, tableName):
        if 'relation-postfix' in Config['history']['type-2']:
            relationPostfix = Config['history']['type-2']['relation-postfix'] if 'relation-postfix' in Config['history']['type-2'] else ''
        if not re.match(".+_v[0-9]+$", tableName, flags=re.IGNORECASE):
            relationName = tableName
        else:
            relationName = tableName[0:tableName.rindex('_')]
        return relationName + relationPostfix
    
    def __remove_ignore_columns (self, columns) -> List[str]:
        return [column for column in columns if column not in self._columnsToIgnore]
    
    def __get_view_columns (self, columns) -> str:
        viewColumns = self.__remove_ignore_columns (columns)
        for rename in map (ColumnRename._make, self._columnsToRename):
            viewColumns = [rename.rTo if column == rename.rFrom else column for column in viewColumns]
        return viewColumns
    
    def __get_column_string (self, columns:List[str]) -> str:
        colStr = ''
        for column in columns:
            colStr += column + ','
        return colStr[:-1]

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def __create_or_update_view (self, type2Schema, publicSchema, relationName) -> None:
        viewName = self.__get_view_name (relationName)
        columns = self._type2Db.retrieve_columns (type2Schema, relationName)
        selectColumns = self.__get_column_string (self.__remove_ignore_columns (columns))
        viewColumns = self.__get_column_string (self.__get_view_columns (columns))

        self._publicHistoryDb.create_or_alter_view (publicSchema, viewName, self._type2HistoryDatabaseName, type2Schema, relationName, viewColumns, selectColumns)
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def __create_or_update_views (self, type_2_schema, public_schema) -> None:
        """Creating public history views for one schema"""
        relations = self._type2Db.retrieve_relations (type_2_schema).list
        type2HistoryRelations = [ri.name for ri in relations]
        print (type2HistoryRelations) # DEBUG
        
        self._publicHistoryDb.create_schema_if_missing (public_schema)
        for relation in type2HistoryRelations:
            self.__create_or_update_view (type_2_schema, public_schema, relation)
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def create (self) -> None:
        """Creating history views for type-2 history"""
        if self._enabled == False:
                Logger.info (Pretty.assemble_simple ("Type-2 history view creation has been disabled!"))
                return
        
        if Config["history"]['type-2']["projects"] == None:
            Logger.debug (Pretty.assemble_simple ("No type-2 history defined!"))
            return
        
        Logger.info (Pretty.assemble_simple (f"Public history database:         {self._publicHistoryDatabaseName}"))
        Logger.info (Pretty.assemble_simple (f"Type-2 history database:        {self._type2HistoryDatabaseName}\n"))

        for item in Config["history"]['type-2']["projects"]:
            type2Schema = item["project"]["history-schema"]
            publicSchema = item["project"]["public-schema"]

            Logger.info (Pretty.assemble_simple ("Project"))
            Logger.info (Pretty.assemble (value=f"type2Schema: {type2Schema}", tabCount=Pretty.Indent+1))
            Logger.info (Pretty.assemble (value=f"publicSchema:   {publicSchema}", tabCount=Pretty.Indent+1))

            self.__create_or_update_views (type2Schema, publicSchema)
        return

def main ():
    return Type2 ().create ()

if __name__ == '__main__':
    main ()