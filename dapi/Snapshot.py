# pip install pyodbc
import re
from logging import NullHandler
from tkinter import FIRST
from colorama import Fore
from typing import List

from .Shared.Decorators import output_headers, execution_time
from .Shared.Config import Config
from .Shared.Utils import Utils
from .Shared.Logger import Logger
from .Shared.PrettyPrint import Pretty
from .Shared.Environment import Environment
from .TargetDatabase.TargetDatabase import Relations, Relation
from .TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory, TargetDatabase

class Snapshot:

    def __init__ (self):
        self._snapshotDateColumnName = Config["history"]["snapshot-date-column"]
        Logger.info (f"Snapshot date colum name: {self._snapshotDateColumnName}")
        
        self._historyDatabaseName = Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')
        self._snapshotDatabaseName = Config['history']['snapshot-database'] if 'snapshot-database' in Config['history'] else Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')

        message = ( f"history database:     {self._historyDatabaseName}\n"
                    f"snapshot database:    {self._snapshotDatabaseName}\n" )
        Logger.info (message)

        self._historyDb = TargetDatabaseFactory ().get_target_database ()
        self._historyDb.set_connection (self._historyDatabaseName)

        self._snapshotDb = TargetDatabaseFactory ().get_target_database ()
        self._snapshotDb.set_connection (self._snapshotDatabaseName)
        return

    def __get_first_column (self, relations:Relations, source_table:str):
        """Retrieves the name of the first column in a relation"""
        firstColumn = relations[source_table].columnNames[0]
        Logger.debug (f"\t\tFirst column for {relations[source_table].schema}.{source_table}: {firstColumn}")
        return firstColumn

    def __create_missing_tables (self, source_database, source_schema, snapshot_schema) -> int:
        """Creates empty snapshot tables if they are missing"""
        missingTables = []
        for table in self._sourceRelations.list:
            if table.name not in self._snapshotRelations.dictionary.keys():
                missingTables.append (table.name)
        noMissingTables = len (missingTables)
        print (f"Number of missing snapshot tables: {noMissingTables}\n")
        for missingTable in missingTables:
            print (f"\tCreating missing snapshot table: {self._snapshotDb.get_database_name()}.{snapshot_schema}.{missingTable} -> ", end="")
            # Our API tables always contain a first column, single column unique id
            firstColumn = self.__get_first_column (self._sourceRelations, missingTable)
            self._snapshotDb.create_empty_target_table (source_database, source_schema, missingTable, [firstColumn], snapshot_schema, missingTable, self._snapshotDateColumnName)
        return noMissingTables

    def __get_view_name (self, tableName):
        if not re.match(".+_v[0-9]+$", tableName, flags=re.IGNORECASE):
            return tableName
        return tableName[0:tableName.rindex('_')]

    @output_headers
    @execution_time(tabCount=1)
    def __create_missing_views (self, snapshot_schema, history_schema) -> int:
        """Creates missing views for snapshot tables"""
        message = ( f"Snapshot schema: {self._snapshotDatabaseName}.{snapshot_schema}\n"
                    f"History schema:  {self._historyDatabaseName}.{history_schema}\n")
        Logger.info (message)
        self._historyDb.create_schema_if_missing (history_schema)
        print ("Creating initial views. Those need to be updated by hand when new versions are released!")
        print (f"Checking for views for {len (self._snapshotRelations.list)} snapshot tables in {self._snapshotDatabaseName}.{snapshot_schema}")
        viewsCreated = 0
        for snapshotTable in self._snapshotRelations.list:
            viewName = self.__get_view_name (snapshotTable.name)
            Logger.debug (f"\n\tProcessing - {self._snapshotDatabaseName}.{snapshot_schema}.{snapshotTable.name} - View: {viewName}")
            message = f"\tChecking for table {snapshotTable}"
            if snapshotTable.name not in self._historyRelations.dictionary.keys():
                message += f" - Creating view {viewName}"
                self._historyDb.create_or_alter_view (history_schema, viewName, self._snapshotDatabaseName, snapshot_schema, snapshotTable.name)
                message += " - Done"
                viewsCreated += 1
            else:
                message += f" - View existed"
            Logger.debug (message)
        Logger.debug (f"\t{viewsCreated} views created.\n")
        return viewsCreated

    def __add_missing_columns (self, source_schema, snapshot_schema):
        """Adding missing columns to snapshot tables missing columns"""
        Logger.info (f"Checking for missing columns in {len (self._sourceRelations.list)} source tables")
        for sourceTable in self._sourceRelations.list:
            
            missingColumns = set (self._sourceRelations[sourceTable.name].columnNames) - set (self._snapshotRelations[sourceTable.name].columnNames)

            Logger.info (f"\t{len(missingColumns)} missing columns in {snapshot_schema}.{sourceTable.name}")
            for missingColumn in missingColumns:
                self._snapshotDb.clone_column (source_schema, sourceTable.name, snapshot_schema, sourceTable.name, missingColumn) # ToDo: Laga. Fer á milli grunna, mun brotna !!!
            # We cannot update the view in the history schema because we don't know if it is union'ing multiple versions of snapshots!
        return

    @output_headers
    @execution_time(tabCount=1)
    def __create_snapshot (self, source_schema, table_name, snapshot_schema, target_date):
        """Creating a single snapshot"""
        Logger.info (f"\tTaking a snapshot of {self._historyDatabaseName}.{source_schema}.{table_name} and adding it to {self._snapshotDatabaseName}.{snapshot_schema}.{table_name} for {self._snapshotDateColumnName} = {target_date}")
        firstColumn = self.__get_first_column (self._sourceRelations, table_name) # ToDo, look at creating keys in dbt models and detecting them.
        columnList = self._sourceRelations[table_name].columnNames
        self._snapshotDb.insert_data (self._historyDatabaseName, source_schema, table_name, columnList, [firstColumn], snapshot_schema, table_name, self._snapshotDateColumnName, target_date) 
        return

    @output_headers
    @execution_time
    def __create_snapshots (self, source_schema, snapshot_schema, history_schema) -> None:
        """Creating snapshots for one schema"""
        # Testing the connection
        targetDate = self._historyDb.get_date () # Ensures we always remove and add the same date, even if we cross midnight, also a great connection test!
        Logger.info (f"Snapshot date - {self._snapshotDateColumnName}: {targetDate}")

        # Relation and column info used to detect missing columns, views, tables etc.
        self._sourceRelations = self._historyDb.retrieve_relations (source_schema)
        self._snapshotRelations = self._snapshotDb.retrieve_relations (snapshot_schema)
        self._historyRelations = self._historyDb.retrieve_relations (history_schema)

        self._snapshotDb.create_schema_if_missing (snapshot_schema)
        self._historyDb.create_schema_if_missing (history_schema)

        # Create missing snapshot 
        if self.__create_missing_tables (self._historyDatabaseName, source_schema, snapshot_schema) > 0: # We created new tables and need to reload snapshot tables
            self._snapshotRelations = self._snapshotDb.retrieve_relations (snapshot_schema)

        self.__add_missing_columns (source_schema, snapshot_schema) # Create missing columns in snapshots

        if self.__create_missing_views (snapshot_schema, history_schema) > 0: # Views created, update view info
            self._historyRelations = self._historyDb.retrieve_relations (history_schema)
            
        for sourceTable in self._sourceRelations.list:
            self._snapshotDb.delete_data (snapshot_schema, sourceTable.name, self._snapshotDateColumnName, targetDate)
            self.__create_snapshot (source_schema, sourceTable.name, snapshot_schema, targetDate)
        return

    @output_headers
    @execution_time
    def create (self) -> None:
        """Taking snapshots for the Latest models"""
        if Config["history"]["projects"] == None:
            Logger.debug ("No snapshots defined!")
            return

        for item in Config["history"]["projects"]:
            sourceSchema = item["project"]["source-schema"]
            snapshotSchema = item["project"]["snapshot-schema"]
            publicSchema = item["project"]["public-schema"]

            message = ( f"sourceSchema:   {sourceSchema}\n"
                        f"snapshotSchema: {snapshotSchema}\n"
                        f"publicSchema:   {publicSchema}\n")
            Logger.info (message)

            self.__create_snapshots (sourceSchema, snapshotSchema, publicSchema)
        return

def main ():
    return Snapshot ().create ()

if __name__ == '__main__':
    main ()