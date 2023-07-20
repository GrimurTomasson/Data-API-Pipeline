import os
import json
from pkgutil import get_data
import re
from dataclasses import dataclass

from .Shared.Decorators import output_headers, execution_time
from .Shared.Config import Config
from .Shared.Environment import Environment
from .Shared.Utils import Utils
from .Shared.Logger import Logger
from .TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory, TargetDatabase
from .TargetKnowledgeBase.TargetKnowledgeBaseFactory import TargetKnowledgeBaseFactory, TargetKnowledgeBase
from .Shared.DataClasses import CountPercentage
from .Shared import Json

@dataclass
class HeaderExecution:
    timestamp: str
    id: str

@dataclass 
class Header:
    api_name: str
    dbt_version: str
    execution: HeaderExecution

@dataclass
class StatsTotal:
    error: CountPercentage
    warning: CountPercentage
    ok: CountPercentage
    skipped: CountPercentage
    total: CountPercentage

@dataclass
class RelationStats:
    database_name: str
    schema_name: str
    name: str
    ok: CountPercentage
    warning: CountPercentage
    error: CountPercentage
    total: int

@dataclass
class Stats:
    total: StatsTotal
    relation: list[RelationStats]

@dataclass
class Error:
    test_name: str
    unique_id: str
    sql_filename: str
    database_name: str
    schema_name: str
    relation_name: str
    rows_on_error: int
    rows_in_relation: int
    rows_on_error_percentage: int
    query_path: str
    sql: str

@dataclass
class HealthReport:
    header: Header
    stats: Stats
    errors: list[Error]

class DataHealthReport: # Main class
    @dataclass
    class TestEntry:
        database_name: str
        schema_name: str
        relation_name: str
        test_name: str
        unique_id: str
        ok: int
        warning: int
        error: int

    @dataclass
    class StatsEntry:
        database_name: set
        schema_name: str
        relation_name: str
        ok: int
        warning: int
        error: int

    @dataclass
    class ManifestEntry:
        database: str
        schema: str
        relation: str
        sql_filename: str
        query_path: str
        sql: str

    def __init__ (self) -> None:
        self._reportFilename = "api_data_health_report.md"
        self._cardinalityMap = {}
        
        self._projectName = Config['latest']['name']
        self._projectRelativePath = Config['latest']['relative-path']

        self._targetDatabase = TargetDatabaseFactory ().get_target_database ()
        self._targetKnowledgeBase = TargetKnowledgeBaseFactory ().get_target_knowledge_base ()
        return

    def __retrieve_relation_cardinality (self, databaseName, schemaName, tableName) -> int:
        key = f"{databaseName}.{schemaName}.{tableName}"
        if key in self._cardinalityMap:
            return self._cardinalityMap[key]
        else:
            rows = -1
            try:
                rows = self._targetDatabase.retrieve_cardinality (schemaName, tableName)    
            except Exception as ex:
                Logger.warning (f"Failure to retrieve relation cardinality: {ex}")
                
            self._cardinalityMap[key] = rows
            return rows

    @execution_time(tabCount=1)
    def __retrieve_json_object (self) -> any:
        """Retrieving json file from disk and fixing it"""
        testLog = Utils.get_file_contents (Config.dbtTestOutputFileInfo.qualified_name)
        # Convert the log file contents into legal json
        testLog = testLog.replace ("\n", "").replace ("\r", "")
        testLog = "{\"entries\": [" + testLog.replace ("}{", "},{") + "]}"
        jsonObject = json.loads (testLog)
        return jsonObject

    def __create_relation_stats (self, testList: list[TestEntry]) -> list[RelationStats]:
        relationStatMap = {}
        # Aggregating results
        for test in testList:
            key = f"{test.database_name}.{test.schema_name}.{test.relation_name}"
            if key in relationStatMap:
                current = relationStatMap[key]
                relationStatMap[key] = DataHealthReport.StatsEntry (test.database_name, test.schema_name, test.relation_name, current.ok + test.ok, current.warning + test.warning, current.error + test.error)
            else:
                relationStatMap[key] = DataHealthReport.StatsEntry (test.database_name, test.schema_name, test.relation_name, test.ok, test.warning, test.error)

        # Creating a relation stats list
        stats = [] #list[DataHealthClasses.RelationStats]
        for key in sorted (relationStatMap):
            ok = relationStatMap[key].ok
            warning = relationStatMap[key].warning
            error = relationStatMap[key].error
            total = ok + warning + error     

            okStats = CountPercentage (ok, Utils.to_percentage (ok, total, 2))
            warningStats = CountPercentage (warning, Utils.to_percentage (warning, total, 2))
            errorStats = CountPercentage (error, Utils.to_percentage (error, total, 2))
            relStats = RelationStats (database_name=relationStatMap[key].database_name, schema_name=relationStatMap[key].schema_name, name = relationStatMap[key].relation_name, ok = okStats, warning = warningStats, error = errorStats, total = total)

            stats.append (relStats)
        #print(f"Relation stats: {stats}")
        return stats

    def __enrich_errors (self, errorList) -> list[Error]:
        for error in errorList:
            noRows = self.__retrieve_relation_cardinality (error.database_name, error.schema_name, error.relation_name)
            error.rows_in_relation = noRows
            error.rows_on_error_percentage = Utils.to_percentage (error.rows_on_error, noRows, 4)

        errorList.sort (key=lambda x: (x.database_name, x.schema_name, x.relation_name, x.test_name))         
        return errorList
    
    def __get_parent_manifest_node (self, manifestJson, nodeKey):
        file_key_name = manifestJson['nodes'][nodeKey]["file_key_name"]
        model_name = file_key_name.partition ('.')[2]
        depends_on_nodes = manifestJson['nodes'][nodeKey]["depends_on"]["nodes"]
        
        for dependsOn in depends_on_nodes:
            if model_name in dependsOn:
                return manifestJson['nodes'][dependsOn]
        
        Logger.error (f"No manifest parent node found unique_id: {manifestJson['nodes'][nodeKey]['unique_id']}")
        return None

    def __create_manifest_map (self):
        with open (Config.dbtManifestFileInfo.qualified_name, encoding="utf-8") as json_file:
            manifestJson = json.load (json_file)
        manifest = {}
        
        testNodes = [x for x in manifestJson['nodes'] if manifestJson['nodes'][x]['resource_type'] == 'test']
        for nodeKey in testNodes:
            unique_id = manifestJson['nodes'][nodeKey]["unique_id"]
            database = manifestJson['nodes'][nodeKey]["database"]
            sql_filename = manifestJson['nodes'][nodeKey]["path"]            
            query_path = manifestJson['nodes'][nodeKey]["compiled_path"]

            if 'compiled_code' in manifestJson['nodes'][nodeKey]:
                sql = manifestJson['nodes'][nodeKey]["compiled_code"]
            else:
                Logger.error (f"No SQL found for test with unique_id: {unique_id}")
            parentNode = self.__get_parent_manifest_node (manifestJson, nodeKey)
            schema = parentNode["schema"]
            relation = parentNode["name"]
            
            #Logger.debug (f"{unique_id} - {database}.{schema}.{relation} - {sql_filename} - {query_path}")
            
            manifest[unique_id] = DataHealthReport.ManifestEntry (database, schema, relation, sql_filename, query_path, sql)
        return manifest


    @execution_time(tabCount=1)
    def __retrieve_data(self, jsonObject) -> HealthReport:
        """Extraction of relevant data from dbt testing json"""
        healthReport = HealthReport (None, Stats(None, list[RelationStats]), list[Error]())
        testList = [] #list[TestEntry]
        
        manifestMap = self.__create_manifest_map ()
        Logger.debug (f"Number of nodes in manifestMap: {len (manifestMap)}")

        for entry in jsonObject['entries']:
            if entry["info"]["name"] == "LogTestResult":
                ok = warning = error = 0
                test_name = entry["data"]["node_info"]["node_name"]
                unique_id = entry["data"]["node_info"]["unique_id"]
                sql_filename = entry["data"]["node_info"]["node_path"]
                query_path = manifestMap[unique_id].sql_filename
                relation_name = manifestMap[unique_id].relation
                database_name = manifestMap[unique_id].database
                schema_name = manifestMap[unique_id].schema
                sql = manifestMap[unique_id].sql

                if entry["data"]["status"] == "pass":
                    ok = 1
                if entry["data"]["status"] == "warning": # Er þetta réttur strengur?
                    warning = 1
                if entry["data"]["status"] == "fail": # Errors
                    error = 1
                    
                    healthReport.errors.append (Error (test_name, unique_id, sql_filename, database_name, schema_name, relation_name, entry["data"]["num_failures"], None, None, query_path, sql))
                    
                testEntry = DataHealthReport.TestEntry(database_name, schema_name, relation_name, test_name, unique_id, ok, warning, error)
                # Logger.debug(f"Test entry: {testEntry}")
                testList.append (testEntry)

            if entry["info"]["name"] == "StatsLine": # Stats
                error = ((entry["data"])["stats"])["error"]
                warning = ((entry["data"])["stats"])["warn"]
                ok = ((entry["data"])["stats"])["pass"]
                skipped = ((entry["data"])["stats"])["skip"]
                total = ((entry["data"])["stats"])["total"]
                
                errorStats = CountPercentage (error, Utils.to_percentage (error, total, 2))
                warningStats = CountPercentage (warning, Utils.to_percentage (warning, total, 2))
                okStats = CountPercentage (ok, Utils.to_percentage (ok, total, 2))
                skippedStats = CountPercentage (skipped, Utils.to_percentage (skipped, total, 2))
                totalStats = CountPercentage (total, 100)
                healthReport.stats.total = StatsTotal (errorStats, warningStats, okStats, skippedStats, totalStats)

            if entry["info"]["name"] == "MainReportVersion": 
                headerExecution = HeaderExecution (entry["info"]["ts"], entry["info"]["invocation_id"])
                databaseName = Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')
                healthReport.header = Header (databaseName, (entry["data"])["version"].replace ("=", ""), headerExecution)     

            
        # Enrichment
        healthReport.stats.relation = self.__create_relation_stats (testList)
        healthReport.errors = self.__enrich_errors (healthReport.errors)
        return healthReport

    @output_headers(tabCount=1)
    @execution_time(tabCount=1)
    def generate_data (self) -> None:
        """Generating data health report data"""
        jsonObject = self.__retrieve_json_object () 
        apiHealth = self.__retrieve_data (jsonObject)
        jsonData = json.dumps (apiHealth, indent=4, cls=Json.EnhancedJSONEncoder)
        Utils.write_file (jsonData, Config.apiDataHealthReportDataFileInfo.qualified_name)
        return 

    @output_headers(tabCount=1)
    @execution_time(tabCount=1)
    def generate_report (self) -> None:
        """Generating data health report"""
        Utils.generate_markdown_document ("api_data_health_report_template.md", Config.apiDataHealthReportDataFileInfo.name, self._reportFilename)
        return

    @output_headers(tabCount=1)
    @execution_time(tabCount=1)
    def publish (self) -> None:
        """Publishing data health report"""
        self._targetKnowledgeBase.publish (self._reportFilename, 'data-health-report') 
        return

    @output_headers
    @execution_time
    def generate (self) -> None:
        """Producing a data health report"""

        if Config['documentation']['data-health-report']['generate'] != True:
            return
        
        message = ( f"\tProject name:  {self._projectName}"
                    f"\tRelative path: {self._projectRelativePath}")
        Logger.info (message)

        self.generate_data ()
        self.generate_report ()
        self.publish ()
        return

def main ():
    return DataHealthReport ().generate ()

if __name__ == '__main__':
    main ()