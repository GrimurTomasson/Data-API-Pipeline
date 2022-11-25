import os
import json
from pkgutil import get_data
import re
from dataclasses import dataclass

from Shared.Decorators import output_headers, execution_time
from Shared.Config import Config
from Shared.Utils import Utils
from TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory, TargetDatabase
from TargetKnowledgeBase.TargetKnowledgeBaseFactory import TargetKnowledgeBaseFactory, TargetKnowledgeBase
from Shared.DataClasses import CountPercentage
import Shared.Json

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
    ok: CountPercentage
    skipped: CountPercentage
    total: CountPercentage

@dataclass
class RelationStats:
    name: str
    ok: CountPercentage
    error: CountPercentage
    total: int

@dataclass
class Stats:
    total: StatsTotal
    relation: list[RelationStats]

@dataclass
class Error:
    name: str
    rows_on_error: int
    sql_filename: str
    relation_name: str
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
        testName: str
        ok: int
        error: int

    @dataclass
    class StatsEntry:
        ok: int
        error: int

    def __init__ (self) -> None:
        self._reportFilename = "api_data_health_report.md"
        self._tableNameRegEx = re.compile ('[a-z\_0-9]+\.yml', re.IGNORECASE)
        self._cardinalityMap = {}

        self._config = Config ()
        self._utils = Utils ()
        
        self._projectName = self._config['latest']['name']
        self._projectRelativePath = self._config['latest']['relative-path']
        self._utils.print_v (f"Project name: {self._projectName} - Relative path: {self._projectRelativePath}")

        self._targetDatabase = TargetDatabaseFactory ().get_target_database ()
        self._databaseConnection = self._targetDatabase.get_connection ()
        
        self._targetKnowledgeBase = TargetKnowledgeBaseFactory ().get_target_knowledge_base ()
        return

    def __get_relation_name (self, filePath) -> str:
        return self._tableNameRegEx.search (filePath).group ()[:-4]

    def __get_relation_name_from_test_name (self, testName) -> str:
        self._utils.print_v (f"project: {self._projectName} - test name: {testName}")
        tableNameFromTestRegEx = re.compile (f"{self._projectName}\_[a-z\_]+\_v[0-9]+", re.IGNORECASE) # source_is_true_Nustada_bekkur_v1_lokadagur__lokadagur_upphafsdagur
        # ToDo: Búa til mynstur sem höndlar fleiri útgáfur, t.d.: project: Latest - test name: accepted_values_Address_fiber_optic_state_v1_L_apartment_number__MISSING_FROM_SOURCE
        searchResults = tableNameFromTestRegEx.search (testName)
        if searchResults == None:
                return ''
        return searchResults.group ()[len (self._projectName)+1:]

    def __retrieve_relation_cardinality (self, filePath) -> int:
        tableName = self.__get_relation_name (filePath)
        if tableName in self._cardinalityMap:
            return self._cardinalityMap[tableName]
        else:
            rows = -1
            try:
                rows = self._databaseConnection.cursor ().execute (f"SELECT COUNT(1) AS fjoldi FROM {self._projectName}.{tableName}").fetchval ()
                print (f"\tCardinality for table {self._projectName}.{tableName} retrieved - cardinality: {rows}")
                
            except Exception as ex:
                print (f"Failure to retrieve relation cardinality: {ex}")
                
            self._cardinalityMap[tableName] = rows
            return rows

    @execution_time(tabCount=1)
    def __retrieve_json_object (self) -> any:
        """Retrieving json file from disk and fixing it"""
        testLog = self._utils.get_file_contents (self._config.dbtTestOutputFileInfo.qualified_name)
        # Convert the log file contents into legal json
        testLog = testLog.replace ("\n", "").replace ("\r", "")
        testLog = "{\"entries\": [" + testLog.replace ("}{", "},{") + "]}"
        jsonObject = json.loads (testLog)
        return jsonObject

    def __create_relation_stats (self, testList: list[TestEntry]) -> list[RelationStats]:
        relationStatMap = {}
        # Aggregating results
        for test in testList:
            relation = self.__get_relation_name_from_test_name (test.testName)
            if relation in relationStatMap:
                relationStatMap[relation] = DataHealthReport.StatsEntry (relationStatMap[relation].ok + test.ok, relationStatMap[relation].error + test.error)
            else:
                relationStatMap[relation] = DataHealthReport.StatsEntry (test.ok, test.error)

        # Creating a relation stats list
        stats = [] #list[DataHealthClasses.RelationStats]
        for key in sorted (relationStatMap):
            ok = relationStatMap[key].ok
            error = relationStatMap[key].error
            total = ok + error     

            okStats = CountPercentage (ok, self._utils.to_percentage (ok, total, 2))
            errorStats = CountPercentage (error, self._utils.to_percentage (error, total, 2))
            relStats = RelationStats (name = key, ok = okStats, error = errorStats, total = total)

            stats.append (relStats)
        #print(f"Relation stats: {stats}")
        return stats

    def __enrich_errors (self, errorList, fileMap) -> list[Error]:
        for error in errorList:
            noRows = 0
            filePath = fileMap[error.sql_filename]
            if len (filePath) > 0:
                fRelativePath = f"{self._projectName}\{filePath}"
                fPath = f"{self._config.workingDirectory}/{self._projectRelativePath}/{filePath}" 

                self._utils.print_v (f"fRelativePath: {fRelativePath}")
                self._utils.print_v (f"fPath: {fPath}")

                sql = self._utils.get_file_contents (fPath).strip ()
                noRows = self.__retrieve_relation_cardinality (fPath)
                relationName = self.__get_relation_name (fPath)

                error.relation_name = relationName 
                error.rows_in_relation = noRows
                error.rows_on_error_percentage = self._utils.to_percentage (error.rows_on_error, noRows, 4)
                error.query_path = fRelativePath
                error.sql = sql
        return errorList

    @execution_time(tabCount=1)
    def __retrieve_data(self, jsonObject) -> HealthReport:
        """Extraction of relevant data from dbt testing json"""
        healthReport = HealthReport (None, Stats(None, list[RelationStats]), list[Error]())
        fileMap = {}
        testList = [] #list[TestEntry]
        
        for entry in jsonObject['entries']:
            if entry["code"] == "Q009": # OK
                testList.append (DataHealthReport.TestEntry(entry["data"]["node_info"]["node_name"], 1, 0))

            if entry["code"] == "Q011" and entry["level"] == "error": # Errors
                error = Error ((entry["data"])["name"], (entry["data"])["failures"], ((entry["data"])["node_info"])["node_path"], None, None, None, None, None)
                healthReport.errors.append (error)
                
                testList.append (DataHealthReport.TestEntry(entry["data"]["node_info"]["node_name"], 0, 1))

            if entry["code"] == "Z023": # Stats
                error = ((entry["data"])["stats"])["error"]
                ok = ((entry["data"])["stats"])["pass"]
                skipped = ((entry["data"])["stats"])["skip"]
                total = ((entry["data"])["stats"])["total"]
                
                errorStats = CountPercentage (error, self._utils.to_percentage (error, total, 2))
                okStats = CountPercentage (ok, self._utils.to_percentage (ok, total, 2))
                skippedStats = CountPercentage (skipped, self._utils.to_percentage (skipped, total, 2))
                totalStats = CountPercentage (total, 100)
                healthReport.stats.total = StatsTotal (errorStats, okStats, skippedStats, totalStats)

            if entry["code"] == "A001": # Header
                headerExecution = HeaderExecution (entry["ts"], entry["invocation_id"])
                healthReport.header = Header (self._config['database']['name'], (entry["data"])["v"].replace ("=", ""), headerExecution)     

            if entry["code"] == "Z026": # File map
                sqlPath = (entry["data"])["path"]
                sqlFile = sqlPath[-(len (sqlPath) - (sqlPath.rindex ("\\") + 1)):]
                fileMap[sqlFile] = sqlPath

        # Enrichment
        healthReport.stats.relation = self.__create_relation_stats (testList)
        healthReport.errors = self.__enrich_errors (healthReport.errors, fileMap)
        
        return healthReport

    @output_headers(tabCount=1)
    @execution_time(tabCount=1)
    def generate_data (self) -> None:
        """Generating data health report data"""
        jsonObject = self.__retrieve_json_object () 
        apiHealth = self.__retrieve_data (jsonObject)
        jsonData = json.dumps (apiHealth, indent=4, cls=Shared.Json.EnhancedJSONEncoder)
        self._utils.print_v (f"\tWriting data health report data to: {self._config.apiDataHealthReportDataFileInfo.qualified_name}")
        self._utils.write_file (jsonData, self._config.apiDataHealthReportDataFileInfo.qualified_name)
        return 

    @output_headers(tabCount=1)
    @execution_time(tabCount=1)
    def generate_report (self) -> None:
        """Generating data health report"""
        self._utils.generate_markdown_document ("api_data_health_report_template.md", self._config.apiDataHealthReportDataFileInfo.name, self._reportFilename)
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
        self.generate_data ()
        self.generate_report ()
        self.publish ()
        return

def main ():
    return DataHealthReport ().generate ()

if __name__ == '__main__':
    main ()