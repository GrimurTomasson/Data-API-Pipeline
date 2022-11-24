import os
import json
from pkgutil import get_data
import re
from dataclasses import dataclass

import Decorators
import APISupport
from SharedDataClasses import CountPercentage

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
        APISupport.initialize ()
        self._reportFilename = "api_data_health_report.md"
        self._tableNameRegEx = re.compile('[a-z\_0-9]+\.yml', re.IGNORECASE)
        self._cardinalityMap = {}
        
        self._projectName = APISupport.config['latest']['name']
        self._databaseServer = APISupport.config['database']['server']
        self._databaseName = APISupport.config['database']['name']
        self._databaseConnection = APISupport.get_database_connection (self._databaseServer, self._databaseName)
        return

    def __get_relation_name (self, filePath) -> str:
        return self._tableNameRegEx.search (filePath).group ()[:-4]

    def __get_relation_name_from_test_name (self, projectName, testName) -> str:
        tableNameFromTestRegEx = re.compile (f"{projectName}\_[a-z\_]+\_v[0-9]+", re.IGNORECASE) # source_is_true_Nustada_bekkur_v1_lokadagur__lokadagur_upphafsdagur
        return tableNameFromTestRegEx.search (testName).group ()[len (projectName)+1:]

    def __retrieve_relation_cardinality (self, project, filePath) -> int:
        tableName = self.__get_relation_name (filePath)
        if tableName in self._cardinalityMap:
            return self._cardinalityMap[tableName]
        else:
            rows = self._databaseConnection.cursor ().execute (f"SELECT COUNT(1) AS fjoldi FROM {project}.{tableName}").fetchval ()
            print (f"\tCardinality for table {project}.{tableName} retrieved - cardinality: {rows}")
            self._cardinalityMap[tableName] = rows
            return rows

    @Decorators.execution_time(tabCount=1)
    def __retrieve_json_object (self) -> any:
        testLog = APISupport.get_file_contents (APISupport.dbt_test_output_file_info.qualified_name)
        # Convert the log file contents into legal json
        testLog = testLog.replace ("\n", "").replace ("\r", "")
        testLog = "{\"entries\": [" + testLog.replace ("}{", "},{") + "]}"
        jsonObject = json.loads (testLog)
        return jsonObject

    def __create_relation_stats (self, testList: list[TestEntry]) -> list[RelationStats]:
        relationStatMap = {}
        # Aggregating results
        for test in testList:
            relation = self.__get_relation_name_from_test_name (self._projectName, test.testName)
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

            okStats = CountPercentage (ok, APISupport.to_percentage (ok, total, 2))
            errorStats =CountPercentage (error, APISupport.to_percentage (error, total, 2))
            relStats = RelationStats(name = key, ok = okStats, error = errorStats, total = total)

            stats.append (relStats)
        #print(f"Relation stats: {stats}")
        return stats

    def __enrich_errors (self, errorList, fileMap) -> list[Error]:
        for error in errorList:
            noRows = 0
            filePath = fileMap[error.sql_filename]
            if len (filePath) > 0:
                fRelativePath = f"{self._projectName}\{filePath}"
                fPath = f"{APISupport.workingDirectory}/dbt/{fRelativePath}" # Á þetta að koma úr config?
                sql = APISupport.get_file_contents (fPath).strip ()
                noRows = self.__retrieve_relation_cardinality (self._projectName, fPath)
                relationName = self.__get_relation_name (fPath)

                error.relation_name = relationName 
                error.rows_in_relation = noRows
                error.rows_on_error_percentage = APISupport.to_percentage (error.rows_on_error, noRows, 4)
                error.query_path = fRelativePath
                error.sql = sql
        return errorList

    @Decorators.execution_time(tabCount=1)
    def __retrieve_data(self, jsonObject) -> HealthReport:
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
                
                errorStats = CountPercentage (error, APISupport.to_percentage (error, total, 2))
                okStats = CountPercentage (ok, APISupport.to_percentage (ok, total, 2))
                skippedStats = CountPercentage (skipped, APISupport.to_percentage (skipped, total, 2))
                totalStats = CountPercentage (total, 100)
                healthReport.stats.total = StatsTotal (errorStats, okStats, skippedStats, totalStats)

            if entry["code"] == "A001": # Header
                headerExecution = HeaderExecution (entry["ts"], entry["invocation_id"])
                healthReport.header = Header (APISupport.config['database']['name'], (entry["data"])["v"].replace ("=", ""), headerExecution)     

            if entry["code"] == "Z026": # File map
                sqlPath = (entry["data"])["path"]
                sqlFile = sqlPath[-(len (sqlPath) - (sqlPath.rindex ("\\") + 1)):]
                fileMap[sqlFile] = sqlPath

        # Enrichment
        healthReport.stats.relation = self.__create_relation_stats (testList)
        healthReport.errors = self.__enrich_errors (healthReport.errors, fileMap)
        
        return healthReport

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def generate_data (self) -> None:
        """Generating data health report data"""
        APISupport.initialize () # Til þess að geta keyrt hverja skriftu sjálfstætt
        jsonObject = self.__retrieve_json_object () 
        apiHealth = self.__retrieve_data (jsonObject)
        jsonData = json.dumps (apiHealth, indent=4, cls=APISupport.EnhancedJSONEncoder)
        APISupport.print_v (f"\tWriting data health report data to: {APISupport.api_data_health_report_data_file_info.qualified_name}")
        APISupport.write_file (jsonData, APISupport.api_data_health_report_data_file_info.qualified_name)
        return 

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def generate_report (self) -> None:
        """Generating data health report"""
        APISupport.generate_markdown_document ("api_data_health_report_template.md", APISupport.api_data_health_report_data_file_info.name, self._reportFilename)
        return

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def publish (self) -> None:
        """Publishing data health report"""
        APISupport.get_target_knowledge_base_interface ().publish (self._reportFilename, 'data-health-report') 
        return

    @Decorators.output_headers
    @Decorators.execution_time
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