import os
import json
from pkgutil import get_data
import re
from dataclasses import dataclass

import APISupport
import DataHealthClasses

@dataclass
class TestEntry:
    testName: str
    ok: int
    error: int

@dataclass
class StatsEntry:
    ok: int
    error: int

tableNameRegEx = re.compile('[a-z\_0-9]+\.yml', re.IGNORECASE)
cardinalityMap = {}

def get_relation_name (filePath) -> str:
    return tableNameRegEx.search (filePath).group ()[:-4]

def get_relation_name_from_test_name (projectName, testName) -> str:
    tableNameFromTestRegEx = re.compile (f"{projectName}\_[a-z\_]+\_v[0-9]+", re.IGNORECASE) # source_is_true_Nustada_bekkur_v1_lokadagur__lokadagur_upphafsdagur
    return tableNameFromTestRegEx.search (testName).group ()[len (projectName)+1:]

def retrieve_relation_cardinality (databaseServer, api, project, filePath) -> int:
    tableName = get_relation_name (filePath)
    if tableName in cardinalityMap:
        return cardinalityMap[tableName]
    else:
        startTime = APISupport.get_current_time ()
        conn = APISupport.get_database_connection (databaseServer, api)
        rows = conn.cursor ().execute (f"SELECT COUNT(1) AS fjoldi FROM {project}.{tableName}").fetchval ()
        print (f"\tCardinality for table {project}.{tableName} retrieved in {APISupport.get_execution_time_in_seconds (startTime)} seconds - cardinality: {rows}")
        cardinalityMap[tableName] = rows
        return rows

def retrieve_json_object (filename):
    testLog = APISupport.get_file_contents (filename)
    # Convert the log file contents into legal json
    testLog = testLog.replace ("\n", "").replace ("\r", "")
    testLog = "{\"entries\": [" + testLog.replace ("}{", "},{") + "]}"
    jsonObject = json.loads (testLog)
    return jsonObject

def create_relation_stats (projectName: str, testList: list[TestEntry]) -> list[DataHealthClasses.RelationStats]:
    relationStatMap = {}
    # Aggregating results
    for test in testList:
        relation = get_relation_name_from_test_name (projectName, test.testName)
        if relation in relationStatMap:
            relationStatMap[relation] = StatsEntry (relationStatMap[relation].ok + test.ok, relationStatMap[relation].error + test.error)
        else:
            relationStatMap[relation] = StatsEntry (test.ok, test.error)

    # Creating a relation stats list
    stats = [] #list[DataHealthClasses.RelationStats]
    for key in sorted (relationStatMap):
        ok = relationStatMap[key].ok
        error = relationStatMap[key].error
        total = ok + error     

        okStats = DataHealthClasses.CountPercentage (ok, APISupport.to_percentage (ok, total, 2))
        errorStats = DataHealthClasses.CountPercentage (error, APISupport.to_percentage (error, total, 2))
        relStats = DataHealthClasses.RelationStats(name = key, ok = okStats, error = errorStats, total = total)

        stats.append (relStats)
    #print(f"Relation stats: {stats}")
    return stats

def enrich_errors (errorList, fileMap, projectName, workingDirectory) -> list[DataHealthClasses.Error]:
    databaseName = APISupport.config['database']['name']
    databaseServer = APISupport.config['database']['server']
    
    for error in errorList:
        noRows = 0
        filePath = fileMap[error.sql_filename]
        if len (filePath) > 0:
            fRelativePath = f"{projectName}\{filePath}"
            fPath = f"{workingDirectory}/dbt/{fRelativePath}" # Á þetta að koma úr config?
            sql = APISupport.get_file_contents (fPath).strip ()
            noRows = retrieve_relation_cardinality (databaseServer, databaseName, projectName, fPath)
            relationName = get_relation_name (fPath)

            error.name = relationName # Yfirskrift ! Skoða betur.
            error.rows_in_relation = noRows
            error.rows_on_error_percentage = APISupport.to_percentage (error.rows_on_error, noRows, 4)
            error.query_path = fRelativePath
            error.sql = sql
    return errorList

def retrieve_data(jsonObject, workingDirectory) -> DataHealthClasses.HealthReport:
    healthReport = DataHealthClasses.HealthReport (None, DataHealthClasses.Stats(None, list[DataHealthClasses.RelationStats]), list[DataHealthClasses.Error]())
    fileMap = {}
    testList = [] #list[TestEntry]
    projectName = APISupport.config['latest']['name']
    
    for entry in jsonObject['entries']:
        if entry["code"] == "Q009": # OK
            testList.append (TestEntry(entry["data"]["node_info"]["node_name"], 1, 0))

        if entry["code"] == "Q011" and entry["level"] == "error": # Errors
            error = DataHealthClasses.Error ((entry["data"])["name"], (entry["data"])["failures"], ((entry["data"])["node_info"])["node_path"], None, None, None, None, None)
            healthReport.errors.append (error)
            
            testList.append (TestEntry(entry["data"]["node_info"]["node_name"], 0, 1))

        if entry["code"] == "Z023": # Stats
            error = ((entry["data"])["stats"])["error"]
            ok = ((entry["data"])["stats"])["pass"]
            skipped = ((entry["data"])["stats"])["skip"]
            total = ((entry["data"])["stats"])["total"]
            
            errorStats = DataHealthClasses.CountPercentage (error, APISupport.to_percentage (error, total, 2))
            okStats = DataHealthClasses.CountPercentage (ok, APISupport.to_percentage (ok, total, 2))
            skippedStats = DataHealthClasses.CountPercentage (skipped, APISupport.to_percentage (skipped, total, 2))
            totalStats = DataHealthClasses.CountPercentage (total, 100)
            healthReport.stats.total = DataHealthClasses.StatsTotal (errorStats, okStats, skippedStats, totalStats)

        if entry["code"] == "A001": # Header
            headerExecution = DataHealthClasses.HeaderExecution (entry["ts"], entry["invocation_id"])
            healthReport.header = DataHealthClasses.Header (APISupport.config['database']['name'], (entry["data"])["v"].replace ("=", ""), headerExecution)     

        if entry["code"] == "Z026": # File map
            sqlPath = (entry["data"])["path"]
            sqlFile = sqlPath[-(len (sqlPath) - (sqlPath.rindex ("\\") + 1)):]
            fileMap[sqlFile] = sqlPath

    # Enrichment
    healthReport.stats.relation = create_relation_stats (projectName, testList)
    healthReport.errors = enrich_errors (healthReport.errors, fileMap, projectName, workingDirectory)
    
    return healthReport

def run():
    workingDirectory = os.getcwd ()
    # Process input params!
    startTime = APISupport.get_current_time ()
    jsonObject = retrieve_json_object (f"{workingDirectory}/test_results.json") 
    print(f"Json object retrieved in {APISupport.get_execution_time_in_seconds (startTime)} seconds")
    # Extract data from json object
    startTime = APISupport.get_current_time ()
    apiHealth = retrieve_data (jsonObject, workingDirectory)
    print(f"Data retrieved from json in {APISupport.get_execution_time_in_seconds (startTime)} seconds")
    # Create results
    startTime = APISupport.get_current_time ()
    jsonData = json.dumps (apiHealth, indent=4, cls=APISupport.EnhancedJSONEncoder)
    APISupport.write_file (jsonData, f"{workingDirectory}/api_data_health_report_data.json")
    print(f"Enriched test data written to disk in {APISupport.get_execution_time_in_seconds (startTime)} seconds")
    return 0

def main ():
    return run ()

if __name__ == '__main__':
    main ()