import json
from pkgutil import get_data
from dataclasses import dataclass
import duckdb

from .Shared.Decorators import post_execution_output
from .Shared.Config import Config
from .Shared.Environment import Environment
from .Shared.Utils import Utils
from .Shared.PrettyPrint import Pretty
from .Shared.LogLevel import LogLevel
from .Shared.Logger import Logger
from .TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory, TargetDatabase
from .TargetKnowledgeBase.TargetKnowledgeBaseFactory import TargetKnowledgeBaseFactory, TargetKnowledgeBase
from .Shared.DataClasses import CountPercentage
from .Shared import Json
from .Shared.AuditDecorators import audit

relationStatsQuery = """
            WITH baseline AS (
                SELECT 
                    database_name
                    ,schema_name
                    ,relation_name
                    ,COUNT(result) FILTER (result = 'pass') AS ok_count
                    ,COUNT(result) FILTER (result = 'warning') AS warning_count
                    ,COUNT(result) FILTER (result = 'fail') AS fail_count
                FROM
                    test_entry
                GROUP BY 
                    ALL
            ), counted AS (
            SELECT 
                *, ok_count + warning_count + fail_count AS total_count
            FROM
                baseline
            )
            SELECT
                database_name
                ,schema_name
                ,relation_name
                ,ok_count
                ,round((ok_count * 100) / total_count, 2) AS ok_percentage
                ,warning_count
                ,round((warning_count * 100) / total_count, 2) AS warning_percentage
                ,fail_count
                ,round((fail_count * 100) / total_count, 2) AS fail_percentage
                ,total_count
            FROM
                counted
        """

statsSummaryQuery = """
            WITH baseline AS (
                SELECT 
                    SUM(ok_count) AS ok_count
                    ,SUM(warning_count) AS warning_count
                    ,SUM(fail_count) AS fail_count
                    ,SUM(total_count) AS total_count 
                FROM 
                    relation_stat
            ) 
            SELECT
                ok_count
                ,round((ok_count * 100) / total_count, 2) AS ok_percentage
                ,warning_count
                ,round((warning_count * 100) / total_count, 2) AS warning_percentage
                ,fail_count
                ,round((fail_count * 100) / total_count, 2) AS fail_percentage
                ,total_count
            FROM
                baseline
        """

databaseStatsQuery = """
            WITH baseline AS (
                SELECT 
                    database_name
                    ,SUM(ok_count) AS ok_count
                    ,SUM(warning_count) AS warning_count
                    ,SUM(fail_count) AS fail_count
                    ,SUM(total_count) AS total_count 
                FROM 
                    relation_stat 
                GROUP BY 
                    database_name
            )
            SELECT
                database_name
                ,ok_count
                ,round((ok_count * 100) / total_count, 2) AS ok_percentage
                ,warning_count
                ,round((warning_count * 100) / total_count, 2) AS warning_percentage
                ,fail_count
                ,round((fail_count * 100) / total_count, 2) AS fail_percentage
                ,total_count
            FROM
                baseline
            ORDER BY 
                database_name
        """

schemaSummaryQuery = """
            WITH baseline AS (
                SELECT 
                    database_name
                    ,schema_name
                    ,SUM(ok_count) AS ok_count
                    ,SUM(warning_count) AS warning_count
                    ,SUM(fail_count) AS fail_count
                    ,SUM(total_count) AS total_count 
                FROM 
                    relation_stat
                GROUP BY
                    ALL
            ) 
            SELECT
                schema_name
                ,ok_count
                ,round((ok_count * 100) / total_count, 2) AS ok_percentage
                ,warning_count
                ,round((warning_count * 100) / total_count, 2) AS warning_percentage
                ,fail_count
                ,round((fail_count * 100) / total_count, 2) AS fail_percentage
                ,total_count
            FROM
                baseline
            WHERE
                database_name =
        """

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
class StatsSummary:
    ok: CountPercentage
    warning: CountPercentage
    error: CountPercentage
    total: CountPercentage

@dataclass
class RelationStats:
    database_name: str
    schema_name: str
    name: str
    summary: StatsSummary

@dataclass
class SchemaStats:
    database_name: str
    name: str
    summary: StatsSummary
    relations: list[RelationStats]

@dataclass
class DatabaseStats:
    name: str
    summary: StatsSummary
    schemas: list[SchemaStats]

@dataclass
class Stats:
    summary: StatsSummary
    databases: list[DatabaseStats]

@dataclass
class Error:
    database_name: str
    schema_name: str
    relation_name: str
    test_name: str
    unique_id: str
    sql_filename: str
    rows_on_error: CountPercentage
    rows_in_relation: int
    query_path: str
    sql: str

@dataclass
class HealthReport:
    header: Header
    stats: Stats
    errors: list[Error]

class DataHealthReport: # Main class
    
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
                Logger.warning (Pretty.assemble_simple (f"Failure to retrieve relation cardinality: {ex}"))
                
            self._cardinalityMap[key] = rows
            return rows

    @post_execution_output (logLevel=LogLevel.INFO)
    def __retrieve_json_object (self) -> any:
        """Retrieving json file from disk and fixing it"""
        testLog = Utils.get_file_contents (Config.dbtTestOutputFileInfo.qualified_name)
        # Convert the log file contents into legal json
        testLog = testLog.replace ("\n", "").replace ("\r", "")
        testLog = "{\"entries\": [" + testLog.replace ("}{", "},{") + "]}"
        jsonObject = json.loads (testLog)
        return jsonObject
    
    def __create_relation_stats (self, database_name, schema_name) -> list[RelationStats]:
        relStats = duckdb.sql(f"SELECT * FROM relation_stat WHERE database_name = '{database_name}' AND schema_name = '{schema_name}' ORDER BY relation_name").fetchall()
        Logger.debug (Pretty.assemble_simple (f"Number of relations in {database_name}.{schema_name} in stats data: {len (relStats)}"))
        stats = []
        for entry in relStats:
            stats.append (RelationStats (entry[0], entry[1], entry[2], StatsSummary (CountPercentage (entry[3], entry[4]), CountPercentage (entry[5], entry[6]), CountPercentage (entry[7], entry[8]), CountPercentage (entry[9], 100))))
        return stats
    
    def __create_schema_stats (self, database_name) -> list[SchemaStats]:
        schemaStatsList = duckdb.sql (f"{schemaSummaryQuery} '{database_name}'").fetchall ()
        Logger.debug (Pretty.assemble_simple (f"Number of schemas in {database_name} in stats data: {len (schemaStatsList)}"))
        schemas = []
        for entry in schemaStatsList:
            Logger.debug (Pretty.assemble_simple (f"Starting work on schema: {entry}"))
            schemaStats = SchemaStats (database_name, entry[0], StatsSummary (CountPercentage (entry[1], entry[2]), CountPercentage (entry[3], entry[4]), CountPercentage (entry[5], entry[6]), CountPercentage (entry[7], 100)), None)
            schemaStats.relations = self.__create_relation_stats (database_name, entry[0])
            schemas.append (schemaStats)
        return schemas
    
    def __create_stats (self) -> Stats:
        # Create the source data
        duckdb.sql(f"CREATE TABLE relation_stat AS {relationStatsQuery}")
        #duckdb.execute (f"EXPORT DATABASE '{Config.workingDirectory}'")
        statsSummary = duckdb.sql (statsSummaryQuery).fetchone ()
        summary = StatsSummary (CountPercentage (statsSummary[0], statsSummary[1]), CountPercentage (statsSummary[2], statsSummary[3]), CountPercentage (statsSummary[4], statsSummary[5]), CountPercentage (statsSummary[6], 100))
        Logger.debug (Pretty.assemble_simple (f"Stats summary: {summary}"))
        stats = Stats (summary, [])
        
        databaseStatsList = duckdb.sql (databaseStatsQuery).fetchall ()
        Logger.debug (Pretty.assemble_simple (f"Number of databases in stats data: {len (databaseStatsList)}"))
        for database in databaseStatsList:
            Logger.debug (Pretty.assemble_simple (f"Starting work on database stats for: {database}"))
            databaseStats = DatabaseStats (database[0], StatsSummary (CountPercentage (database[1], database[2]), CountPercentage (database[3], database[4]), CountPercentage (database[5], database[6]), CountPercentage (database[7], 100)), None)
            databaseStats.schemas = self.__create_schema_stats (databaseStats.name)
            stats.databases.append (databaseStats)
        return stats
    
    def __get_parent_manifest_node (self, manifestJson, nodeKey):
        file_key_name = manifestJson['nodes'][nodeKey]["file_key_name"]
        model_name = file_key_name.partition ('.')[2]
        depends_on_nodes = manifestJson['nodes'][nodeKey]["depends_on"]["nodes"]
        
        for dependsOn in depends_on_nodes:
            if model_name in dependsOn:
                return manifestJson['nodes'][dependsOn]
        
        Logger.error (Pretty.assemble_simple (f"No manifest parent node found unique_id: {manifestJson['nodes'][nodeKey]['unique_id']}"))
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
                Logger.error (Pretty.assemble_simple (f"No SQL found for test with unique_id: {unique_id}"))
            parentNode = self.__get_parent_manifest_node (manifestJson, nodeKey)
            schema = parentNode["schema"]
            relation = parentNode["name"]
            
            #Logger.debug (f"{unique_id} - {database}.{schema}.{relation} - {sql_filename} - {query_path}")
            
            manifest[unique_id] = DataHealthReport.ManifestEntry (database, schema, relation, sql_filename, query_path, sql)
        return manifest


    @post_execution_output (logLevel=LogLevel.INFO)
    def __retrieve_data(self, jsonObject) -> HealthReport:
        """Extraction of relevant data from dbt testing json"""
        healthReport = HealthReport (None, Stats(None, list[DatabaseStats]), list[Error]())
        
        manifestMap = self.__create_manifest_map ()
        Logger.debug (Pretty.assemble_simple (f"Number of nodes in manifestMap: {len (manifestMap)}"))

        duckdb.sql("create table test_entry(database_name varchar, schema_name varchar, relation_name varchar, test_name varchar, unique_id varchar, result varchar)")
        duckdb.sql("create table error(database_name varchar, schema_name varchar, relation_name varchar, test_name varchar, unique_id varchar, sql_filename varchar, rows_on_error integer, rows_in_relation integer, query_path varchar, sql varchar)")

        for entry in jsonObject['entries']:
            if entry["info"]["name"] == "LogTestResult":
                test_name = entry["data"]["node_info"]["node_name"]
                unique_id = entry["data"]["node_info"]["unique_id"]
                sql_filename = entry["data"]["node_info"]["node_path"]
                query_path = manifestMap[unique_id].query_path
                relation_name = manifestMap[unique_id].relation
                database_name = manifestMap[unique_id].database
                schema_name = manifestMap[unique_id].schema
                sql = manifestMap[unique_id].sql
                result = entry["data"]["status"]

                duckdb.sql(f"insert into test_entry values ('{database_name}', '{schema_name}', '{relation_name}', '{test_name}', '{unique_id}', '{result}')")
                
                if result == "fail": # Error -> Skoða að hafa bara eina töflu fyrir bæði, mismunandi select upp en annars eins
                    errors = entry["data"]["num_failures"]
                    rowsInRelation = self.__retrieve_relation_cardinality (database_name, schema_name, relation_name)
                    safeSql = sql.replace('\'', '#|#')
                    duckdb.sql(f"insert into error values ('{database_name}', '{schema_name}', '{relation_name}', '{test_name}', '{unique_id}', '{sql_filename}', {errors}, {rowsInRelation}, '{query_path}', '{safeSql}')")

            if entry["info"]["name"] == "MainReportVersion": 
                headerExecution = HeaderExecution (entry["info"]["ts"], entry["info"]["invocation_id"])
                databaseName = Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')
                healthReport.header = Header (databaseName, (entry["data"])["version"].replace ("=", ""), headerExecution)     

        return healthReport
    
    def __generate_errors (self) -> list[Error]:
        errors = []
        for e in duckdb.sql ("SELECT database_name, schema_name, relation_name, test_name, unique_id, sql_filename, rows_on_error, round ((rows_on_error * 100) / rows_in_relation, 4), rows_in_relation, query_path, sql FROM error ORDER BY database_name, schema_name, relation_name, test_name").fetchall ():
            errors.append (Error (e[0], e[1], e[2], e[3], e[4], e[5], CountPercentage(e[6], e[7]), e[8], e[9], e[10]))
        return errors

    @post_execution_output (logLevel=LogLevel.INFO)
    def generate_data (self) -> None:
        """Generating data health report data"""
        jsonObject = self.__retrieve_json_object () 
        apiHealth = self.__retrieve_data (jsonObject)
 
        apiHealth.stats = self.__create_stats ()
        apiHealth.errors = self.__generate_errors ()

        jsonData = json.dumps (apiHealth, indent=4, cls=Json.EnhancedJSONEncoder)
        Utils.write_file (jsonData, Config.apiDataHealthReportDataFileInfo.qualified_name)
        return 

    @post_execution_output (logLevel=LogLevel.INFO)
    def generate_report (self) -> None:
        """Generating data health report"""
        Utils.generate_markdown_document ("api_data_health_report_template.md", Config.apiDataHealthReportDataFileInfo.name, self._reportFilename)
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    def publish (self) -> None:
        """Publishing data health report"""
        self._targetKnowledgeBase.publish (self._reportFilename, 'data-health-report') 
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def generate (self) -> None:
        """Producing a data health report"""

        if Config['documentation']['data-health-report']['generate'] != True:
            return
    
        Logger.debug (Pretty.assemble_simple (f"Project name:  {self._projectName}"))
        Logger.debug( Pretty.assemble (value=f"Relative path: {self._projectRelativePath}", tabCount=Pretty.Indent+1))

        self.generate_data ()
        self.generate_report ()
        self.publish ()
        return

def main ():
    return DataHealthReport ().generate ()

if __name__ == '__main__':
    main ()