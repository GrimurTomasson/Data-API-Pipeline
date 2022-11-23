import os
import subprocess
import pyodbc
import yaml
import time
from datetime import timedelta
from dataclasses import dataclass
import dataclasses
import json
from jinja2 import Environment, FileSystemLoader

import TargetDatabaseInterface
from TargetDatabase_SQLServer import TargetDatabase_SQLServer

@dataclass
class FileInfo:
    name: str
    path: str
    qualified_name: str

supportedDatabases = ['SQL-Server']
maxConfigVersion = float(1.99999)
separator = "-" * 120

def get_file_info(name, path) -> FileInfo:
    return FileInfo (name, path, os.path.join(path, name))

def initialize() -> None:
    if 'config' in globals():
        return

    print ('Initializing APISupport')
    global workingDirectory
    workingDirectory = os.getcwd ()
    print (f"\nWorking directory: {workingDirectory}")
    
    get_config ()

    # Verbose output support 
    global print_v
    print_v = get_verbose_print ()
    print (f"Verbose: {config['verbose']}")
    get_verbose_print ()

    print_v (f"\n{config}\n")

    get_target_database_interface ()

    # Setting globals to simplify the rest of the code
    global scriptDirectory
    scriptDirectory = os.path.realpath (os.path.dirname (__file__))
    print (f"Script directory: {scriptDirectory}")

    global reportTemplateDirectory
    reportTemplateDirectory = f"{scriptDirectory}/../shared_report_templates/"
    print (f"Report template directory: {reportTemplateDirectory}")

    global runFileDirectory
    runFileDirectory = os.path.join (workingDirectory, config['data-api-pipeline']['data-file-location'])
    print (f"Run file directory: {runFileDirectory}")

    print ("") #newline

    global dbt_test_output_file_info
    dbt_test_output_file_info = get_file_info ('1_dbt_test_output.json', runFileDirectory)

    global api_data_health_report_data_file_info
    api_data_health_report_data_file_info = get_file_info ('2_api_data_health_report_data.json', runFileDirectory)

    global latest_path
    latest_path = f"{workingDirectory}{config['latest']['relative-path']}"

    global enriched_dbt_catalog_file_info
    enriched_dbt_catalog_file_info = get_file_info ('5_enriched_dbt_catalog.json', runFileDirectory) # manifest.json = 3, catalog.json = 4

    global api_definition_health_report_data_file_info
    api_definition_health_report_data_file_info = get_file_info ('6_api_definition_health_report_data.json', runFileDirectory)

    global api_documentation_data_file_info
    api_documentation_data_file_info = get_file_info ('7_api_documentation_data.json', runFileDirectory)

    return

def get_verbose_print () -> any:
    return print if config["verbose"] else lambda *a, **k: None

def process_config () -> any:
    global config
    try:
        with open (f"{workingDirectory}/api_config.yml", "r", encoding="utf8") as stream:
            config = yaml.safe_load (stream)
        
        if float (config["version"]) > maxConfigVersion:
            print (f"Config version not supported, max 1.x, config version: {config['version']}")
            raise
    except Exception as ex:
            print (f"Error in config retrieval: {ex}")
            raise
    # validation, is everything we need included?
    return config

def get_config () -> any:
    if 'config' in globals():
        return config

    return process_config ()

def process_target_database_interface () -> TargetDatabaseInterface:
    global targetDatabaseInterface 
    supportedDatabase = False
    targetDatabaseName = config['database']['type']
    for database in supportedDatabases:
        if targetDatabaseName == database:
            supportedDatabase = True
    
    if not supportedDatabase:
        print(f"Database in config is not support by pipeline. Config: {config['database']['type']}. Supported databases: {supportedDatabases}")
        raise
    if targetDatabaseName == 'SQL-Server':    
        targetDatabaseInterface = TargetDatabase_SQLServer()
    return targetDatabaseInterface

def get_target_database_interface() -> TargetDatabaseInterface:
    if 'targetDatabaseInterface' in globals():
        return targetDatabaseInterface
    return process_target_database_interface ()

def get_database_connection (database_server, database_name) -> any:
    print_v (f"Creating a DB connection to: {database_server} - {database_name}")
    conn = pyodbc.connect ('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+database_server+';DATABASE='+database_name+';Trusted_Connection=yes;')
    conn.autocommit = True
    return conn

def render_jinja_template (jinjaTemplateFilename, qualifiedJsonFilename, templateDirectory) -> any:
    environment = Environment (loader = FileSystemLoader (templateDirectory))
    template = environment.get_template (jinjaTemplateFilename)
    with open (qualifiedJsonFilename, encoding="utf-8") as json_file:
        testResults = json.load (json_file)
    return template.render (testResults)

def generate_markdown_document (templateFilename, jsonDataFilename, targetFilename, templateNotShared=False) -> None:

    qualifiedDataFilename = os.path.join (runFileDirectory, jsonDataFilename)
    if templateNotShared == True:
        templateDirectory = workingDirectory
    else:
        templateDirectory = reportTemplateDirectory
    print_v (f"GenerateHealthReport:\n\tTemplate filename: {templateFilename}\n\tTemplate directory: {templateDirectory}\n\tJson data filename: {qualifiedDataFilename}\n\tWorking directory: {workingDirectory}\n\tTarget filename: {targetFilename}\n")
    #
    report = render_jinja_template (templateFilename, qualifiedDataFilename, templateDirectory)
    write_file (report, os.path.join (workingDirectory, targetFilename))
    print (f"Markdown document has been generated!")
    return

def get_execution_time_in_seconds(start_time) -> float:
    return timedelta(seconds=get_current_time() - start_time).total_seconds()

def get_current_time() -> float:
    return time.monotonic()

def to_percentage(teljari, nefnari, aukastafir = 2) -> int:
    if nefnari == 0:
        return 0
    else:
        return round ((teljari / nefnari)*100, aukastafir)

def get_file_contents (filename) -> str:
    with open (filename, mode="r", encoding="utf-8") as f:
        return f.read ()

def write_file (contents, filename) -> None:
    with open (filename, mode="w", encoding="utf-8") as f:
        f.write (contents)
    return

class EnhancedJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if dataclasses.is_dataclass(o):
                return dataclasses.asdict(o)
            return super().default(o)