import os
import subprocess
import pyodbc
import yaml
import time
from datetime import timedelta
import dataclasses
import json
from jinja2 import Environment, FileSystemLoader

import TargetDatabaseInterface
from TargetDatabase_SQLServer import TargetDatabase_SQLServer

supportedDatabases = ['SQL-Server']
maxConfigVersion = float(1.99999)
separator = "-" * 120

def get_verbose_print () -> any:
    return print if config["verbose"] else lambda *a, **k: None

def get_config () -> any:
    workingDirectory = os.getcwd ()
    print (f"\nWorking directory: {workingDirectory}")
    global config
    try:
        with open (f"{workingDirectory}/api_config.yml", "r", encoding="utf8") as stream:
            config = yaml.safe_load (stream)
        #
        if float (config["version"]) > maxConfigVersion:
            print (f"Config version not supported, max 1.x, config version: {config['version']}")
            raise
        # Verbose output support 
        global print_v
        print_v = get_verbose_print ()
        print (f"Verbose: {config['verbose']}")
        #
        print_v (f"\n{config}\n")
    except Exception as ex:
            print (f"Error in config retrieval: {ex}")
            raise
    # validation, is everything we need included?
    return config

def get_target_database_interface() -> TargetDatabaseInterface:
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
    workingDirectory = os.getcwd ()

    qualifiedDataFilename = f"{workingDirectory}/{jsonDataFilename}"
    if templateNotShared == True:
        templateDirectory = workingDirectory
    else:
        templateDirectory = f"{os.path.dirname (os.path.realpath (__file__))}/templates/" # APISupport.py verður að vera á sama stað og template mappan fyrir jinja templates ef um samnýtt sniðmát er að ræða!
    print_v (f"GenerateHealthReport:\n\tTemplate filename: {templateFilename}\n\tJson data filename: {qualifiedDataFilename}\n\tWorking directory: {workingDirectory}\n\tTarget filename: {targetFilename}\n")
    #
    report = render_jinja_template (templateFilename, qualifiedDataFilename, templateDirectory)
    write_file (report, f"{workingDirectory}/{targetFilename}")
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

def generate_count_and_percentage_dictionary (teljari, nefnari, aukastafir=2) -> dict[str, any]:
    return {'count':teljari, 'percentage':to_percentage (teljari, nefnari, aukastafir) }

class EnhancedJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if dataclasses.is_dataclass(o):
                return dataclasses.asdict(o)
            return super().default(o)