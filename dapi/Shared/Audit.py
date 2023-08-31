import os
import uuid
import shutil
import json

from importlib.metadata import version
from getpass import getuser
from socket import gethostname

from .Utils import Utils
from .Environment import Environment
from .Config import Config
from .Logger import Logger
from ..TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory
from ..TargetDatabase.TargetDatabase import TargetDatabase, dapi_invocation, dbt_invocation, dbt_action

class Audit:
    def __init__ (self) -> None:
        Audit.enabled =  'audit' in Config._config and 'enabled' in Config['audit']
        if Audit.enabled == False:
            Logger.debug ("Audit is disabled !!!")
            return
        
        Audit.database = Utils.retrieve_variable ('Audit database name', Environment.auditDatabaseName, Config['audit'], 'database')
        Audit.schema = Utils.retrieve_variable ('Audit database schema name', Environment.auditDatabaseSchema, Config['audit'], 'schema')
        Audit.enabled = Config['audit']['enabled'] == True and len (Audit.database) > 0 and len (Audit.schema) > 0 
        if Audit.enabled == False:
            Logger.debug ("Audit is disabled !!!")
            return
        
        Audit.user = getuser()
        Audit.host = gethostname()
        
        # Make sure audit tables exist
        Audit.targetDatabase = TargetDatabaseFactory ().get_target_database ()
        Audit.targetDatabase.set_connection (Audit.database)
    
        Audit.targetDatabase.create_schema_if_missing (Audit.schema)
        Audit.targetDatabase.create_table_if_missing (Audit.schema, dapi_invocation.__name__, dapi_invocation) 
        Audit.targetDatabase.create_table_if_missing (Audit.schema, dbt_invocation.__name__, dbt_invocation	)
        Audit.targetDatabase.create_table_if_missing (Audit.schema, dbt_action.__name__, dbt_action)
        
        Audit.id = str (uuid.uuid4())
        Audit.version = version("dapi")
        Audit.model_to_relation_map = {}
        return
    
    @staticmethod
    def dapi (start_time, operation, execution_time_in_seconds):
        if Audit.enabled == False:
            return
        if not hasattr (Audit, 'database'):
            Audit()
            
        Audit.targetDatabase.insert_dataclass (Audit.database, Audit.schema, dapi_invocation.__name__, dapi_invocation (Audit.id, Audit.database, operation, execution_time_in_seconds, Audit.version, Audit.user, Audit.host, start_time))
        return
    
    @staticmethod
    def __get_model_to_relation_map (filename) -> {}:
        modelMap = {}
        with open (filename, encoding="utf-8") as json_file:
            manifestJson = json.load (json_file)
            
        for relationKey in manifestJson['nodes']:
            node = manifestJson['nodes'][relationKey]
            modelMap[node['unique_id']] = node['relation_name'].replace ('"', '')
            
        return modelMap
    
    @staticmethod
    def dbt (start_time, operation):
        if Audit.enabled == False:
            return
        if not hasattr (Audit, 'database'):
            Audit()
            
        # Afrita run_results.json yfir í vinnsluskráamöppu, endurskýra {operation}_run_results.json
        dbt_output_path = os.path.join (Config.latestPath, "target")
        run_results_file = os.path.join (dbt_output_path, "run_results.json")
        normalized_operation = operation.replace (".", "_")
        target_file = os.path.join (Config.runFileDirectory, f"{normalized_operation}_run_results.json")
        
        shutil.copy2 (run_results_file, target_file)
        
        # Afrita manifest yfir í vinnsluskráamöppu, endurskýra {operation}_manifest.json
        manifest_file = os.path.join (dbt_output_path, "manifest.json")
        target_manifest_file = os.path.join (Config.runFileDirectory, f"{normalized_operation}_manifest.json")
        shutil.copy2 (manifest_file, target_manifest_file)
        
        if len (Audit.model_to_relation_map) == 0:
            Audit.model_to_relation_map = Audit.__get_model_to_relation_map (target_manifest_file)
        
        # Lesa upp afritaða run-result skrá
        with open (target_file, encoding="utf-8") as json_file:
            runResults = json.load (json_file)
        
        # Vinna samantekt & skrifa
        version = runResults['metadata']['dbt_version']
        id = runResults['metadata']['invocation_id']
        execution_time = runResults['elapsed_time']
        params = str (runResults['args'])
        
        invocation = dbt_invocation (id, Audit.id, operation, execution_time, params, version, start_time )
        Audit.targetDatabase.insert_dataclass (Audit.database, Audit.schema, dbt_invocation.__name__, invocation)
        
        # Vinna nóður & skrifa
        actions = []
        for action in runResults['results']:
            unique_id = action['unique_id']
            status = action['status']
            rows_affected = action['adapter_response']['rows_affected'] if 'adapter_response' in action and 'rows_affected' in action['adapter_response'] else 0
            execution_time = action['execution_time']
            thread_id = action['thread_id']
            relation_name = Audit.model_to_relation_map[unique_id] if unique_id in Audit.model_to_relation_map else ''
            
            act = dbt_action (id, unique_id, relation_name, status, execution_time, rows_affected, thread_id)
            actions.append (act)
            
        Audit.targetDatabase.insert_dataclasses (Audit.database, Audit.schema, dbt_action.__name__, actions)
        return