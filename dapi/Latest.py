import os
import sys
import argparse
import shutil

from .Shared.Decorators import post_execution_output
from .Shared.Config import Config
from .Shared.Utils import Utils
from .Shared.PrettyPrint import Pretty
from .Shared.LogLevel import LogLevel
from .Shared.Logger import Logger
from .Shared.Environment import Environment
from .Shared.AuditDecorators import audit, audit_dbt

class Latest:
    _argParser = argparse.ArgumentParser (prog='Latest.py', description='Creates new relations and tests them.')
    _argParser.add_argument ('operation', choices=['build', 'test'])

    def __init__ (self) -> None:
        return
    
    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    @audit_dbt
    def refresh (self):
        """Running dbt to refresh models and data (Latest)"""
        operation = ["dbt", "run", "--fail-fast"]
        operation.extend (Config[Environment.dbtRunParameters])

        dbtOperation = Utils.add_dbt_profile_location (operation) 
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        
        # Afritum manifest fyrir auditing & enrichment
        dbt_output_path = os.path.join (Config.latestPath, "target")
        source_manifest_file = os.path.join (dbt_output_path, "manifest.json")
        Logger.debug (Pretty.assemble_simple ("Manifest"))
        Logger.debug (Pretty.assemble (value=f"Source: {source_manifest_file}", tabCount=Pretty.Indent+1))
        Logger.debug (Pretty.assemble (value=f"Target: {Config.dbtManifestFileInfo.qualified_name}", tabCount=Pretty.Indent+1))
        shutil.copy2 (source_manifest_file, Config.dbtManifestFileInfo.qualified_name)
        return
    
    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    @audit_dbt
    def snapshot (self):
        """Running dbt to create type-2 snapshots of current data"""
        
        snapshotDatabase = Config['history']['snapshot-database'] if 'history' in Config._config and 'snapshot-database' in Config['history'] else None
        if snapshotDatabase is None:
            Logger.debug (Pretty.assemble_simple (f"No history->snapshot-database in config!"))    
            return
        operation = ["dbt", "snapshot", "--fail-fast"] # Ath, við notum ekki auka params hér!
        dbtOperation = Utils.add_dbt_profile_location (operation) 
        # Vísun í réttan grunn
        dbInstance = Utils.retrieve_variable (Environment.databaseName, Environment.databaseName, None, None)
        os.environ[Environment.databaseName] = snapshotDatabase
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        # Grunn vísun breytt til baka
        os.environ[Environment.databaseName] = dbInstance
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    @audit_dbt
    def run_tests (self):
        """Running dbt tests"""
        dbtOperation = Utils.add_dbt_profile_location (["dbt", "--log-format", "json",  "test"])
        output = Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation, True)
        # ToDo: Meta hvort við ætlum að stoppa eða ekki, skoða config breytu?
        Logger.debug (Pretty.assemble_simple (f"Output for dbt test results: {Config.dbtTestOutputFileInfo.qualified_name}"))
        Utils.write_file (output.stdout, Config.dbtTestOutputFileInfo.qualified_name)
        return
    
    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    @audit_dbt
    def generate_docs (self):
        """Generating dbt docs"""
        dbtOperation = Utils.add_dbt_profile_location (["dbt", "docs", "generate"])
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        
        source_path = os.path.join (Config.latestPath, "target")
        target_path = os.path.join (Config.workingDirectory, "dbt_docs")

        for file in ['manifest.json', 'catalog.json', 'index.html']:
            source_file = os.path.join (source_path, file)
            target_file = os.path.join (target_path, file)
            Logger.debug (Pretty.assemble_simple (f"Copying {file}"))
            Logger.debug (Pretty.assemble (value=f"Source: {source_file}", tabCount=Pretty.Indent+1))
            Logger.debug (Pretty.assemble (value=f"Target: {target_file}", tabCount=Pretty.Indent+1))
            shutil.copy2 (source_file, target_file)
        return


def main (args):
    options = Latest._argParser.parse_args (args)
    if options.operation == 'build':
        return Latest ().refresh ()
    elif options.operation == 'test':
        return Latest ().run_tests ()
    else:
        Latest._argParser.print_help ()

if __name__ == '__main__':
    main (sys.argv[1:]) # Getting rid of the filename