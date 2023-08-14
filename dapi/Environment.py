import os
import sys
import shutil
import argparse
import yaml

from .Shared.ConfigBase import ConfigBase
from .Shared.EnvironmentVariable import EnvironmentVariable

class Environment:
    _argParser = argparse.ArgumentParser (prog='Environment.py', description='Updates config files for multi-instance APIs.')
    _argParser.add_argument ('operation', choices=['set', 'restore'])
    _argParser.add_argument ('-d', '--database', required=False)
    _argParser.add_argument ('-p', '--dbtProfile', required=False)

    def __init__ (self) -> None:
        self._workingDirectory = os.getcwd ()

        self._currentConfig = ConfigBase.qualifiedConfig
        self._backedUpConfig = f"{self._currentConfig}.backup"
        
        self._currentDbtProject = self._get_dbt_project_path ()
        self._backedUpDbtProject = f"{self._currentDbtProject}.backup"

    def _set_database (self, databaseName: str) -> None:
        config = ConfigBase.process_config ()
        config['database']['name'] = databaseName
        os.environ[EnvironmentVariable.databaseName] = databaseName # When the next iteration is run, for the default environment, this is overwritten when config is read.

        shutil.copy2 (self._currentConfig, self._backedUpConfig)

        with open (ConfigBase.qualifiedConfig, 'w', encoding='utf-8') as configFile:
            yaml.dump (config, configFile)
        return

    def _get_dbt_project_path (self) -> str:
        config = ConfigBase.process_config ()
        dbtCurrentPath = os.path.join (self._workingDirectory, config['latest']['relative-path'])
        dbtCurrentPath = os.path.join (dbtCurrentPath, "dbt_project.yml")
        return dbtCurrentPath

    def _set_dbt_profile (self, profileName: str) -> None:
        shutil.copy2 (self._currentDbtProject, self._backedUpDbtProject)

        with open (self._currentDbtProject, mode='r', encoding='utf-8') as dbtProjectFile:
            dbtProject = yaml.safe_load (dbtProjectFile)

        dbtProject['profile'] = profileName

        with open (self._currentDbtProject, mode='w', encoding='utf-8') as dbtProjectFile:
            yaml.dump (dbtProject, dbtProjectFile)
        return

    def set (self, database, dbtProfile) -> None:
        if database != None and len (database) > 0:
            self._set_database (database)

        if dbtProfile != None and len (dbtProfile) > 0:
            self._set_dbt_profile (dbtProfile)

    def restore (self) -> None:
        if os.path.exists (self._backedUpConfig):
            shutil.copy2 (self._backedUpConfig, self._currentConfig)
            os.remove (self._backedUpConfig)

        if os.path.exists (self._backedUpDbtProject):
            shutil.copy2 (self._backedUpDbtProject, self._currentDbtProject)
            os.remove (self._backedUpDbtProject)

        print ('All config files have been restored!\n')
        return


    def run_operation ():
        options = Environment._argParser.parse_args (sys.argv[1:]) # Getting rid of the filename
        print(options)

        if options.operation == 'set':
            Environment ().set (options.database, options.dbtProfile)
        if options.operation == 'restore':
                Environment ().restore ()

def main():
    Environment ().run_operation ()

if __name__ == '__main__':
    main ()
