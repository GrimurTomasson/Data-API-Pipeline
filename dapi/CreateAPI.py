import os
import sys
import argparse
import shutil
import subprocess

from .Shared.BasicDecorators import execution_output

class CreateApi:

    def __init__ (self) -> None:
        self._packageName = 'dapi'
        self._configName = 'api_config.yml'
        self._dbt_profile = 'profiles.yml'
        self._env_file = 'dapi.env'
        self._current = 'nustada'
        self._implementationTemplates = os.path.join(self._packageName, 'ImplementationTemplates')

        self._templates = ['api_documentation_template.md', 'api_config.yml', 'dapi.env', 'load_env.ps1', 'set_terminal.ps1']
        
        self._workingDir = os.getcwd ()
        self._dataApiPath = os.path.join (self._workingDir, self._data_api)
        self._superPath, self._workingDirName = os.path.split (self._workingDir)
        return

    @execution_output
    def __copy_templates (self) -> None:
        for template in self._templates:
            qualifiedFilename = os.path.join (self._templateDir, template)
            qualifiedTarget = os.path.join (self._workingDir, template)

            shutil.copy2 (qualifiedFilename, qualifiedTarget)
            print (f"\t{template} copied to {self._workingDir}")
        return
    
    @execution_output
    def __copy_profile (self) -> None:
        qualifiedFilename = os.path.join (self._templateDir, self._dbt_profile)
        qualifiedTarget = os.path.join (self._dataApiPath, self._dbt_profile)
        print (f"Source file: {qualifiedFilename}")
        print (f"Target file: {qualifiedTarget}")
        shutil.copy2 (qualifiedFilename, qualifiedTarget)

    @execution_output
    def __process_file (self, path, filename, databaseName, databaseServer, databasePort, databaseNamePrivate) -> None:
        print (f"File: {filename}")
        qualifiedFilename = os.path.join (path, filename)
        with open (qualifiedFilename, mode="r", encoding="utf-8") as f:
            file = f.read ()

        # Við breytum þessu með strengjaleikfimi frekar en yml til þess að tapa ekki athugasemdum í config!
        databaseNamePrivate = databaseNamePrivate if databaseNamePrivate is not None else ''
        file = file.replace ('?DATABASE_NAME_PRIVATE', databaseNamePrivate)
        file = file.replace ('?DATABASE_NAME', databaseName)
        file = file.replace ('?DATABASE_SERVER', databaseServer)
        file = file.replace ('?DATABASE_PORT', databasePort if databasePort is not None else '')
        
        with open (qualifiedFilename, mode="w", encoding="utf-8") as f:
            f.write (file)
        return

    @execution_output
    def __process_config (self, databaseName, databaseServer, databasePort, databaseNamePrivate) -> None:
        return self.__process_file (self._workingDir, self._configName, databaseName, databaseServer, databasePort, databaseNamePrivate)    
    
    @execution_output
    def __process_env (self, databaseName, databaseServer, databasePort, databaseNamePrivate) -> None:
        return self.__process_file (self._workingDir, self._env_file, databaseName, databaseServer, databasePort, databaseNamePrivate)    
    
    @execution_output
    def __create_dbt_project (self) -> None:
        subprocess.run (['dbt', 'init', self._current])
        
    @execution_output
    def generate (self, databaseName, databaseServer, databasePort, databaseNamePrivate) -> None:
        print (f"\nWorking directory: {self._workingDir}")
        
        pipelineLocation = os.path.join(self._workingDir, '.venv\Lib\site-packages') # Finna með leit?
        self._templateDir = os.path.abspath (os.path.join (pipelineLocation, self._implementationTemplates))
        print (f"Template directory: {self._templateDir}\n")
        
        self.__create_dbt_project ()
        self.__copy_profile ()
        self.__copy_templates ()
        self.__process_config (databaseName, databaseServer, databasePort, databaseNamePrivate)
        self.__process_env (databaseName, databaseServer, databasePort, databaseNamePrivate)

        print ("Run set_terminal.ps1 to set up the environment and then dapi to see what your options are.")
        return

def main():
    argParser = argparse.ArgumentParser (prog='dapi', 
                                         description='Data API pipeline creation. Run this in the project folder you already created.', 
                                         formatter_class=argparse.RawTextHelpFormatter,
                                         epilog='''
    Instructions
    -----------------------------------------------------------------------------------------------------
    1. Create a project folder and perform the following steps in a terminal (PowerShell) in that folder.
    2. To find the highest version Python, we need at least 3.11, run: 
        py -0p, .
    3. Create a virtual environment for Python, called .venv. 
        Example: c:\"Program files"\python311\python.exe -m venv .venv
    4. Activate the virtual environment, run: 
        .\.venv\Scripts\Activate.ps1
    5. Install dapi in the virtual environment, run:
        pip install -U git+https://github.com/GrimurTomasson/Data-API-Pipeline
    6. Run create-dapi with parameters.
    -----------------------------------------------------------------------------------------------------''')
    
    argParser.add_argument ('-d', '--databaseName', required=True, help='Database name.')
    argParser.add_argument ('-s', '--databaseServer', required=True, help='Database server.')
    argParser.add_argument ('-p', '--databasePort', required=False, help='Database port, if not default.')
    argParser.add_argument ('-r', '--databaseNamePrivate', required=False, help='Database name for the private database.')
    options = argParser.parse_args (sys.argv[1:]) # Getting rid of the filename

    # print (sys.argv)
    # print (options.databaseNamePrivate)

    CreateApi ().generate (options.databaseName, options.databaseServer, options.databasePort, options.databaseNamePrivate)

if __name__ == '__main__':
    main ()