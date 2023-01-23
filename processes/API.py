import shutil
import logging
import argparse

from os import path, mkdir
from sys import argv

from Shared.Config import Config
from Shared.Decorators import output_headers, execution_time
from Shared.Utils import Utils

from Latest import Latest
from Snapshot import Snapshot
from DataHealthReport import DataHealthReport
from MetadataCatalog import MetadataCatalog
from DefinitionHealthReport import DefinitionHealthReport
from Documentation import Documentation

class API:
    _argParser = argparse.ArgumentParser (prog='API.py', description='Runs a data API creation pipeline.')
    _argParser.add_argument ('operation', choices=['build', 'clean'])

    @output_headers
    def __run_file_cleanup (self):
        """Cleaning up runfiles"""
        runFileDirectory = Config.runFileDirectory
        if len (runFileDirectory) < 15: # Við viljum ekki henda hálfu drifi út af mistökum!
            message = f"The run file directory looks suspicious, no files deleted! Run file directory: {runFileDirectory}"
            logging.error (message)
            raise Exception (message)

        if path.exists (runFileDirectory):
            shutil.rmtree (runFileDirectory)
            logging.info ("\tRun file directory deleted!")
        
        mkdir (runFileDirectory)
        logging.info (f"\tRun file directory created at: {runFileDirectory}")
        return

    @output_headers
    @execution_time
    def cleanup (self) -> None:
        """Removes temporary (run) files created by the API Pipeline and dbt"""
        self.__run_file_cleanup ()
        Utils.run_operation (Config.workingDirectory, Config.latestPath, ["dbt", "clean"])

    @output_headers
    @execution_time
    def generate (self) -> None:
        """API pipeline run"""
        self.cleanup ()
        
        Latest ().update_dependencies ()
        Latest ().refresh ()
        Snapshot ().create() # Creates current state snapshots, removes re-run data and creates and extends snapshot tables as needed. Creates snapshot views, does not maintain them.    
        Latest ().run_tests () # Skrifar skrá: 1
        
        if Config['documentation']['data-health-report']['generate'] == True:
            DataHealthReport ().generate () # Skrifar skrá: 2

        MetadataCatalog ().enrich () # Skrifar skrár: 3, 4, 5

        if Config['documentation']['definition-health-report']['generate'] == True:
            DefinitionHealthReport ().generate () # Skrifar skrá: 6

        if Config['documentation']['user-documentation']['generate'] == True:
            Documentation ().generate () # Skrifar skrá: 7
        return

def main (args):
    options = API._argParser.parse_args (args)
    if options.operation == 'build':
        return API ().generate ()
    return API ().cleanup ()

if __name__ == '__main__':
    main (argv[1:]) # Getting rid of the filename