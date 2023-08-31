import shutil
import logging

from os import path, mkdir
from sys import argv

from .Shared.Config import Config
from .Shared.Decorators import output_headers, execution_time
from .Shared.Utils import Utils
from .Shared.AuditDecorators import audit

class Cleanup:
    
    def __init__ (self) -> None:
        return

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
    @audit
    def cleanup (self) -> None:
        """Removes temporary (run) files created by the API Pipeline and dbt"""
        self.__run_file_cleanup ()
        Utils.run_operation (Config.workingDirectory, Config.latestPath, ["dbt", "clean"])

def main (args):
    return Cleanup ().cleanup()

if __name__ == '__main__':
    main (argv[1:]) # Getting rid of the filename