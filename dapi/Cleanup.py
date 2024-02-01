import shutil
import logging

from os import path, mkdir
from sys import argv

from .Shared.Config import Config
from .Shared.LogLevel import LogLevel
from .Shared.Logger import Logger
from .Shared.PrettyPrint import Pretty
from .Shared.Decorators import post_execution_output
from .Shared.Utils import Utils
from .Shared.AuditDecorators import audit

class Cleanup:
    
    def __init__ (self) -> None:
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    def __run_file_cleanup (self):
        """Cleaning up runfiles"""
        runFileDirectory = Config.runFileDirectory
        if len (runFileDirectory) < 15: # Við viljum ekki henda hálfu drifi út af mistökum!
            message = f"The run file directory looks suspicious, no files deleted! Run file directory: {runFileDirectory}"
            Logger.error (Pretty.assemble_simple (message))
            raise Exception (message)

        if path.exists (runFileDirectory):
            shutil.rmtree (runFileDirectory)
            Logger.debug (Pretty.assemble_simple ("Run file directory deleted!"))
        
        mkdir (runFileDirectory)
        Logger.info (Pretty.assemble_simple (f"Run file directory created at: {runFileDirectory}"))
        return
    
    @post_execution_output (logLevel=LogLevel.INFO)
    def __dbt_docs_cleanup (self):
        """dbt docs cleanup"""
        dbtDocsPath = path.join(Config.workingDirectory, "dbt_docs")
        if path.exists (dbtDocsPath):
            shutil.rmtree (dbtDocsPath)
            Logger.debug (Pretty.assemble_simple ("dbt docs directory deleted!"))
        
        mkdir (dbtDocsPath)
        Logger.info (Pretty.assemble_simple (f"dbt docs directory created at: {dbtDocsPath}"))
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def cleanup (self) -> None:
        """Removes temporary (run) files created by the API Pipeline and dbt"""
        self.__run_file_cleanup ()
        self.__dbt_docs_cleanup ()
        Utils.run_operation (Config.workingDirectory, Config.latestPath, ["dbt", "clean"])

def main (args):
    return Cleanup ().cleanup()

if __name__ == '__main__':
    main (argv[1:]) # Getting rid of the filename