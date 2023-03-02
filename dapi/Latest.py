import sys
import argparse

from .Shared.Decorators import output_headers, execution_time
from .Shared.Config import Config
from .Shared.Utils import Utils
from .Shared.Logger import Logger

class Latest:
    _argParser = argparse.ArgumentParser (prog='Latest.py', description='Creates new relations and tests them.')
    _argParser.add_argument ('operation', choices=['build', 'test', 'deps'])

    def __init__ (self) -> None:
        return
        
    @output_headers
    @execution_time
    def update_dependencies (self):
        """Update dbt dependencies"""
        dbtOperation = ["dbt", "deps"] 
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        return

    @output_headers
    @execution_time
    def refresh (self):
        """Running dbt to refresh models and data (Latest)"""
        dbtOperation = ["dbt", "run", "--fail-fast"] #  --fail-fast / --full-refresh
        dbtOperation = Utils.add_dbt_profile_location (dbtOperation)

        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        return

    @output_headers
    @execution_time
    def run_tests (self):
        """Running dbt tests"""
        dbtOperation = ["dbt", "--log-format", "json",  "test"]
        dbtOperation = Utils.add_dbt_profile_location (dbtOperation)

        output = Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation, True)
        # ToDo: Meta hvort við ætlum að stoppa eða ekki, skoða config breytu?
        Logger.debug (f"\tOutput for dbt test results: {Config.dbtTestOutputFileInfo.qualified_name}")
        Utils.write_file (output.stdout, Config.dbtTestOutputFileInfo.qualified_name)
        return

def main (args):
    options = Latest._argParser.parse_args (args)
    if options.operation == 'build':
        return Latest ().refresh ()
    elif options.operation == 'test':
        return Latest ().run_tests ()
    elif options.operation == 'deps':
        return Latest ().update_dependencies ()

if __name__ == '__main__':
    main (sys.argv[1:]) # Getting rid of the filename