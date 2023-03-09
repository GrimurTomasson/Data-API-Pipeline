import os
import sys
import argparse

from .Shared.Decorators import output_headers, execution_time
from .Shared.Config import Config
from .Shared.Utils import Utils
from .Shared.Logger import Logger

class Latest:
    _argParser = argparse.ArgumentParser (prog='Latest.py', description='Creates new relations and tests them.')
    _argParser.add_argument ('operation', choices=['build', 'test'])

    def __init__ (self) -> None:
        return
        
    @output_headers
    @execution_time
    def refresh (self):
        """Running dbt to refresh models and data (Latest)"""
        dbtOperation = Utils.add_dbt_profile_location (["dbt", "run", "--fail-fast"]) #  --fail-fast / --full-refresh
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        return

    @output_headers
    @execution_time
    def run_tests (self):
        """Running dbt tests"""
        dbtOperation = Utils.add_dbt_profile_location (["dbt", "--log-format", "json",  "test"])
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
    else:
        Latest._argParser.print_help ()

if __name__ == '__main__':
    main (sys.argv[1:]) # Getting rid of the filename