import sys
import argparse

from .Shared.Decorators import output_headers, execution_time
from .Shared.Config import Config
from .Shared.Utils import Utils

class Dependencies:
    _argParser = argparse.ArgumentParser (prog='Dependencies.py', description='Updates dependencies.')
    _argParser.add_argument ('operation', choices=['dbt'])

    def __init__ (self) -> None:
        return
        
    @output_headers
    @execution_time
    def update_dbt (self):
        """Update dbt dependencies"""
        dbtOperation = ["dbt", "deps"] 
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        return
    
    def update_all (self):
        self.update_dbt ()
    
def main ():
    options = Dependencies._argParser.parse_args (sys.argv[1:]) # Getting rid of the filename
    if options.operation == 'dbt':
        return Dependencies ().update_dbt ()
    else:
        Dependencies ().update_all ()

if __name__ == '__main__':
    main () 