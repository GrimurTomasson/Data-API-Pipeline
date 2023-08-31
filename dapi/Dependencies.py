import sys
import argparse

from .Shared.Decorators import output_headers, execution_time
from .Shared.Config import Config
from .Shared.Utils import Utils
from .TargetDatabase.TargetDatabase import Relations, Relation
from .TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory, TargetDatabase
from .Shared.AuditDecorators import audit

class Dependencies:
    _argParser = argparse.ArgumentParser (prog='Dependencies.py', description='Updates dependencies.')
    _argParser.add_argument ('operation', choices=['dbt'])

    def __init__ (self) -> None:
        return
        
    @output_headers
    @execution_time
    @audit
    def update_dbt (self):
        """Update dbt dependencies"""
        dbtOperation = ["dbt", "deps"] 
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        return
    
    @output_headers
    @execution_time
    @audit
    def test_dbt (self):
        """Test dbt setup"""
        operation = Utils.add_dbt_profile_location (['dbt', 'debug'])
        Utils.run_operation (Config.workingDirectory, Config.latestPath, operation)
        return
    
    @output_headers
    @execution_time
    @audit
    def test_db_connection (self):
        """Test db connection"""
        TargetDatabaseFactory ().get_target_database ()
        return
    
    def update_all (self):
        self.update_dbt ()

    def test_all (self):
        self.test_dbt ()
        self.test_db_connection ()
    
def main ():
    options = Dependencies._argParser.parse_args (sys.argv[1:]) # Getting rid of the filename
    if options.operation == 'dbt':
        Dependencies ().update_dbt ()
        Dependencies ().test_dbt ()
    else:
        Dependencies ().update_all ()
        Dependencies ().test_all ()

if __name__ == '__main__':
    main () 