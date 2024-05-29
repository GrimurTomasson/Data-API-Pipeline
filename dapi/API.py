import argparse

from sys import argv

from .Shared.LogLevel import LogLevel
from .Shared.Logger import Logger
from .Shared.Decorators import post_execution_output
from .Shared.AuditDecorators import audit

from .Cleanup import Cleanup
from .Dependencies import Dependencies
from .Latest import Latest
from .Snapshot import Snapshot
from .DataHealthReport import DataHealthReport
from .MetadataCatalog import MetadataCatalog
from .DefinitionHealthReport import DefinitionHealthReport
from .Documentation import Documentation

class API:
    
    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def generate (self) -> None:
        """API pipeline run"""
        
        self.generate_data_only ()
        
        Latest ().run_tests () # Skrifar skrá: 1
        MetadataCatalog ().enrich () # Skrifar skrár: 2, 3, 4
        DataHealthReport ().generate () # Skrifar skrá: 5
        DefinitionHealthReport ().generate () # Skrifar skrá: 6
        Documentation ().generate () # Skrifar skrá: 7
        return
    

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def generate_data_only (self) -> None:
        """API pipeline run, limited to only creating data (useful for multi-instance api)"""
        
        Cleanup ().cleanup ()
        Dependencies ().update_all ()
        Dependencies ().test_all ()
        Latest ().refresh ()
        Latest ().snapshot ()
        Latest ().generate_docs ()
        Snapshot ().create() # Creates current state snapshots, removes re-run data and creates and extends snapshot tables as needed. Creates snapshot views, does not maintain them.    
        return

if __name__ == '__main__':
    API ().generate ()


