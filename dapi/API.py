import argparse

from sys import argv

from .Shared.Decorators import output_headers, execution_time

from .Cleanup import Cleanup
from .Dependencies import Dependencies
from .Latest import Latest
from .Snapshot import Snapshot
from .DataHealthReport import DataHealthReport
from .MetadataCatalog import MetadataCatalog
from .DefinitionHealthReport import DefinitionHealthReport
from .Documentation import Documentation

class API:
    
    @output_headers
    @execution_time
    def generate (self) -> None:
        """API pipeline run"""
        
        Cleanup ().cleanup ()
        Dependencies ().update_all ()
        Latest ().refresh ()
        Snapshot ().create() # Creates current state snapshots, removes re-run data and creates and extends snapshot tables as needed. Creates snapshot views, does not maintain them.    
        Latest ().run_tests () # Skrifar skrá: 1
        DataHealthReport ().generate () # Skrifar skrá: 2
        MetadataCatalog ().enrich () # Skrifar skrár: 3, 4, 5
        DefinitionHealthReport ().generate () # Skrifar skrá: 6
        Documentation ().generate () # Skrifar skrá: 7
        return

if __name__ == '__main__':
    API ().generate ()


