import os
import shutil

import Decorators
import APISupport

from Snapshot import Snapshot
from Latest import Latest

from DataHealthReport import DataHealthReport
from MetadataCatalog import MetadataCatalog
from DefinitionHealthReport import DefinitionHealthReport
from Documentation import Documentation


@Decorators.output_headers
@Decorators.execution_time
def run_file_cleanup ():
    """Cleaning up runfiles"""
    if len (APISupport.runFileDirectory) < 15: # Við viljum ekki henda hálfu drifi út af mistökum!
        print (f"The run file directory looks suspicious, no files deleted! Run file directory: {APISupport.runFileDirectory}")
        raise

    if os.path.exists (APISupport.runFileDirectory):
        shutil.rmtree (APISupport.runFileDirectory)
        APISupport.print_v ("Run file directory deleted!")
    
    os.mkdir (APISupport.runFileDirectory)
    APISupport.print_v (f"Run file directory created at: {APISupport.runFileDirectory}")
    return

@Decorators.output_headers
@Decorators.execution_time
def generate_api () -> None:
    """API pipeline run"""
    APISupport.initialize ()
    run_file_cleanup ()
    
    Latest().refresh ()
    Snapshot().create() # Creates current state snapshots, removes re-run data and creates and extends snapshot tables as needed. Creates snapshot views, does not maintain them.    
    Latest().run_tests () # Skrifar skrá: 1
    
    DataHealthReport ().generate ()
    MetadataCatalog ().enrich () # Skrifar skrár: 3, 4, 5
    DefinitionHealthReport ().generate () # Skrifar skrá: 6
    Documentation ().generate () # Skrifar skrá: 7
    return

def main ():
    return generate_api ()

if __name__ == '__main__':
    main ()