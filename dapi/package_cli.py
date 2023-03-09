import sys
import argparse

from .Shared.Environment import Environment

from .API import API
from .Cleanup import Cleanup
from .Dependencies import Dependencies
from .Latest import Latest
from .Snapshot import Snapshot
from .DataHealthReport import DataHealthReport
from .MetadataCatalog import MetadataCatalog
from .DefinitionHealthReport import DefinitionHealthReport
from .Documentation import Documentation

argParser = argparse.ArgumentParser (
    prog='dapi', 
    description='Data API pipeline.')

argParser.add_argument ('operation', choices=['build', 'build-data', 'cleanup', 'dependencies', 'refresh', 'test', 'history', 'data-health-report', 'enrich-metadata', 'definition-health-report', 'documentation'])
argParser.add_argument ('-e', '--environment', required=False)

def main ():
    options = argParser.parse_args (sys.argv[1:]) # Getting rid of the filename

    # Overriding the environment, multi-instance support
    if options.environment != None and len (options.environment) > 0:
        Environment.load (envFilename=options.environment)

    if options.operation == 'build':
        API ().generate ()
    elif options.operation == 'build-data':
        API ().generate_data_only ()
    elif options.operation == 'cleanup':
        Cleanup ().cleanup ()    
    elif options.operation == 'dependencies':
        Dependencies ().update_all ()
        Dependencies ().test_all ()
    elif options.operation == 'refresh':
        Latest ().refresh ()
    elif options.operation == 'test':    
        Latest ().run_tests () # Skrifar skrá: 1
    elif options.operation == 'history':
        Snapshot ().create() # Creates current state snapshots, removes re-run data and creates and extends snapshot tables as needed. Creates snapshot views, does not maintain them.    
    elif options.operation == 'data-health-report':
        DataHealthReport ().generate () # Skrifar skrá: 2
    elif options.operation == 'enrich-metadata':
        MetadataCatalog ().enrich () # Skrifar skrár: 3, 4, 5
    elif options.operation == 'definition-health-report':
        DefinitionHealthReport ().generate () # Skrifar skrá: 6
    elif options.operation == 'documentation':
        Documentation ().generate () # Skrifar skrá: 7
    else:
        argParser.print_help()