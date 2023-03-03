import sys
import argparse

from .API import API
from .Cleanup import Cleanup
from .Dependencies import Dependencies
from .Latest import Latest
from .Snapshot import Snapshot
from .DataHealthReport import DataHealthReport
from .MetadataCatalog import MetadataCatalog
from .DefinitionHealthReport import DefinitionHealthReport
from .Documentation import Documentation

from .Environment import Environment

argParser = argparse.ArgumentParser (prog='command_line.py', description='API pipeline command line interface.')
argParser.add_argument ('operation', choices=['build', 'cleanup', 'dependencies', 'refresh', 'test', 'history', 'data-health-report', 'enrich-metadata', 'definition-health-report', 'documentation'])

def main ():
    options = argParser.parse_args (sys.argv[1:]) # Getting rid of the filename
    if options.operation == 'build':
        API ().generate ()
    elif options.operation == 'cleanup':
        Cleanup ().cleanup ()    
    elif options.operation == 'dependencies':
        Dependencies ().update_all ()
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

def environment ():
    Environment ().run_operation ()