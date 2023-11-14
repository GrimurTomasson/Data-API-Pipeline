import sys
import os
import argparse

from importlib.metadata import version

from .Shared.Environment import Environment
from .Shared.Config import Config

from .API import API
from .Cleanup import Cleanup
from .Dependencies import Dependencies
from .Latest import Latest
from .Snapshot import Snapshot
from .DataHealthReport import DataHealthReport
from .MetadataCatalog import MetadataCatalog
from .DefinitionHealthReport import DefinitionHealthReport
from .Documentation import Documentation

from .Shared.Audit import Audit
from .Shared.Logger import Logger
from .Shared.PrettyPrint import Pretty
from colorama import Fore

argParser = argparse.ArgumentParser (
    prog='dapi', 
    description='Data API pipeline.',
    formatter_class=argparse.RawTextHelpFormatter)

argParser.add_argument ('operation', 
                        choices=['build', 'build-data', 'cleanup', 'dependencies', 'refresh', 'test', 'history', 'data-health-report', 'enrich-metadata', 'definition-health-report', 'documentation'], 
                        help='''
Aside from build & cleanup, the following steps require that previous steps, from cleanup forward have been run. 
The steps generate data files, so a single step can be run multiple times as long as those files exist.
The only parameter for operations is en environment file, which is optional.
                        
0. build                        Runs the following steps.
1. cleanup                      Removes run files from previous runs.
2. dependencies                 Makes sure we have dbt plugins installed and a connection to the DB.
3. refresh                      Creates relations (tables & views) from the dbt models.
4. history                      Creates a history snapshot of the results from refresh.
5. test                         Runs dbt tests.
6. data-health-report           Generates a markdown data health report and publishes it.
7. enrich-metadata              Enriches metadata using a concept glossary and target database metadata.
8. definition-health-report     Generates a markdown API definition health report, using the enriched metadata and publishes it.
9. documentation                Generates markdown end-user documentation and publishes it.
                                ''')
argParser.add_argument ('-e', '--environment', required=False, help='Select an environment file to load.')
argParser.add_argument('-d', '--dbt_run_parameters', required=False, type=str, help='Add any dbt parameters for the run command.')
argParser.add_argument('-v', '--version', action='version', version=f"dapi version: {version('dapi')}")

def main ():
    options = argParser.parse_args (sys.argv[1:]) # Getting rid of the filename
    print (Pretty.assemble (value=f"\nRunning dapi version: {version('dapi')}\n", color=Fore.GREEN, prefixWithIndent=False))
    print (Pretty.assemble (value=Pretty.get_postfix_line ("Initialization starts"), color=Fore.LIGHTBLACK_EX, prefixWithIndent=False))
    
    Logger()
    Config() 
    
    # Overriding the environment, multi-instance support
    envFile = options.environment if options.environment != None and len (options.environment) > 0 else Environment.environmentVariableFilename
    Environment.load (envFilename=envFile)

    #os.environ[Environment.dbtRunParameters] = options.dbt_run_parameters if options.dbt_run_parameters != None else []
    #Config[Environment.dbtRunParameters] = ['1', '2']
    Config.add(Environment.dbtRunParameters, options.dbt_run_parameters.split(' ') if options.dbt_run_parameters != None else [])
    Audit() # Initialize audit variables

    print (Pretty.assemble (value=Pretty.get_postfix_line ("Initialization ends") + "\n", color=Fore.LIGHTBLACK_EX, prefixWithIndent=False))

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