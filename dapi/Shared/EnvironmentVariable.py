import os
import sys
import argparse

from dotenv import load_dotenv

class EnvironmentVariable:
    databaseServer = str("DAPI_DATABASE_SERVER")
    databasePort = str("DAPI_DATABASE_PORT")
    databaseName = str("DAPI_DATABASE_INSTANCE")
    databaseUser = str("DAPI_DATABASE_USER")
    databasePassword= str("DAPI_DATABASE_PASSWORD")

    # For Confluence, only required if you publish to Confluence.
    markUser = str("DAPI_MARK_USER")
    markPassword = str("DAPI_MARK_PASSWORD")
    markBaseUri = str("DAPI_MARK_BASE_URI")

    environmentVariableFilename = str("data-api-pipeline.env")

    _argParser = argparse.ArgumentParser (prog='EnvironmentVariable', description='Loads environment variables.')
    _argParser.add_argument ('-r', '--relative_path', required=False)

    @staticmethod 
    def load (relativePath) -> None:
        pathBase = os.getcwd()
        if relativePath != None and len (relativePath) > 0:
            pathBase = os.path.join (pathBase, relativePath)
        load_dotenv (dotenv_path=os.path.join (pathBase, EnvironmentVariable.environmentVariableFilename), verbose=True, override=True) 
        return
    
def main ():
    options = EnvironmentVariable._argParser.parse_args (sys.argv[1:]) # Getting rid of the filename
    EnvironmentVariable.load (options.relative_path)
    return

if __name__ == '__main__':
    main ()