import os

from dotenv import load_dotenv
from colorama import init, Fore

from .PrettyPrint import Pretty
from .Logger import Logger

class Environment:
    databaseServer = str("DAPI_DATABASE_SERVER")
    databasePort = str("DAPI_DATABASE_PORT")
    databaseName = str("DAPI_DATABASE_INSTANCE")
    databaseUser = str("DAPI_DATABASE_USER")
    databasePassword= str("DAPI_DATABASE_PASSWORD")

    # For Confluence, only required if you publish to Confluence.
    markUser = str("DAPI_MARK_USER")
    markPassword = str("DAPI_MARK_PASSWORD")
    markBaseUri = str("DAPI_MARK_BASE_URI")

    environmentVariableFilename = str("dapi.env") # Default value

    @staticmethod 
    def load (relativePath = None, envFilename = None) -> None:
        filepath = os.path.join (os.getcwd(), relativePath) if relativePath != None and len (relativePath) > 0 else os.getcwd()
        filename = envFilename if envFilename != None and len (envFilename) > 0 else Environment.environmentVariableFilename
        qualifiedName = os.path.join (filepath, filename)

        if not os.path.isfile (qualifiedName):
            print (f"Environment file not found, qualified name: {qualifiedName}")
            return

        loadType = "" if relativePath == None and envFilename == None else "overridden"
        Logger.debug (Pretty.assemble (f"\nLoading {loadType} environment variables - {qualifiedName}\n", False, False, Fore.CYAN))

        load_dotenv (dotenv_path=qualifiedName, verbose=True, override=True) 

        dapiEnvVarKeys = [x for x in os.environ.keys() if x.startswith('DAPI_') and x.find("PASSWORD") == -1]
        for e in dapiEnvVarKeys:
            Logger.debug (f"{e} = " + Pretty.assemble (f"{os.environ[e]}", False, False, Fore.LIGHTGREEN_EX))
        Logger.debug ("")

        return