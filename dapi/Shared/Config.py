import os
from dataclasses import dataclass
from colorama import init, Fore, Style 

from dotenv import load_dotenv

from .ConfigBase import ConfigBase
from .PrettyPrint import Pretty
from .Logger import Logger

@dataclass
class FileInfo:
    name: str
    path: str
    qualified_name: str

    def __str__(self):
        return (f"name:           {self.name}\n"
                f"path:           {self.path}\n"
                f"qualified_name: {self.qualified_name}"
        )

class Config (ConfigBase):
    _config = ConfigBase.process_config ()
    logLevel = Logger.logLevel # Upp á config variable logging

    def __init__ (self) -> None:    
        if hasattr (Config, 'apiDocumentationDataFileInfo'): # Það síðasta sem __generate_path_variables býr til
            return
            
        load_dotenv (dotenv_path=os.path.join (self.workingDirectory, 'data-api-pipeline.env'), verbose=True, override=True) # .env file in working folder!

        Config.__generate_path_variables ()
        Config.__override_values_from_environment ()

        self.__print_contents ()
        return

    def __class_getitem__ (cls, key):
        return Config._config[key]

    @staticmethod
    def __print_contents ():
        Logger.debug (Pretty.assemble ("Config variable values", True, True))
        attributes = Config.get_attributes (Config)
        for key, value in attributes.items(): 
            Logger.debug (Pretty.assemble (f"\n{key}", False, False, Fore.CYAN))
            Logger.debug (str (value))
        Logger.debug ("\n" + Pretty.Separator + "\n")
        return

    @staticmethod
    def get_attributes (object):
        return { k:v for (k,v) in object.__dict__.items() if not k.startswith ('__') and not callable (getattr (object, k)) }
 
    @staticmethod
    def __override_values_from_environment () -> None:
        databaseServer = 'DAPI_DATABASE_SERVER'
        if os.environ.get(databaseServer) is not None and len (os.environ.get(databaseServer)) > 0:
            Config['database']['server'] = os.environ.get(databaseServer)
            Logger.debug (f"Database server overwritten from environment: {Config['database']['server']}")
        
        databaseInstance = 'DAPI_DATABASE_INSTANCE'
        if os.environ.get(databaseInstance) is not None and len (os.environ.get(databaseInstance)) > 0:
            Config['database']['name'] = os.environ.get(databaseInstance)
            Logger.debug (f"Database instance overwritten from environment: {Config['database']['name']}")

    @staticmethod
    def __generate_path_variables () -> None:
        # Keeping the paths all here simplifies the solution, even if they don't all come from config.
        Config.scriptDirectory = os.path.abspath (os.path.join (os.path.dirname (__file__), '..'))
        Config.reportTemplateDirectory = os.path.abspath (os.path.join (Config.scriptDirectory, "../dapi/Templates/"))
        Config.runFileDirectory = os.path.abspath (os.path.join (Config.workingDirectory, Config['data-api-pipeline']['data-file-location']))
        Config.latestPath = os.path.abspath (os.path.join (Config.workingDirectory, Config['latest']['relative-path']))

        Config.dbtTestOutputFileInfo = Config.__get_file_info ('1_dbt_test_output.json', Config.runFileDirectory)
        Config.apiDataHealthReportDataFileInfo = Config.__get_file_info ('2_api_data_health_report_data.json', Config.runFileDirectory)
        Config.enrichedDbtCatalogFileInfo = Config.__get_file_info ('5_enriched_dbt_catalog.json', Config.runFileDirectory) # manifest.json = 3, catalog.json = 4
        Config.apiDefinitionHealthReportDataFileInfo = Config.__get_file_info ('6_api_definition_health_report_data.json', Config.runFileDirectory)
        Config.apiDocumentationDataFileInfo = Config.__get_file_info ('7_api_documentation_data.json', Config.runFileDirectory)

        Config.dbtProfilePath = Config.__find_file_location ('profiles.yml', Config.workingDirectory )
        return

    @staticmethod
    def __get_file_info (name, path) -> FileInfo:
        return FileInfo (name, path, os.path.join (path, name))

    @staticmethod
    def __find_file_location(name, path):
        for root, dirs, files in os.walk(path):
            if name in files:
                return root
        return None

Config() # Initialization