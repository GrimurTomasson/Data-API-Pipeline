import os
from dataclasses import dataclass
from colorama import init, Fore, Style 

from .ConfigBase import ConfigBase
from .PrettyPrint import Pretty
from .Logger import Logger
from .BasicDecorators import execution_output

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
    _config = None

    def __init__ (self) -> None:    
        if Config._config is not None:
            return
        Config.initialize ()
        return

    @staticmethod 
    @execution_output
    def initialize () -> None:
        Config._config = ConfigBase.process_config ()    
        
        Config.logLevel = Config._config['log-level']
        Logger.set_log_level (Config.logLevel)
        
        Config.__generate_path_variables ()
        Config.__print_contents ()
        return

    @staticmethod
    def __init_if_needed ():
        if Config._config is None:
            Config()

    def __class_getitem__ (cls, key):
        Config.__init_if_needed()
        return Config._config[key]

    @staticmethod
    def add (key, value):
        Config.__init_if_needed()
        Config._config[key] = value

    @staticmethod
    def __print_contents ():
        Config.__init_if_needed()
        Logger.debug (Pretty.Separator)
        Logger.debug (Pretty.assemble (value="Config variable values\n", prefixWithIndent=False))
        attributes = Config.get_attributes (Config)
        for key, value in attributes.items(): 
            Logger.debug (Pretty.assemble (value=f"{key}", color=Fore.CYAN, prefixWithIndent=False))
            Logger.debug (str (value))
        Logger.debug ("\n" + Pretty.Separator)
        return

    @staticmethod
    def get_attributes (object):
        Config.__init_if_needed()
        return { k:v for (k,v) in object.__dict__.items() if not k.startswith ('__') and not callable (getattr (object, k)) }

    @staticmethod
    def __generate_path_variables () -> None:
        # Keeping the paths all here simplifies the solution, even if they don't all come from config.
        Config.scriptDirectory = os.path.abspath (os.path.join (os.path.dirname (__file__), '..'))
        Config.reportTemplateDirectory = os.path.abspath (os.path.join (Config.scriptDirectory, "../dapi/Templates/"))
        Config.runFileDirectory = os.path.abspath (os.path.join (Config.workingDirectory, Config['data-api-pipeline']['data-file-location']))
        Config.latestPath = os.path.abspath (os.path.join (Config.workingDirectory, Config['latest']['relative-path']))

        Config.dbtTestOutputFileInfo = Config.__get_file_info ('1_dbt_test_output.json', Config.runFileDirectory)
        Config.dbtManifestFileInfo = Config.__get_file_info ('2_dbt_manifest.json', Config.runFileDirectory)
        Config.dbtCatalogFileInfo = Config.__get_file_info ('3_dbt_catalog.json', Config.runFileDirectory)
        Config.enrichedDbtCatalogFileInfo = Config.__get_file_info ('4_enriched_dbt_catalog.json', Config.runFileDirectory) # manifest.json = 2, catalog.json = 3
        Config.apiDataHealthReportDataFileInfo = Config.__get_file_info ('5_api_data_health_report_data.json', Config.runFileDirectory)
        Config.apiDefinitionHealthReportDataFileInfo = Config.__get_file_info ('6_api_definition_health_report_data.json', Config.runFileDirectory)
        Config.apiDocumentationDataFileInfo = Config.__get_file_info ('7_api_documentation_data.json', Config.runFileDirectory)
        return

    @staticmethod
    def __get_file_info (name, path) -> FileInfo:
        return FileInfo (name, path, os.path.join (path, name))