import os
import yaml
from dataclasses import dataclass
from colorama import init, Fore, Style 

from Shared.Decorators import output_headers
from Shared.PrettyPrint import Pretty

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

class Config:
    _maxConfigVersion = float (1.99999)
   
    init () #colorama init, fixes colored output on Windows
    workingDirectory = os.getcwd ()

    def __init__ (self) -> None:    
        if hasattr (Config, '_config'):
            return
        Config._config = Config.__process_config ()
        Config._verbose = Config['verbose']
        Config.__generate_path_variables ()
        Config.__print_contents ()
        return

    def __class_getitem__ (cls, key):
        return Config._config[key]
    
    @staticmethod 
    def __print_contents ():
        if Config._verbose != True:
            return
        Pretty.print ("Config variable values", True, True)
        for attribute, value in Config.__dict__.items():
            Pretty.print (f"\n{attribute}", False, False, Fore.CYAN)
            print (value)
        print ("\n" + Pretty.Separator + "\n")
        return
 
    @staticmethod
    @output_headers
    def __process_config () -> any:
        """Reading config from disk"""
        try:
            qualifiedConfig = os.path.join(Config.workingDirectory, "api_config.yml")
            with open (qualifiedConfig, "r", encoding="utf8") as stream:
                config = yaml.safe_load (stream)
            
            if float (config["version"]) > Config._maxConfigVersion:
                error = f"Config version not supported, max: {Config._maxConfigVersion}, config version: {config['version']}"
                Pretty.print (error, True, True, Fore.RED)
                raise Exception (error)
        except Exception as ex:
                error = f"Error in config retrieval: {ex}"
                Pretty.print (error, True, True, Fore.RED)
                raise Exception (error)
        # validation, is everything we need included?
        return config

    @staticmethod
    def __generate_path_variables () -> None:
        # Keeping the paths all here simplifies the solution, even if they don't all come from config.
        Config.scriptDirectory = os.path.abspath (os.path.join (os.path.dirname (__file__), '..'))
        Config.reportTemplateDirectory = os.path.abspath (os.path.join (Config.scriptDirectory, "../shared_report_templates/"))
        Config.runFileDirectory = os.path.abspath (os.path.join (Config.workingDirectory, Config['data-api-pipeline']['data-file-location']))
        Config.latestPath = os.path.abspath (os.path.join (Config.workingDirectory, Config['latest']['relative-path']))

        Config.dbtTestOutputFileInfo = Config.__get_file_info ('1_dbt_test_output.json', Config.runFileDirectory)
        Config.apiDataHealthReportDataFileInfo = Config.__get_file_info ('2_api_data_health_report_data.json', Config.runFileDirectory)
        Config.enrichedDbtCatalogFileInfo = Config.__get_file_info ('5_enriched_dbt_catalog.json', Config.runFileDirectory) # manifest.json = 3, catalog.json = 4
        Config.apiDefinitionHealthReportDataFileInfo = Config.__get_file_info ('6_api_definition_health_report_data.json', Config.runFileDirectory)
        Config.apiDocumentationDataFileInfo = Config.__get_file_info ('7_api_documentation_data.json', Config.runFileDirectory)
        return

    @staticmethod
    def __get_file_info (name, path) -> FileInfo:
        return FileInfo (name, path, os.path.join (path, name))

Config () # Constuction run, safety feature