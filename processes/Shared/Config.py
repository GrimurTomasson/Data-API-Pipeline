import os
import yaml
from dataclasses import dataclass

@dataclass
class FileInfo:
    name: str
    path: str
    qualified_name: str

class Config:
    _maxConfigVersion = float(1.99999)

    def __init__ (self) -> None:
        self.workingDirectory = os.getcwd ()
        if hasattr (Config, '_config'):
            self.__generate_path_variables ()
            return
        
        Config._config = self.__process_config ()
        print (f"Verbose: {self._config['verbose']}")
        self.__generate_path_variables ()
        self.__print_contents ()
        return

    def __getitem__(self, key):
        return self._config[key]

    def __print_contents(self):
        for attribute, value in self.__dict__.items():
            print (f"Attribute: {attribute} - Value: {value}")

    def __process_config (self) -> any:
        print (f"Reading config from disk")
        try:
            with open (f"{self.workingDirectory}/api_config.yml", "r", encoding="utf8") as stream:
                config = yaml.safe_load (stream)
            
            if float (config["version"]) > Config._maxConfigVersion:
                print (f"Config version not supported, max: {Config._maxConfigVersion}, config version: {config['version']}")
                raise
        except Exception as ex:
                print (f"Error in config retrieval: {ex}")
                raise
        # validation, is everything we need included?
        return config

    def __generate_path_variables (self) -> None:
        # Keeping the paths all here simplifies the solution, even if they don't all come from config.
        self.scriptDirectory = os.path.abspath (os.path.join (os.path.dirname (__file__), '..'))
        self.reportTemplateDirectory = os.path.abspath (os.path.join (self.scriptDirectory, "../shared_report_templates/"))
        self.runFileDirectory = os.path.abspath (os.path.join (self.workingDirectory, Config._config['data-api-pipeline']['data-file-location']))
        self.latestPath = os.path.abspath (os.path.join (self.workingDirectory, self._config['latest']['relative-path']))

        self.dbtTestOutputFileInfo = self.__get_file_info ('1_dbt_test_output.json', self.runFileDirectory)
        self.apiDataHealthReportDataFileInfo = self.__get_file_info ('2_api_data_health_report_data.json', self.runFileDirectory)
        self.enrichedDbtCatalogFileInfo = self.__get_file_info ('5_enriched_dbt_catalog.json', self.runFileDirectory) # manifest.json = 3, catalog.json = 4
        self.apiDefinitionHealthReportDataFileInfo = self.__get_file_info ('6_api_definition_health_report_data.json', self.runFileDirectory)
        self.apiDocumentationDataFileInfo = self.__get_file_info ('7_api_documentation_data.json', self.runFileDirectory)
        return

    def __get_file_info (self, name, path) -> FileInfo:
        return FileInfo (name, path, os.path.join (path, name))
        