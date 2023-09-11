import os
import yaml
from colorama import Fore, init

from .PrettyPrint import Pretty
from .BasicDecorators import execution_output

class ConfigBase:
    _maxConfigVersion = float (1.99999)
    workingDirectory = os.getcwd ()
    configFilename = "api_config.yml"
    qualifiedConfig = os.path.join(workingDirectory, configFilename)
    init () #colorama init, fixes colored output on Windows (Skoða að setja þetta framar í init keðjuna)
    _parsedConfig = None

    @staticmethod
    def process_config () -> any:
        """Reading config from disk"""
        if ConfigBase._parsedConfig is not None:
            return ConfigBase._parsedConfig
        
        return ConfigBase.__process_config ()
    
    @staticmethod 
    @execution_output
    def __process_config () -> any:
        try:
            with open (ConfigBase.qualifiedConfig, "r", encoding="utf8") as stream:
                ConfigBase._parsedConfig = yaml.safe_load (stream)
            
            if float (ConfigBase._parsedConfig["version"]) > ConfigBase._maxConfigVersion:
                error = f"Config version not supported, max: {ConfigBase._maxConfigVersion}, config version: {ConfigBase._parsedConfig['version']}"
                Pretty.print (error, True, True, Fore.RED)
                raise Exception (error)
        except Exception as ex:
                error = f"Error in config retrieval: {ex}"
                Pretty.print (error, True, True, Fore.RED)
                raise Exception (error)
        # validation, is everything we need included?
        return ConfigBase._parsedConfig