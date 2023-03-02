import os
import yaml
from colorama import Fore, init

from .PrettyPrint import Pretty

class ConfigBase:
    _maxConfigVersion = float (1.99999)
    workingDirectory = os.getcwd ()
    configFilename = "api_config.yml"
    qualifiedConfig = os.path.join(workingDirectory, configFilename)
    init () #colorama init, fixes colored output on Windows (Skoða að setja þetta framar í init keðjuna)

    @staticmethod
    def process_config () -> any:
        """Reading config from disk"""
        Pretty.print (ConfigBase.process_config.__doc__, True, True)
        try:
            with open (ConfigBase.qualifiedConfig, "r", encoding="utf8") as stream:
                config = yaml.safe_load (stream)
            
            if float (config["version"]) > ConfigBase._maxConfigVersion:
                error = f"Config version not supported, max: {ConfigBase._maxConfigVersion}, config version: {config['version']}"
                Pretty.print (error, True, True, Fore.RED)
                raise Exception (error)
        except Exception as ex:
                error = f"Error in config retrieval: {ex}"
                Pretty.print (error, True, True, Fore.RED)
                raise Exception (error)
        # validation, is everything we need included?
        return config
