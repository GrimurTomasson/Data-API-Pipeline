import os
import yaml
from colorama import Fore, init

from Shared.PrettyPrint import Pretty

class ConfigBase:
    _maxConfigVersion = float (1.99999)
    workingDirectory = os.getcwd ()
    init () #colorama init, fixes colored output on Windows (Skoða að setja þetta framar í init keðjuna)

    @staticmethod
    def process_config () -> any:
        """Reading config from disk"""
        Pretty.print (ConfigBase.process_config.__doc__, True, True)
        try:
            qualifiedConfig = os.path.join(ConfigBase.workingDirectory, "api_config.yml")
            with open (qualifiedConfig, "r", encoding="utf8") as stream:
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
