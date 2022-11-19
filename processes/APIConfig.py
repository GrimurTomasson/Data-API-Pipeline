import os
import subprocess
import yaml

maxConfigVersion = float(1.99999)

def getVerbosePrint():
    return print if config["verbose"] else lambda *a, **k: None

def retrieveConfig():
    workingDirectory = os.getcwd()
    print(f"\nWorking directory: {workingDirectory}")
    global config
    try:
        with open(f"{workingDirectory}/api_config.yml", "r", encoding="utf8") as stream:
            config = yaml.safe_load(stream)
        #
        if float(config["version"]) > maxConfigVersion:
            print(f"Config version not supported, max 1.x, config version: {config['version']}")
        # Verbose output support 
        global v_print
        v_print = getVerbosePrint()
        print(f"Verbose: {config['verbose']}")
        #
        v_print(f"\n{config}\n")
    except Exception as ex:
            print(f"Error in config retrieval: {ex}")
            return False
    return True

# validation, is everything we need included?