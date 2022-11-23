import os
import subprocess
import sys
import yaml

# Kóða endurtekning (APISupport) til þess að koma slóð á pipeline í config, snyrtilegra gagnvart notendum.
with open (f"{os.getcwd()}/api_config.yml", "r", encoding="utf8") as stream:
    config = yaml.safe_load (stream)
pipelineScriptLocation = config['data-api-pipeline']['relative-location']
sys.path.append(f"{os.getcwd()}/{pipelineScriptLocation}") 

import APISupport
import GenerateAPIShared

def refreshPipelineScripts(workingDirectory, pipelineBranch):
    scriptDir = f"{workingDirectory}/{pipelineScriptLocation}"
    APISupport.print_v(f"Pipeline repo dir: {scriptDir} - Branch: {pipelineBranch}")
    os.chdir(scriptDir)
    # Pull this once by hand if you get a authentication/authorization error
    subprocess.run(["git", "checkout", pipelineBranch])
    subprocess.run(["git", "pull"])
    os.chdir(workingDirectory)
    return

def main():
    APISupport.get_config() # Fá villuna strax ef þetta er ekki í lagi
    
    workingDirectory = os.getcwd()
    APISupport.print_v(f"Starting location: {workingDirectory}")
    # Retrieve the most recent version of the pipeline scripts
    pipelineBranch = APISupport.config["data-api-pipeline"]["git-branch-name"]
    if len (pipelineBranch) > 0:
        refreshPipelineScripts(workingDirectory, pipelineBranch)
    
    GenerateAPIShared.run()
    os.chdir(workingDirectory)
    return 0

if __name__ == '__main__':
    main()

# Forskilyrði
#   pip install pyyaml
#   pip install pyodbc