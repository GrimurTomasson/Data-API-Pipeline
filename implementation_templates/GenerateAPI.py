import os
import subprocess
import sys
import yaml

# Kóða endurtekning (APISupport) til þess að koma slóð á pipeline í config, snyrtilegra gagnvart notendum.
with open (f"{os.getcwd()}/api_config.yml", "r", encoding="utf8") as stream:
    config = yaml.safe_load (stream)
pipelineScriptLocation = config['data-api-pipeline']['relative-location']
sys.path.append(f"{os.getcwd()}/{pipelineScriptLocation}") 

from Shared.Config import Config
from Shared.Utils import Utils
from API import API

def refreshPipelineScripts(workingDirectory, pipelineBranch):
    """Getting the most recent version of the pipeline"""
    scriptDir = f"{workingDirectory}/{pipelineScriptLocation}"
    Utils().print_v(f"Pipeline repo dir: {scriptDir} - Branch: {pipelineBranch}")
    os.chdir(scriptDir)
    # Pull this once by hand if you get a authentication/authorization error
    subprocess.run(["git", "checkout", pipelineBranch])
    subprocess.run(["git", "pull"])
    os.chdir(workingDirectory)
    return

def main():
    workingDirectory = os.getcwd()
    Utils().print_v(f"Starting location: {workingDirectory}")
    # Retrieve the most recent version of the pipeline scripts
    pipelineBranch = Config["data-api-pipeline"]["git-branch-name"]
    if len (pipelineBranch) > 0:
        refreshPipelineScripts(workingDirectory, pipelineBranch)
    
    API ().generate()
    os.chdir(workingDirectory)
    return 0

if __name__ == '__main__':
    main()

# Forskilyrði
#   pip install pyyaml
#   pip install pyodbc