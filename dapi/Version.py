import os
import sys
import argparse
import requests
import re
import subprocess

from importlib.metadata import version

from .Shared.BasicDecorators import execution_output
from .Shared.PrettyPrint import Pretty

class Version:

    def __init__ (self) -> None:
        self._branchReplacementToken = "#replace#"
        self._packageName = 'dapi'
        self._masterBranchName = 'Master'

    def check (self, git_branch) -> None:
        uriMaster = "https://raw.githubusercontent.com/GrimurTomasson/Data-API-Pipeline/master/pyproject.toml"
        uriBranch = f"https://raw.githubusercontent.com/GrimurTomasson/Data-API-Pipeline/{self._branchReplacementToken}/pyproject.toml"
        if not git_branch is None and len (git_branch) > 0:
            uri = uriBranch.replace(self._branchReplacementToken, git_branch)
            branchName = git_branch
        else:
            uri = uriMaster
            branchName = self._masterBranchName

        toml = requests.session ().get (uri).text
        versionString = re.search ("version = \"[0-9]\.[0-9][0-9]\"", toml)
        if versionString is None:
            print (f"No version was found for branch {branchName}!")
            return
        current = re.search ("[0-9]\.[0-9][0-9]", versionString.group ()).group ()
        installed = version(self._packageName)
        upToDate = str (current == installed)

        print (f"Installed version: {installed}\nCurrent version: {current}\nBranch: {branchName}\nUp to date: {upToDate}")
        return 
    
    def _execute (self, cmd):
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for stdout_line in iter(popen.stdout.readline, ""):
            yield stdout_line 
        popen.stdout.close()
        return_code = popen.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, cmd)
        
    def update (self, git_branch) -> None:
        if self.check (git_branch) == True:
            print ("Your version is up to date!")
            return
        
        uri = "git+https://github.com/GrimurTomasson/Data-API-Pipeline.git"
        if not git_branch is None and len (git_branch) > 0:
            uri = f"{uri}@{git_branch}"

        for output in self._execute(['pip', 'install', '-U', uri, "--upgrade"]):
            print (output, end="")
        return

def main():
    argParser = argparse.ArgumentParser (
        prog='dapi-version', 
        description='Helper to check for version updates and perform updates.',
        formatter_class=argparse.RawTextHelpFormatter)

    argParser.add_argument ('operation', 
                            choices=['check', 'update', 'for-update'],
                            help='' )
    
    argParser.add_argument ('-b', '--git_branch', required=False, help='The name of the git branch used for this installation, if left empty we use the Master branch.')
    options = argParser.parse_args (sys.argv[1:]) # Getting rid of the filename

    if options.operation == "check":
        Version ().check (options.git_branch)
    elif options.operation == "update":
        Version ().update (options.git_branch)
    else:
        argParser.print_help()

if __name__ == '__main__':
    main ()