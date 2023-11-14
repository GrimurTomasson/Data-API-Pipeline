import os
import sys
import argparse
import re
import subprocess

from .Shared.BasicDecorators import execution_output
from .Shared.PrettyPrint import Pretty

class Update:

    def __init__ (self) -> None:
        return
    
    def _execute (self, cmd):
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for stdout_line in iter(popen.stdout.readline, ""):
            yield stdout_line 
        popen.stdout.close()
        return_code = popen.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, cmd)
        
    def prepare_for_update (self, postfix="old") -> None:
        # Find installation path using pip
        for output in self._execute (['pip', 'show', 'dapi']):
            location = re.search ("Location: .*", output)
            if location is not None:
                pipPath = re.search (": .*", output).group().replace(': ', '').replace('site-packages', 'Scripts')

        # Rename file so we can update.
        source_file = os.path.join (pipPath, "dapi-version.exe")
        if not os.path.exists (source_file):
            return # no work needed.

        target_file = os.path.join (pipPath, f"dapi-version-{postfix}.exe")
        os.replace (source_file, target_file)
        return

def main():
    argParser = argparse.ArgumentParser (
        prog='dapi-prepare', 
        description='Helper for version updates.',
        formatter_class=argparse.RawTextHelpFormatter)

    argParser.add_argument ('operation', 
                            choices=['for-update'],
                            help='' )

    options = argParser.parse_args (sys.argv[1:]) # Getting rid of the filename

    if options.operation == "for-update":
        Update ().prepare_for_update ()
    else:
        argParser.print_help()

if __name__ == '__main__':
    main ()