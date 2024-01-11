import os
import shutil
import re
import subprocess
import glob

from .Shared.BasicDecorators import execution_output
from .Shared.PrettyPrint import Pretty

class PatchDbt:
    def patch(self):
        # Find installation paths using pip
        for output in self._execute (['pip', 'show', 'dapi']):
            location = re.search ("Location: .*", output)
            if location is not None:
                srcDir = re.search (": .*", output).group().replace(': ', '') + '/dapi/DbtPatch'

        # Find installation path using pip
        for output in self._execute (['pip', 'show', 'dbt-core']):
            location = re.search ("Location: .*", output)
            if location is not None:
                targetDir = re.search (": .*", output).group().replace(': ', '') + '/dbt'
        
        print (Pretty.assemble_simple (f"Source folder: {srcDir}"))
        print (Pretty.assemble_simple (f"Target folder: {targetDir}"))
               
        # shutil.move (dapiPipPath, dbtPipPath)
        for p in glob.glob('**/*.*', recursive=True, root_dir=srcDir):
            os.makedirs(os.path.join(targetDir, os.path.dirname(p)), exist_ok=True)
            srcFile = os.path.join(srcDir, p)
            targetFile = os.path.join(targetDir, p)
            print (Pretty.assemble_simple (f"Source: {srcFile} - Target: {targetFile}"))
            shutil.move(srcFile, targetFile)
        
        print (Pretty.assemble_simple ("All done!"))
        return
    
    # ToDo: Second instance, move to utils!
    def _execute (self, cmd):
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for stdout_line in iter(popen.stdout.readline, ""):
            yield stdout_line 
        popen.stdout.close()
        return_code = popen.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, cmd)
    
def main (args):
    return PatchDbt ().patch ()

if __name__ == '__main__':
    main (argv[1:]) # Getting rid of the filename