import shutil
import re
import subprocess

from .Shared.BasicDecorators import execution_output
from .Shared.PrettyPrint import Pretty

class PatchDbt:
    def patch(self):
        # Find installation paths using pip
        for output in self._execute (['pip', 'show', 'dapi']):
            location = re.search ("Location: .*", output)
            if location is not None:
                dapiPipPath = re.search (": .*", output).group().replace(': ', '') + '/dapi/DbtPatch'

        # Find installation path using pip
        for output in self._execute (['pip', 'show', 'dbt-core']):
            location = re.search ("Location: .*", output)
            if location is not None:
                dbtPipPath = re.search (": .*", output).group().replace(': ', '') + '/dbt'
        
        print (Pretty.assemble_simple (f"Source folder: {dapiPipPath}"))
        print (Pretty.assemble_simple (f"Target folder: {dbtPipPath}"))
               
        shutil.move (dapiPipPath, dbtPipPath)
        
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