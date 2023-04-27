import os
import sys
import argparse
import shutil
import subprocess

class CreateApi:

    def __init__ (self) -> None:
        self._packageName = 'dapi'
        self._configName = 'api_config.yml'
        self._implementationTemplates = os.path.join(self._packageName, 'ImplementationTemplates')

        self._templates = ['api_documentation_template.md', 'api_config.yml', 'dapi.env', 'profiles.yml', 'load_env.ps1']

        self._workingDir = os.getcwd ()
        self._superPath, self._workingDirName = os.path.split (self._workingDir)
        return

    def __find_package (self) -> str:
        operation = ['pip3', 'show', self._packageName]
        packageInfo = subprocess.run (operation, capture_output=True, text=True)
        locationTxt = "Location: "
        location = [x for x in packageInfo.stdout.split("\n") if x.startswith(locationTxt)][0].removeprefix(locationTxt)
        return location

    def __copy_templates (self) -> None:
        print ("Copying template files")
        for template in self._templates:
            qualifiedFilename = os.path.join (self._templateDir, template)
            qualifiedTarget = os.path.join (self._workingDir, template)

            shutil.copy2 (qualifiedFilename, qualifiedTarget)
            print (f"\t{template} copied to {self._workingDir}")
        return

    def __process_config (self, databaseName, databaseServer, databasePort) -> None:
        print (f"\nEditing {self._configName}")
        qualifiedConfig = os.path.join (self._workingDir, self._configName)

        with open (qualifiedConfig, mode="r", encoding="utf-8") as f:
            config = f.read ()

        # Við breytum þessu með strengjaleikfimi frekar en yml til þess að tapa ekki athugasemdum í config!
        config = config.replace ('?DATABASE_NAME', databaseName)
        print (f"\tDatabase name: {databaseName}")

        config = config.replace ('?DATABASE_SERVER', databaseServer)
        print (f"\tDatabase server: {databaseServer}")

        if databasePort is None:
            databasePort = ''
        config = config.replace ('?DATABASE_PORT', databasePort)
        print (f"\tDatabase server port: {databasePort}")
        
        targetConfig = os.path.join (os.getcwd (), self._configName)
        with open (targetConfig, mode="w", encoding="utf-8") as f:
            f.write (config)
        print ("\nUpdated config written")
        return
        
    def generate (self, databaseName, databaseServer, databasePort) -> None:
        print (f"\nWorking directory: {self._workingDir}")

        print (f"\nLooking for the {self._packageName} package location")
        pipelineLocation = self.__find_package ()
        if len (pipelineLocation) == 0:
            print (f"We cannot find the location of the {self._packageName} package!")
            exit(1)

        print (f"{self._packageName} found at: {pipelineLocation}\n")
        self._templateDir = os.path.abspath (os.path.join (pipelineLocation, self._implementationTemplates))
        print (f"Template directory: {self._templateDir}\n")

        self.__copy_templates ()
        self.__process_config (databaseName, databaseServer, databasePort)

        print ("\nAll done! Remember to edit the config!\n")
        return

def main():
    argParser = argparse.ArgumentParser (prog='dapi', description='Data API pipeline creation.', formatter_class=argparse.RawTextHelpFormatter)
    argParser.add_argument ('-d', '--databaseName', required=True, help='Database name.')
    argParser.add_argument ('-s', '--databaseServer', required=True, help='Database server.')
    argParser.add_argument ('-p', '--databasePort', required=False, help='Database port, if not default.')
    options = argParser.parse_args (sys.argv[1:]) # Getting rid of the filename

    CreateApi ().generate (options.databaseName, options.databaseServer, options.databasePort)

if __name__ == '__main__':
    main ()