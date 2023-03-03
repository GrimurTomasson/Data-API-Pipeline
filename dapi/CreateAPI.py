import os
import shutil

from .Shared import Utils

class CreateApi:

    def __init__ (self) -> None:
        self._packageName = 'dapi'
        self._configName = 'api_config.yml'
        self._implementationTemplates = os.path.join(self._packageName, 'ImplementationTemplates')

        self._templates = ['api_documentation_template.md', 'api_config.yml', 'data-api-pipeline.env']

        self._workingDir = os.getcwd ()
        self._superPath, self._workingDirName = os.path.split (self._workingDir)

        return

    def __find_package (self) -> str:
        operation = ['pip3', 'show', self._packageName]
        packageInfo = Utils.run_operation (self._workingDir, self._workingDir, operation, True)
        print (packageInfo)
        return 'smu'

    def __copy_templates (self) -> None:
        print ("Copying template files")
        for template in self._templates:
            qualifiedFilename = os.path.join (self._templateDir, template)
            qualifiedTarget = os.path.join (self._workingDir, template)

            shutil.copy2 (qualifiedFilename, qualifiedTarget)
            print (f"\t{template} copied to {self._workingDir}")
        return

    def __process_config (self) -> None:
        print (f"\nEditing {self._configName}")
        qualifiedConfig = os.path.join (os.path.join (self._pipelineDirectory, self._implementationTemplates), self._configName)

        with open (qualifiedConfig, mode="r", encoding="utf-8") as f:
            config = f.read ()

        # Við breytum þessu með strengjaleikfimi frekar en yml til þess að tapa ekki athugasemdum í config!
        config = config.replace ('?SOME-API', self._workingDirName)
        print (f"\tDatabase name: {self._workingDirName}")

        targetConfig = os.path.join (os.getcwd (), self._configName)
        with open (targetConfig, mode="w", encoding="utf-8") as f:
            f.write (config)
        print ("\nUpdated config written")
        return
        

    def generate (self) -> None:
        print (f"\nWorking directory: {self._workingDir}")
        print (f"Working directory-, API-, Database name -> {self._workingDirName}")

        print (f"\nLooking for the {self._packageName} package location")
        pipelineLocation = self.__find_package ()
        if len (pipelineLocation) == 0:
            print (f"We cannot find the location of the {self._packageName} package!")
            exit(1)

        pipelineDirectory = os.path.abspath (os.path.join (pipelineLocation, '..'))
        print (f"{self._packageName} found at: {pipelineDirectory}\n")

        templateDir = os.path.abspath (os.path.join (pipelineDirectory, self._implementationTemplates))
        print (f"Template directory: {templateDir}\n")

        self.__copy_templates ()
        self.__process_config ()

        print ("\nAll done! Remember to edit the config!\n")
        return

if __name__ == '__main__':
    CreateApi ().generate ()