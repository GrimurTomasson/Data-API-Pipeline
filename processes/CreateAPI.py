import os
import shutil
import platform
import yaml

configName = 'api_config.yml'
pipelineProcesses = 'Data-API-Pipeline/processes'
implementationTemplates = 'implementation_templates'

templates = ['api_documentation_template.md', 'GenerateAPI.py']

pathDelimiter = '/'
if platform.system() == 'Windows':
    pathDelimiter = '\\'

workingDir = os.getcwd ()
superPath, workingDirName = os.path.split (workingDir)

def directory_scan (path):
    for entry in os.scandir(path):
        try:
            if entry.is_dir (follow_symlinks=False):
                yield from directory_scan (entry.path)
                yield entry
        except OSError as error:
            pass #print (f"directory_scan error: {error}")

def find_directory(path, endsWith):
    """Find a directory whose path contains a pattern."""
    endsWith = endsWith.replace ('\\', pathDelimiter)
    endsWith = endsWith.replace ('/', pathDelimiter)

    for entry in directory_scan (path):
        if str (entry.path).endswith (endsWith):
            return entry.path
    return ""

def copy_templates () -> None:
    print ("Copying template files")
    for template in templates:
        qualifiedFilename = os.path.join (templateDir, template)
        qualifiedTarget = os.path.join (workingDir, template)
        shutil.copy2 (qualifiedFilename, qualifiedTarget)
        print (f"\t{template} copied to {workingDir}")
    return

def process_config () -> None:
    print (f"\nEditing {configName}")
    qualifiedConfig = os.path.join (os.path.join (pipelineDirectory, implementationTemplates), configName)
    relativePath = os.path.relpath (pipelineDirectory, os.getcwd ())

    with open (qualifiedConfig, mode="r", encoding="utf-8") as f:
        config = f.read ()

    # Við breytum þessu með strengjaleikfimi frekar en yml til þess að tapa ekki athugasemdum í config!
    config = config.replace ('?/Data-API-Pipeline/processes', relativePath)
    print (f"\tRelative path to pipeline repo: {relativePath}")

    config = config.replace ('?SOME-API', workingDirName)
    print (f"\tDatabase name: {workingDirName}")

    targetConfig = os.path.join (os.getcwd (), configName)
    with open (targetConfig, mode="w", encoding="utf-8") as f:
        f.write (config)
    print ("\nUpdated config written")
    return
        
rootDirectory = os.path.abspath (os.sep)

print (f"\nWorking directory: {workingDir}")
print (f"Working directory-, API-, Database name -> {workingDirName}")

print ("\nLooking for the Data-API-Pipeline repo")
pipelineLocation = find_directory (rootDirectory, pipelineProcesses)
if len (pipelineLocation) == 0:
    print ("We cannot find the location of the Data-API-Pipeline repo!")
    exit(1)

pipelineDirectory = os.path.abspath (os.path.join (pipelineLocation, '..'))
print (f"Data-API-Pipeline found at: {pipelineDirectory}\n")

templateDir = os.path.abspath (os.path.join (pipelineDirectory, implementationTemplates))
print (f"Template directory: {templateDir}\n")

copy_templates ()
process_config ()

print ("\nAll done! Remember to edit the config!\n")