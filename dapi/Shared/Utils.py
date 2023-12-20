import os
import subprocess
import json
import copy
from jinja2 import Environment, FileSystemLoader
from colorama import Fore
from dataclasses import field

from .Config import Config
from .Logger import Logger
from .PrettyPrint import Pretty

class Utils:
    @staticmethod 
    def render_jinja_template (jinjaTemplateFilename, qualifiedJsonFilename, templateDirectory) -> any:
        environment = Environment (loader = FileSystemLoader (templateDirectory))
        template = environment.get_template (jinjaTemplateFilename)
        with open (qualifiedJsonFilename, encoding="utf-8") as json_file:
            testResults = json.load (json_file)
        return template.render (testResults)

    @staticmethod 
    def generate_markdown_document (templateFilename, jsonDataFilename, targetFilename, metadataCsv, templateNotShared=False) -> None:
        qualifiedDataFilename = os.path.join (Config.runFileDirectory, jsonDataFilename)
        if templateNotShared == True:
            templateDirectory = Config.workingDirectory
        else:
            templateDirectory = Config.reportTemplateDirectory

        message = Pretty.assemble (value=f"{Utils.generate_markdown_document.__qualname__}:\n", color=Fore.LIGHTBLUE_EX, tabCount=Pretty.Indent)
        message += Pretty.assemble (value="Template filename:  " + templateFilename + "\n", tabCount=Pretty.Indent+1)
        message += Pretty.assemble (value="Template directory: " + templateDirectory + "\n", tabCount=Pretty.Indent+1)
        message += Pretty.assemble (value="Json data filename: " + qualifiedDataFilename + "\n", tabCount=Pretty.Indent+1)
        message += Pretty.assemble (value="Working directory:  " + Config.workingDirectory + "\n", tabCount=Pretty.Indent+1)
        message += Pretty.assemble (value="Target filename:    " + targetFilename + "\n", tabCount=Pretty.Indent+1)
        message += Pretty.assemble (value="Metadata csv:       " + metadataCsv if metadataCsv is not None else 'None' + "\n", tabCount=Pretty.Indent+1)
        Logger.debug (message)
        
        prefix = metadataCsv.replace (',', '\n') + "\n" if metadataCsv is not None else str()
        report = prefix + Utils.render_jinja_template (templateFilename, qualifiedDataFilename, templateDirectory)
        
        Utils.write_file (report, os.path.join (Config.workingDirectory, targetFilename))
        Logger.info (Pretty.assemble_simple (f"Markdown document {targetFilename} has been generated!"))
        return

    @staticmethod
    def to_percentage(teljari, nefnari, aukastafir = 2) -> int:
        if nefnari == 0:
            return 0
        else:
            return round ((teljari / nefnari)*100, aukastafir)

    @staticmethod
    def get_file_contents (filename) -> str:
        Logger.debug (Pretty.assemble_simple (f"Reading file: {filename}"))
        with open (filename, mode="r", encoding="utf-8") as f:
            return f.read ()

    @staticmethod
    def write_file (contents, filename) -> None:
        Logger.debug (Pretty.assemble_simple (f"Writing file: {filename}"))
        with open (filename, mode="w", encoding="utf-8") as f:
            f.write (contents)
        return

    @staticmethod
    def run_operation (startingLocation, location, operation, captureOutput = False):
        operation_string = str.join(' ', operation)
        Logger.info (Pretty.assemble (value=f"\n{Pretty.get_postfix_line ('Operation output starts')}", color=Fore.LIGHTBLUE_EX, prefixWithIndent=False))
        Logger.info (Pretty.assemble (value=f"{operation_string}\n", color=Fore.LIGHTMAGENTA_EX, prefixWithIndent=False))
        
        Logger.debug (Pretty.assemble (value=f"Starting location: {startingLocation}", prefixWithIndent=False))
        Logger.debug (Pretty.assemble (value=f"Location:          {location}", prefixWithIndent=False))
        
        os.chdir (location)
        output = subprocess.run (operation, capture_output=captureOutput, text=True)
        os.chdir (startingLocation)
        Logger.info (Pretty.assemble (value=f"{Pretty.get_postfix_line ('Operation output ends')}\n", color=Fore.LIGHTBLUE_EX, prefixWithIndent=False))
        if output.returncode != 0 and not captureOutput: # We should not throw if the caller needs output to decide what to do
            raise Exception(f"\nError:\n\treturn value: {output.returncode}\n\trunning:\n{operation_string}")
        return output

    @staticmethod
    def default_field(obj):
        return field(default_factory=lambda: copy.deepcopy(obj))
    
    @staticmethod
    def add_dbt_profile_location (operation):
        profileFile = os.path.join (Config.workingDirectory, 'profiles.yml')
        if os.path.isfile (profileFile):
            operation.append (f"--profiles-dir")
            operation.append (Config.workingDirectory)
        return operation
    
    @staticmethod
    def retrieve_variable (description, envVarName, configRoot, configVarName, optional=False):
        if os.environ.get(envVarName) is not None and len (os.environ.get(envVarName)) > 0:
            value = os.environ.get(envVarName)
            if not envVarName.find("PASSWORD"):
                Logger.debug (Pretty.assemble_simple (f"{description} overwritten from environment: {value}"))
            return value
        
        if configVarName in configRoot.keys():
            return configRoot[configVarName]    
        
        if optional is True:
            return str('') # None endar sem strengur í environment ofl.
        
        raise (f"{description} value was neither found in environment variables nor config!")
    
    @staticmethod
    def environment_variable_with_value (envVarName) -> bool:
        return os.environ.get(envVarName) is not None and len (os.environ.get(envVarName)) > 0
            