import os
import subprocess
import json
import copy
from jinja2 import Environment, FileSystemLoader
from colorama import Fore
from dataclasses import field

from Shared.Config import Config
from Shared.Logger import Logger
from Shared.PrettyPrint import Pretty

class Utils:
    @staticmethod 
    def render_jinja_template (jinjaTemplateFilename, qualifiedJsonFilename, templateDirectory) -> any:
        environment = Environment (loader = FileSystemLoader (templateDirectory))
        template = environment.get_template (jinjaTemplateFilename)
        with open (qualifiedJsonFilename, encoding="utf-8") as json_file:
            testResults = json.load (json_file)
        return template.render (testResults)

    @staticmethod 
    def generate_markdown_document (templateFilename, jsonDataFilename, targetFilename, templateNotShared=False) -> None:
        qualifiedDataFilename = os.path.join (Config.runFileDirectory, jsonDataFilename)
        if templateNotShared == True:
            templateDirectory = Config.workingDirectory
        else:
            templateDirectory = Config.reportTemplateDirectory

        header = Pretty.assemble (f"{Utils.generate_markdown_document.__qualname__}:\n", False, False, Fore.LIGHTBLUE_EX, 0, 1)
        message = header + (    "\t\tTemplate filename:  " + templateFilename + "\n"
                                "\t\tTemplate directory: " + templateDirectory + "\n"
                                "\t\tJson data filename: " + qualifiedDataFilename + "\n"
                                "\t\tWorking directory:  " + Config.workingDirectory + "\n"
                                "\t\tTarget filename:    " + targetFilename + "\n")
        Logger.info (message)
        
        report = Utils.render_jinja_template (templateFilename, qualifiedDataFilename, templateDirectory)
        Utils.write_file (report, os.path.join (Config.workingDirectory, targetFilename))
        Logger.info (f"\tMarkdown document has been generated!")
        return

    @staticmethod
    def to_percentage(teljari, nefnari, aukastafir = 2) -> int:
        if nefnari == 0:
            return 0
        else:
            return round ((teljari / nefnari)*100, aukastafir)

    @staticmethod
    def get_file_contents (filename) -> str:
        Logger.debug (f"\n\tReading file: {filename}")
        with open (filename, mode="r", encoding="utf-8") as f:
            return f.read ()

    @staticmethod
    def write_file (contents, filename) -> None:
        Logger.debug (f"\n\tWriting file: {filename}")
        with open (filename, mode="w", encoding="utf-8") as f:
            f.write (contents)
        return

    @staticmethod
    def run_operation (startingLocation, location, operation, captureOutput = False):
        header = Pretty.assemble (f"{Utils.run_operation.__qualname__}\n", False, False, Fore.LIGHTMAGENTA_EX, 0, 1)
        message = header + (f"\t\t - Starting location: {startingLocation}\n"
                            f"\t\t - Location:          {location}\n"
                            f"\t\t - Operation:         {operation}\n")
        Logger.info (message)
        os.chdir (location)
        output = subprocess.run (operation, capture_output=captureOutput, text=True)
        os.chdir (startingLocation)
        if output.returncode != 0 and not captureOutput: # We should not throw if the caller needs output to decide what to do
            raise Exception(f"\nError:\n\treturn value: {output.returncode}\n\trunning:\n{message}")
        return output

    @staticmethod
    def default_field(obj):
        return field(default_factory=lambda: copy.copy(obj))
    
    @staticmethod
    def add_dbt_profile_location (operation):
        if Config.dbtProfilePath != None:
            operation.append (f"--profiles-dir")
            operation.append (Config.dbtProfilePath)
        return operation