import os
import subprocess
import json
from jinja2 import Environment, FileSystemLoader

from Shared.Config import Config

class Utils:
    def __init__(self):
        self._config = Config ()
        # Verbose output support 
        self.print_v = print if self._config["verbose"] else lambda *a, **k: None
        return

    def render_jinja_template (self, jinjaTemplateFilename, qualifiedJsonFilename, templateDirectory) -> any:
        environment = Environment (loader = FileSystemLoader (templateDirectory))
        template = environment.get_template (jinjaTemplateFilename)
        with open (qualifiedJsonFilename, encoding="utf-8") as json_file:
            testResults = json.load (json_file)
        return template.render (testResults)

    def generate_markdown_document (self, templateFilename, jsonDataFilename, targetFilename, templateNotShared=False) -> None:

        qualifiedDataFilename = os.path.join (self._config.runFileDirectory, jsonDataFilename)
        if templateNotShared == True:
            templateDirectory = self._config.workingDirectory
        else:
            templateDirectory = self._config.reportTemplateDirectory
        self.print_v (f"GenerateHealthReport:\n\tTemplate filename: {templateFilename}\n\tTemplate directory: {templateDirectory}\n\tJson data filename: {qualifiedDataFilename}\n\tWorking directory: {self._config.workingDirectory}\n\tTarget filename: {targetFilename}\n")
        #
        report = self.render_jinja_template (templateFilename, qualifiedDataFilename, templateDirectory)
        self.write_file (report, os.path.join (self._config.workingDirectory, targetFilename))
        print (f"Markdown document has been generated!")
        return

    def to_percentage(self, teljari, nefnari, aukastafir = 2) -> int:
        if nefnari == 0:
            return 0
        else:
            return round ((teljari / nefnari)*100, aukastafir)

    def get_file_contents (self, filename) -> str:
        with open (filename, mode="r", encoding="utf-8") as f:
            return f.read ()

    def write_file (self, contents, filename) -> None:
        with open (filename, mode="w", encoding="utf-8") as f:
            f.write (contents)
        return

    def run_operation (self, startingLocation, location, operation, captureOutput = False):
        self.print_v (f"Starting location: {startingLocation} - Location: {location} - Operation: {operation}")
        os.chdir (location)
        output = subprocess.run (operation, capture_output=captureOutput, text=True)
        os.chdir (startingLocation)
        return output