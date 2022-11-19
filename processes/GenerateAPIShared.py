import os
import subprocess
import shutil

import APISupport
import CreateSnapshots
import GenerateDataHealthReportData
import GenerateDataHealthReport
import EnrichMetadataCatalog
import GenerateDefinitionHealthReportData
import GenerateDocumentationData


separator = "-" * 120

def run_operation (startingLocation, location, operation, captureOutput = False):
    startTime = APISupport.get_current_time ()
    print (separator)
    APISupport.print_v (f"Starting location: {startingLocation} - Location: {location} - Operation: {operation}")
    os.chdir (location)
    output = subprocess.run (operation, capture_output=captureOutput, text=True)
    os.chdir (startingLocation)
    APISupport.print_v (f"Execution time: {APISupport.get_execution_time_in_seconds(startTime)} seconds.")
    return output

def refresh_latest (workingDirectory):
    dbtOperation = ["dbt", "run", "--full-refresh"] #  --fail-fast fjarlægt þar sem dbt rakti dependencies ekki nógu vel
    path = f"{workingDirectory}{APISupport.config['latest']['relative-path']}"
    run_operation (workingDirectory, path, dbtOperation)
    return

def generate_documentation_data (workingDirectory):
    dbtOperation = ["dbt", "docs", "generate"]
    path = f"{workingDirectory}{APISupport.config['latest']['relative-path']}"
    run_operation (workingDirectory, path, dbtOperation)
    #shutil.copy2 (f"{path}/target/catalog.json", f"{workingDirectory}/documentation_data.json")
    return

def run_tests (workingDirectory):
    dbtOperation = ["dbt", "--log-format", "json",  "test"]
    path = f"{workingDirectory}{APISupport.config['latest']['relative-path']}"
    output = run_operation (workingDirectory, path, dbtOperation, True)
    outputLocation = f"{workingDirectory}/test_results.json"
    APISupport.print_v (f"Output location for dbt test results: {outputLocation}")
    APISupport.write_file (output.stdout, outputLocation)
    return

# Þetta er sértækt, útfæra á sama hátt og target database (interface, útfærslur, APISupport)
def publish_to_confluence (workingDirectory, documentName, configParam):
    if APISupport.config['documentation']['publish-to'] != 'Confluence' or APISupport.config['documentation']['publish'][configParam] != True:
        print (f"{configParam} not published due to config settings!")
        return

    scriptPath = os.path.dirname (os.path.realpath (__file__))
    qualifiedConfigName = f"{scriptPath}/mark_config.txt"
    # Skoða að setja trace flaggið inn líka, þegar verbose er sett í config!
    operation = ['mark', '-c', qualifiedConfigName, '-f', documentName]
    run_operation (workingDirectory, workingDirectory, operation)
    return

def print_bordered_comment (comment):
    print (f"\n{separator}\n\n{comment}\n{separator}\n")

def run ():
    startTime = APISupport.get_current_time()

    workingDirectory = os.getcwd ()
    APISupport.print_v (f"Starting location: {workingDirectory}")
    # Til þess að fá villur snemma
    APISupport.get_config ()
    APISupport.get_target_database_interface ()

    print_bordered_comment ("Running dbt to refresh models and data (Latest)")
    refresh_latest (workingDirectory)

    print_bordered_comment ("Taking snapshots for the Latest models")
    CreateSnapshots.run () # Creates current state snapshots, removes re-run data and creates and extends snapshot tables as needed. Creates snapshot views, does not maintain them.
    
    print_bordered_comment ("Running dbt tests")
    run_tests (workingDirectory)
    
    print_bordered_comment ("Generating data health report data")
    GenerateDataHealthReportData.run () # Reads dbt test results and returns a transformed and enriched version for report generation
    print_bordered_comment ("Generating data health report")
    dataHealthReportFilename = "api_data_health_report.md"
    APISupport.generate_markdown_document ("api_data_health_report_template.md", "api_data_health_report_data.json", dataHealthReportFilename)
    print_bordered_comment ("Publishing data health report")
    publish_to_confluence (workingDirectory, dataHealthReportFilename, 'data-health-report') # Skipta út fyrir almennari útgáfu! Sjá publish fall.

    print_bordered_comment ("Enriching dbt test result data with Concept Glossary and Data Dicationary data, along with DB type info")
    EnrichMetadataCatalog.run ()
    
    print_bordered_comment ("Generating definition health report data")
    GenerateDefinitionHealthReportData.run ()
    print_bordered_comment ("Generating definition health report")
    definitionHealthReportFilename = "api_definition_health_report.md"
    APISupport.generate_markdown_document ("api_definition_health_report_template.md", "api_definition_health_report_data.json", definitionHealthReportFilename)
    print_bordered_comment ("Publishing definition health report")
    publish_to_confluence (workingDirectory, definitionHealthReportFilename, 'definition-health-report')
    
    print_bordered_comment ("Generating user documentation data")
    GenerateDocumentationData.run()
    documentationFilename = "api_documentation.md"
    print_bordered_comment ("Generating user documentation")
    APISupport.generate_markdown_document ("api_documentation_template.md", "api_documentation_data.json", documentationFilename, True)
    print_bordered_comment ("Publishing user documentation")
    publish_to_confluence (workingDirectory, documentationFilename, 'user-documentation')

    print_bordered_comment (f"API pipeline finished in {APISupport.get_execution_time_in_seconds (startTime)} seconds!")

    return 0

def main ():
    return run ()

if __name__ == '__main__':
    main ()