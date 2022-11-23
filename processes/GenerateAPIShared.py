import os
import subprocess
import shutil

import APISupport
import CreateSnapshots
import GenerateDataHealthReportData
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

def refresh_latest ():
    dbtOperation = ["dbt", "run", "--full-refresh"] #  --fail-fast fjarlægt þar sem dbt rakti dependencies ekki nógu vel
    run_operation (APISupport.workingDirectory, APISupport.latest_path, dbtOperation)
    return

def generate_documentation_data (workingDirectory):
    dbtOperation = ["dbt", "docs", "generate"]
    run_operation (workingDirectory, APISupport.latest_path, dbtOperation)
    return

def run_tests ():
    dbtOperation = ["dbt", "--log-format", "json",  "test"]
    output = run_operation (APISupport.workingDirectory, APISupport.latest_path, dbtOperation, True)
    APISupport.print_v (f"Output for dbt test results: {APISupport.dbt_test_output_file_info.qualified_name}")
    APISupport.write_file (output.stdout, APISupport.dbt_test_output_file_info.qualified_name)
    return

# Þetta er sértækt, útfæra á sama hátt og target database (interface, útfærslur, APISupport)
def publish_to_confluence (documentName, configParam):
    if APISupport.config['documentation']['publish-to'] != 'Confluence' or APISupport.config['documentation']['publish'][configParam] != True:
        print (f"{configParam} not published due to config settings!")
        return
    
    qualifiedConfigName = f"{APISupport.workingDirectory}/mark_config.txt"
    # Skoða að setja trace flaggið inn líka, þegar verbose er sett í config!
    operation = ['mark', '-c', qualifiedConfigName, '-f', documentName]
    run_operation (APISupport.workingDirectory, APISupport.workingDirectory, operation)
    return

def print_bordered_comment (comment):
    print (f"\n{separator}\n\n{comment}\n{separator}\n")

def run_file_cleanup ():
    if len (APISupport.runFileDirectory) < 15: # Við viljum ekki henda hálfu drifi út af mistökum!
        print (f"The run file directory looks suspicious, no files deleted! Run file directory: {APISupport.runFileDirectory}")
        raise

    if os.path.exists (APISupport.runFileDirectory):
        shutil.rmtree (APISupport.runFileDirectory)
        APISupport.print_v ("Run file directory deleted!")
    
    os.mkdir (APISupport.runFileDirectory)
    APISupport.print_v (f"Run file directory created at: {APISupport.runFileDirectory}")
    return

def run ():
    startTime = APISupport.get_current_time()

    APISupport.initialize ()
    run_file_cleanup ()

    print_bordered_comment ("Running dbt to refresh models and data (Latest)")
    refresh_latest ()

    print_bordered_comment ("Taking snapshots for the Latest models")
    CreateSnapshots.run () # Creates current state snapshots, removes re-run data and creates and extends snapshot tables as needed. Creates snapshot views, does not maintain them.
    
    print_bordered_comment ("Running dbt tests")
    run_tests () # Skilar skrá: 1
    
    print_bordered_comment ("Generating data health report data")
    GenerateDataHealthReportData.run () # Reads dbt test results and returns a transformed and enriched version for report generation. Skilar skrá: 2
    
    print_bordered_comment ("Generating data health report")
    dataHealthReportFilename = "api_data_health_report.md"
    APISupport.generate_markdown_document ("api_data_health_report_template.md", APISupport.api_data_health_report_data_file_info.name, dataHealthReportFilename)
    
    print_bordered_comment ("Publishing data health report")
    publish_to_confluence (dataHealthReportFilename, 'data-health-report') # Skipta út fyrir almennari útgáfu! Sjá publish fall.

    print_bordered_comment ("Enriching dbt test result data with Concept Glossary and Data Dicationary data, along with DB type info")
    EnrichMetadataCatalog.run () # Skilar skrám: 3, 4, 5
    
    print_bordered_comment ("Generating definition health report data")
    GenerateDefinitionHealthReportData.run () # Skilar skrá: 6
    
    print_bordered_comment ("Generating definition health report")
    definitionHealthReportFilename = "api_definition_health_report.md"
    APISupport.generate_markdown_document ("api_definition_health_report_template.md", APISupport.api_definition_health_report_data_file_info.name, definitionHealthReportFilename)
    
    print_bordered_comment ("Publishing definition health report")
    publish_to_confluence (definitionHealthReportFilename, 'definition-health-report')
    
    print_bordered_comment ("Generating user documentation data")
    GenerateDocumentationData.run() # Skilar skrá 7
    
    documentationFilename = "api_documentation.md"
    print_bordered_comment ("Generating user documentation")
    APISupport.generate_markdown_document ("api_documentation_template.md", APISupport.api_documentation_data_file_info.name, documentationFilename, True)
    
    print_bordered_comment ("Publishing user documentation")
    publish_to_confluence (documentationFilename, 'user-documentation')

    print_bordered_comment (f"API pipeline finished in {APISupport.get_execution_time_in_seconds (startTime)} seconds!")
    return 0

def main ():
    return run ()

if __name__ == '__main__':
    main ()