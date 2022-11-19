import os

import APISupport

def run(): 
    APISupport.generate_markdown_document ("api_data_health_report_template.md", "api_data_health_report_data.json", "api_data_health_report.md")
    #workingDirectory = os.getcwd()
    
    #templateName = "api_data_health_report_template.md"
    #testResultFilename = "api_data_health_report_data.json"
    #healthReportFilename = "api_data_health_report.md"
    #qualifiedTestResultFilename = f"{workingDirectory}/{testResultFilename}"
    #
    #APISupport.print_v(f"GenerateHealthReport:\n\tWorking directory: {workingDirectory}\n\tTest results filename: {testResultFilename}\n\tHealth report filename: {healthReportFilename}\n\tQualified result filename: {qualifiedTestResultFilename}\n")
    #
    #report = APISupport.render_jinja_template (templateName, qualifiedTestResultFilename)
    #APISupport.write_file(report, f"{workingDirectory}/{healthReportFilename}")
    #print(f"Data health report has been generated!")
    return 0

def main():
    return run()

if __name__ == '__main__':
    main()