import json
from collections import namedtuple
import argparse
from pkgutil import get_data
import re
import pyodbc

import APISupport

tableWidth = "" #"style=\"width:100%\""

Header = namedtuple("Header", "version timestamp id")
Error = namedtuple("Error", "name failures sqlFile")
Stats = namedtuple("Stats", "error ok skipped total")
DocData = namedtuple("DocData", "Header Errors Stats FileMap RelationStats")
TestEntry = namedtuple("TestEntry", "testName ok error")
RelationStats = namedtuple("RelationStats", "relation ok error")

tableNameRegEx = re.compile('[a-z\_0-9]+\.yml', re.IGNORECASE)
cardinalityMap = {}

def get_database_connection(database_server, database_name):
    print(f"\tCreating a DB connection to: {database_server} - {database_name}")
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+database_server+';DATABASE='+database_name+';Trusted_Connection=yes;')
    conn.autocommit = True
    return conn

def get_relation_name(filePath):
    return tableNameRegEx.search(filePath).group()[:-4]

def get_relation_name_from_test_name(projectName, testName):
    tableNameFromTestRegEx = re.compile(f"{projectName}\_[a-z\_]+\_v[0-9]+", re.IGNORECASE) # source_is_true_Nustada_bekkur_v1_lokadagur__lokadagur_upphafsdagur
    return tableNameFromTestRegEx.search(testName).group()[len(projectName)+1:]

def retrieve_relation_cardinality(databaseServer, api, project, filePath):
    tableName = get_relation_name(filePath)
    if tableName in cardinalityMap:
        return cardinalityMap[tableName]
    else:
        startTime = APISupport.get_current_time()
        conn = get_database_connection(databaseServer, api)
        rows = conn.cursor().execute(f"SELECT COUNT(1) AS fjoldi FROM {project}.{tableName}").fetchval()
        print(f"\tCardinality for table {project}.{tableName} retrieved in {APISupport.get_execution_time_in_seconds(startTime)} seconds - cardinality: {rows}")
        cardinalityMap[tableName] = rows
        return rows

def retrieve_file_contents(filename):
    with open (filename, mode="r", encoding="utf-8") as f:
        return f.read()

def retrieve_json_object(filename):
    testLog = retrieve_file_contents (filename)
    # Convert the log file contents into legal json
    testLog = testLog.replace("\n", "").replace("\r", "")
    testLog = "{\"entries\": [" + testLog.replace("}{", "},{") + "]}"
    jsonObject = json.loads (testLog)
    return jsonObject

def retrieve_data(jsonObject, projectName):
    errorList = []
    stats = Stats(0, 0, 0, 0)
    header = ""
    fileMap = {}
    testList = []
    for entry in jsonObject['entries']:
        if entry["code"] == "Q009": # OK
            testList.append (TestEntry(entry["data"]["node_info"]["node_name"], 1, 0))
        if entry["code"] == "Q011" and entry["level"] == "error": # Errors
            testName = (entry["data"])["name"]
            noFailures = (entry["data"])["failures"]
            query = ((entry["data"])["node_info"])["node_path"]
            errorList.append (Error(testName, noFailures, query)) 
            testList.append (TestEntry(entry["data"]["node_info"]["node_name"], 0, 1))
        if entry["code"] == "Z023": # Stats
            error = ((entry["data"])["stats"])["error"]
            ok = ((entry["data"])["stats"])["pass"]
            skip = ((entry["data"])["stats"])["skip"]
            total = ((entry["data"])["stats"])["total"]
            stats = Stats(error, ok, skip, total)
        if entry["code"] == "A001": # Header
            version = (entry["data"])["v"].replace("=", "")
            timestamp = entry["ts"]
            id = entry["invocation_id"]
            header = Header(version, timestamp, id)
        if entry["code"] == "Z026": # File map
            sqlPath = (entry["data"])["path"]
            sqlFile = sqlPath[-(len(sqlPath) - (sqlPath.rindex("\\") + 1)):]
            fileMap[sqlFile] = sqlPath
    # Aggregate relation stats
    relationStatMap = {}
    for test in testList:
        relation = get_relation_name_from_test_name(projectName, test.testName)
        if relation in relationStatMap:
            relStats = relationStatMap[relation]
            relationStatMap[relation] = RelationStats(relation, relStats.ok + test.ok, relStats.error + test.error)
        else:
            relationStatMap[relation] = RelationStats(relation, test.ok, test.error)
    relationStatMap = dict(sorted(relationStatMap.items()))
    return DocData (header, errorList, stats, fileMap, list(relationStatMap.values())) 

def to_percentage(teljari, nefnari, aukastafir):
    if nefnari == 0:
        return 0
    else:
        return round((teljari / nefnari)*100, aukastafir)

def create_stats_total_table(docData, htmlTables):
    table = ""
    if htmlTables == False:
        table += f"|               | Fjöldi prófana     | Prósenta   |\n"
        table += f"| :------------ | ------------------: | ---------: |\n"
        table += f"| Villur | {docData.Stats.error} | {to_percentage(docData.Stats.error, docData.Stats.total, 2)} |\n"
        table += f"| Í lagi | {docData.Stats.ok} | {to_percentage(docData.Stats.ok, docData.Stats.total, 2)} |\n"
        table += f"| Sleppt | {docData.Stats.skipped} | {to_percentage(docData.Stats.skipped, docData.Stats.total, 2)} |\n"
        table += f"| **Samtals** | **{docData.Stats.total}** | |\n\n"    
    else:
        table += "<table>\n<thead>\n<tr><th></th><th align=\"right\">Fjöldi prófana</th><th align=\"right\">Prósenta</th></tr>\n</thead>\n<tbody>\n"
        table += f"<tr><td align=\"left\">Villur</td><td align=\"right\">{docData.Stats.error}</td><td align=\"right\">{to_percentage(docData.Stats.error, docData.Stats.total, 2)}</td></tr>\n"
        table += f"<tr><td align=\"left\">Í lagi</td><td align=\"right\">{docData.Stats.ok}</td><td align=\"right\">{to_percentage(docData.Stats.ok, docData.Stats.total, 2)}</td></tr>\n"
        table += f"<tr><td align=\"left\">Sleppt</td><td align=\"right\">{docData.Stats.skipped}</td><td align=\"right\">{to_percentage(docData.Stats.skipped, docData.Stats.total, 2)}</td></tr></tbody>\n"
        table += f"<tfoot>\n<tr><td align=\"left\"><b>Samtals</b></td><td align=\"right\"><b>{docData.Stats.total}</b></td><td align=\"right\"></td></tr>\n</tfoot>\n"
        table += "</table>\n\n"
    return table

def create_stats_relation_table(docData, htmlTables):
    table = ""
    if htmlTables == False:
        table += f"| Vensl                                | Villur | Prósent á villu | OK     | Heildarfjöldi prófana    |\n"
        table += f"| :----------------------------------- | -----: | --------------: | -----: | -----------------------: |\n"
        for relationStats in docData.RelationStats:
            total = relationStats.ok + relationStats.error
            table += f"| {relationStats.relation} | {relationStats.error} | {to_percentage(relationStats.error, total, 2)} | {relationStats.ok} | {total} |\n"
    else:
        table += f"<table {tableWidth}>\n<thead>\n<tr> <th align=\"left\">Vensl</th> <th align=\"right\">Villur</th> <th align=\"right\">Prósent á villu</th> <th align=\"right\">OK</th> <th align=\"right\">Heildarfjöldi prófana</th> </tr>\n</thead>\n<tbody>\n"
        for relationStats in docData.RelationStats:
            total = relationStats.ok + relationStats.error
            table += f"<tr> <td align=\"left\">{relationStats.relation}</td> <td align=\"right\">{relationStats.error}</td> <td align=\"right\">{to_percentage(relationStats.error, total, 2)}</td> <td align=\"right\">{relationStats.ok}</td> <td align=\"right\">{total}</td> </tr>\n"
        table += "</tbody>\n</table>"
    return table + "\n\n"

def create_error_report_part(docData, api, project, rootRepoPath, databaseServer, htmlTables):
    errorPart = "## Villur\n"
    sqlAppendix = ""
    #Prefix
    if htmlTables == False:
        errorPart += f"| Vensl                 | Prófun                                     | Raðir á villu  | Prósent raða á villu | Raðir        |\n"
        errorPart += f"| :-------------------- |:------------------------------------------ | -------------: | -------------------: | -----------: |\n"    
    else:
        errorPart += f"<table {tableWidth}>\n<thead>\n <tr> <th align=\"left\">Vensl</th> <th align=\"left\">Prófun</th> <th align=\"right\">Raðir á villu</th> <th align=\"right\">Prósent raða á villu</th> <th align=\"right\">Raðir</th> </tr>\n</thead>\n<tbody>\n"
    #Rows
    for error in docData.Errors:
        noRows = 0
        filePath = docData.FileMap[error.sqlFile]
        if len(filePath) > 0:
            fRelativePath = f"{project}\{filePath}"
            fPath = f"{rootRepoPath}/dbt/{fRelativePath}"
            #if htmlTables != False:
            #    sqlAppendix += f"<a id=\"{error.name}\"></a>\n"
            #    sqlAppendix += f"<table style=\"width:100%\">\n<tbody>\n<tr> <td>Prófun</td> <td>{error.name}</td> </tr><tr> <td>Slóð á SQL fyrirspurn</td> <td><code>{fRelativePath}</code></td></tr>\n</tbody>\n</table>\n\n"
            #else:
            if htmlTables != False:
                sqlAppendix += f"<a id=\"{error.name}\"></a>\n"
            sqlAppendix += f"### {error.name}\n"
            sqlAppendix += f"Slóð á SQL fyrirspurn: `{fRelativePath}`\n"
            sql = retrieve_file_contents(fPath).strip()
            sqlAppendix += f"```\n{sql}\n```\n"
            noRows = retrieve_relation_cardinality(databaseServer, api, project, fPath)
            relationName = get_relation_name(fPath)
        if htmlTables == False:
            errorPart += f"| {relationName} | [{error.name}](###{error.name}) | {error.failures} | {to_percentage(error.failures, noRows, 4)} | {noRows} |\n" # skoða hvort við getum komið sql inn í MD og lyklað á það hér
        else:
            errorPart += f"<tr> <td align=\"left\">{relationName}</td> <td align=\"left\"><a href=\"#{error.name}\">{error.name}</a></td> <td align=\"right\">{error.failures}</td> <td align=\"right\">{to_percentage(error.failures, noRows, 4)}</td> <td align=\"right\">{noRows}</td> </tr>\n"
    #Postfix
    if htmlTables != False:
        errorPart += "</tbody>\n</table>\n"
    #Common
    errorPart += "\n---\n"
    errorPart += "## Villu fyrirspurnir\n"
    errorPart += sqlAppendix
    return errorPart + "\n\n"

def create_md_report(docData, api, project, rootRepoPath, databaseServer, htmlTables):
    # Mark support
    mdReport = "<!-- Space: DAT -->\n" 
    mdReport += "<!-- Parent: Skjölun -->\n"
    mdReport += f"<!-- Title: {api} - Gagnagæði -->\n"
    #
    mdReport += f"# Prófana niðurstöður fyrir {api}->{project}\n"
    # Header info
    mdReport += f"|               |            |\n"
    mdReport += f"| :------------ | :--------- |\n"
    mdReport += f"| Keyrslutími | {docData.Header.timestamp} |\n"
    mdReport += f"| DBT útgáfa | {docData.Header.version} |\n"
    mdReport += f"| Keyrslu auðkenni | {docData.Header.id} |\n"
    mdReport += "---\n"
    # Statistics
    mdReport += f"## Tölfræði\n"
    mdReport += f"### Heild\n"
    mdReport += create_stats_total_table(docData, htmlTables)
    mdReport += f"### Vensl\n"
    mdReport += create_stats_relation_table(docData, htmlTables)
    mdReport += "---\n\n"
    # Errors
    mdReport += create_error_report_part(docData, api, project, rootRepoPath, databaseServer, htmlTables)
    mdReport += "---\n"
    return mdReport

def write_file(contents, filename):
    with open (filename, mode="w", encoding="utf-8") as f:
        f.write (contents)
    return

# python ../RVK-Data-API-Tools/pipelineScripts/GenerateTestReport.py -a RVK-DATA-SFS-API -p Nustada -r "c:\\src\\git\\RVK-DATA-SFS-API" -d "instdataservice.rvk.borg,64839" -h
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--api", help="Name of the API (same as the repo). Mandatory", required=True)
    parser.add_argument("-p", "--project", help="Name of the project (Nustada/Saga). Mandatory.", required=True)
    parser.add_argument("-r", "--rootRepoPath", help="The file path to the root of the repo. Mandatory.", required=True)
    parser.add_argument("-d", "--databaseServer", help="Name and port of the database server the API resides in. Mandatory.", required=True)
    parser.add_argument("-t", "--htmlTables", help="Create HTML tables instead of Markdown ones. Optional.", default=False)
    #
    args = parser.parse_args()
    if not(args.api and args.project and args.rootRepoPath and args.databaseServer):
        parser.print_usage()
        return
    # Process input params!
    startTime = APISupport.get_current_time()
    jsonObject = retrieve_json_object (f"{args.rootRepoPath}/test_run.json") #Param!
    print(f"Json object retrieved in {APISupport.get_execution_time_in_seconds(startTime)} seconds")
    # Extract data from json object
    startTime = APISupport.get_current_time()
    docData = retrieve_data (jsonObject, args.project)
    print(f"Data retrieved from json in {APISupport.get_execution_time_in_seconds(startTime)} seconds")
    # Create report
    startTime = APISupport.get_current_time()
    mdReport = create_md_report(docData, args.api, args.project, args.rootRepoPath, args.databaseServer, args.htmlTables)
    print(f"MD report created in {APISupport.get_execution_time_in_seconds(startTime)} seconds")
    # startTime = get_current_time()
    write_file (mdReport, f"{args.rootRepoPath}/{args.project}_test_run_report.md")
    print(f"Report written to disk in {APISupport.get_execution_time_in_seconds(startTime)} seconds")
    return 0

if __name__ == '__main__':
    main()