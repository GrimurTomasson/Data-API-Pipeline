# Data-API-Pipeline

The **Data-API-Pipeline** is an automated pipeline for creating Data APIs. 
The goal is to make creating and refreshing a **Data API** as simple as running a *CI/CD* pipeline for process software. 

## Instructions
The following is focused on getting this software running on a development machine, writing *CI/CD* pipelines on top of it is not covered. 

### Setup
Make sure you have the [dependencies](##Dependencies) covered.  
- Create a [virtual environment for Python](https://docs.python.org/3/library/venv.html). For example, on a *Windows* machine we could run `python -m venv .\pyEnv`  
- Enable the virtual environment by running `.\pyEnv\Scripts\activate`  
- Run `pip install git+https://github.com/GrimurTomasson/Data-API-Pipeline`

### API Configuration
1. Create a `dbt` project in a subfolder of the root directory of your Data API.
2. In a *console* at the root directory of your Data API, run `create-dapi` to initialize the pipeline.
3. Consider using the `profiles.yml` file as the base for your `dbt` profiles, it supports environment variables quite well.
4. Edit `api_config.yml`, it contains helpful information in the form of comments.
5. Edit `api_documentation_template.md`, write a high level description of the API.
6. Edit `dapi.env` if you need to store secrets on development machines.

#### Example folder structure
Here, *reference* is the concept we are working with as well as a folder, *data_api* and *current* are subfolders. 
```
reference  
    data_api -> contains api_config.yml, api_documentation_template.md and optionally dapi.env  
        current -> contains the dbt project and optionally profiles.yml  
```

### General use
In a *console* at the root directory of your Data API, run `dapi build` to run all the steps.

#### Multi-instance API
This is highly useful if you seperate your analytical- and online api, which have very different SLAs. 
In a *console* at the root directory of your Data API, run  
`dapi build -e instance_environment.env`  
or   
`dapi build-data -e instance_environment.env` if you don't want documentation or testing (not needed after the first instance).

All reports and documentation are written to the root directory of your Data API.


## Details
In addition to the following logical steps, we clean up and create a run-file directory where the results from each steps are stored. The run-file directory location is config controlled. All files created by the logical steps are prefixed with a sequence and can be found in the run-file directory.
Note, all of the following steps are run without parameters but some of them require files from prior steps. Any steps without a defined input can be run in isolation. Note that doing so in production may cause discrepancies, for example if we refresh our current relations without creating history, they will likely not match.

### 1. Refresh dbt models
We run *dbt* to create or refresh relations defined in our models, our current models (*Latest*), not history.  
Implemented in: **Latest.py**
#### Input
Models in a *dbt* project.
#### Output
Relations in the target database have been updated or created. The pipeline uses no files created by this process.
`manifest.json` - Model information in json format.

### 2. Create history
We create a [Type 4](https://en.wikipedia.org/wiki/Slowly_changing_dimension) history, taking daily snapshots of all tables defined in the *history* part of the *config*. In case of re-runs, we delete data generated by a previous run for the same date.
We create history schemas and tables if needed and add columns to existing relations where possible.  
Implemented in: **Snapshot.py**
#### Input
Relations created by [Step 1](###1.Refreshdbtmodels).
#### Output
History relations in the target database have been updated or created. 

### 3. Run dbt tests
We run *dbt* to run automated tests for the *Latest* part of the API.  
Implemented in: **Latest.py**
#### Input
Relations created by [Step 1](###1.Refreshdbtmodels).
#### Output
`1_dbt_test_output.json` - The concents of this file are in a *dbt* format.

### 4. Generate data health report
Generates a data health report from the results of automated testing, using a *Jinja* template in *markdown* format. If a knowledge base has been defined in config, publishes the report to it. Current implementations: *Confluence*.  
Implemented in: **DataHealthReport.py**
#### Input
Json file create in [Step 3](###3.Rundbttests).  
Relation cardinality is retrieved from the target database.
`api_data_health_report_template.md` - A shared report template, in the Data-API-Pipeline repository.
#### Output
`2_api_data_health_report_data.json` - The data used to generate the final report.  
`api_data_health_report.md` - The data health report, in *markdown* format.

### 5. Enrich metadata
Enriches the *dbt* metadata-catalog using *Concept Glossary* definitions and *dbt* manifest information. Uses *dbt* docs functionality to generate `catalog.json`.  
Implemented in: **MetadataCatalog.py**
#### Input
`manifest.json` - Created by [Step 1](###1.Refreshdbtmodels).  
`catalog.json` - Created by this step.

#### Output
`3_dbt_manifest.json` - Copy, not modified.  
`4_dbt_catalog.json` - Copy, not modified.  
`5_enriched_dbt_catalog.json` - Enriched *dbt* catalog.

### 6. Generate definition health report
Generates a definition health report, detailing how well defined our API is, including data-type adherence to *Data Dictionary* definitions. If a knowledge base has been defined in config, publishes the report to it. Current implementations: *Confluence*.  
Implemented in: **DefinitionHealthReport.py**
#### Input
`5_enriched_dbt_catalog.json` - Created by [Step 5](###5.Enrichmetadata).  
`api_definition_health_report_template.md` - A shared report template, in the Data-API-Pipeline repository.
#### Output
`6_api_definition_health_report_data.json` - The data used to generate the final report.  
`api_definition_health_report.md` - The data definition health report, in *markdown* format.

### 7. Generate documentation
Generates end-user documentation for the API. If a knowledge base has been defined in config, publishes the report to it. Current implementations: *Confluence*.  
Implemented in: **Documentation.py**
#### Input
`api_documentation_template.md` - A *Jinja* template in *markdown* format, specific to the API.
#### Output
`7_api_documentation_data.json` - The data used to generate the final documentation.  
`api_documentation.md` - The documentation, in *markdown* format.

## Dependencies
This solution relies on *dbt* to create relations and perform automated testing. It also uses and enriches *dbt* json files in order to create reports and documentation. Note, it relies on quite a few *Python* pakcages that *dbt* relies on. Aside from *Python* and database specific dependencies, everything should be installed as a part of this package. Try installing *dbt* if you are having problems.
It also relies on *Python*, minimum version 3.9.11.
Follow the links for installation instructions for [Python](###InstallingPython) and [dbt](###Installingdbt).

### Databases
The following dependencies only apply to specific databases.
#### SQL Server
[ODBC Driver version 18](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).

---

## Appendices
Hopefully helpful information that is not about this software

### Installing Python
Use installer/source from [www.python.org](https://www.python.org/downloads/). Check all marks except those that require certain software, like *Visual Studio*. Adding *Python* to path is most important.

### Installing dbt
The following are *console* operations.
If one of the core *dbt* databases is your target, run `pip install dbt-core==1.3.2`.
If *SQL Server* is your target database, run `pip install dbt-sqlserver==1.3.0` 

### Making Python files runnable on Windows
The following is only needed for development.
Run the following steps in an *admin console*.
1. `assoc .py=Python`
2. `where python` (add it to path if it is not found!)
3. `ftype | find "Python"`
    
If the results from step three ends with `"%L" %*`

4. `ftype Python=(path from step 2, including python.exe) "%L" %*`

Else

4. `ftype Python=(path from step 2, including python.exe) "%1" %*`

If this still doesn't work, change the properties of each *.py* file in `Data-API-Pipeline/processes` and change `Opens with:` to *Python*

