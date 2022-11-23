import os
import json

import APISupport
from SharedDataClasses import CountPercentage
from DefinitionHealthClasses import HealthReport, Errors, Error, Concept, Stats, StatsRelation, StatsTotal

def check_for_documentation_error (schemaName, relationName, columnName, column) -> Error:
    if len (column['description']) == 0 and len (column['glossary_info']['description']) == 0:
        return Error (schemaName, relationName, columnName, 'Skjölun vantar!')
    return None

def check_for_concept_overwrite (schemaName, relationName, columnName, column) -> Concept:
    if len (column['description']) > 0 and len (column['glossary_info']['description']) > 0:
        return Concept (schemaName, relationName, columnName, column['glossary_info']['concept_name'])
    return None

def check_for_type_error (schemaName, relationName, columnName, column) -> Error:
    if len (column['glossary_info']['data_type']) == 0: # Ekki hugtak úr CG/DD
        return None

    glossaryType = column['glossary_info']['data_type']
    databaseType = column['database_info']['type_name']
    if glossaryType != databaseType:
        return Error (schemaName, relationName, columnName, f"Gagnatýpa í skilgreiningu hugtaks: {glossaryType} - Gagnatýpa í grunni: {databaseType}")
    if glossaryType == databaseType and glossaryType in ['char', 'nchar', 'varchar', 'nvarchar'] and column['glossary_info']['max_length'] != column['database_info']['max_length']:
        return Error (schemaName, relationName, columnName, f"Lengd gagnatýpu í skilgreiningu hugtaks: {column['glossary_info']['max_length']} - Lengd í grunni: {column['database_info']['max_length']}")
    return None

def generate_health_data(enrichedCatalogJson) -> HealthReport:
    apiHealth = HealthReport (api_name = APISupport.config['database']['name'])
    relationsTotal = 0 
    columnsTotal = 0
    okColumnsTotal = 0
    
    for relationKey in enrichedCatalogJson['sources']:
        relation = enrichedCatalogJson['sources'][relationKey]
        schemaName = relation['metadata']['schema']
        relationName = relation['metadata']['name']
        APISupport.print_v (f"Schema: {schemaName} - Relation: {relationName}")
        if not schemaName in APISupport.config['public-schemas']: # Það koma með öðrum orðum hvorki öll vensl né dálkar inn
            APISupport.print_v (f"Non public schema: {schemaName}")
            continue
        # Per relation stats
        relationTypeErrorList = []
        relationDocsErrorList = []
        relationOverwrittenConceptList = []
        relationOkColumns = 0
        
        relationsTotal += 1
        for columnKey in relation['columns']:
            column = relation['columns'][columnKey]
            columnName = column['name']
            
            docError = check_for_documentation_error (schemaName, relationName, columnName, column)
            if docError is not None:
                relationDocsErrorList.append (docError)
            
            conceptOverwrite = check_for_concept_overwrite (schemaName, relationName, columnName, column)
            if conceptOverwrite is not None:
                relationOverwrittenConceptList.append (conceptOverwrite)
            
            typeError = check_for_type_error (schemaName, relationName, columnName, column) # Skrifa þetta fall, og líka föll fyrir hinar villurnar!
            if typeError is not None:
                relationTypeErrorList.append (typeError)
            
            relationOkColumns += docError is not None and typeError is not None
            
        relationColumns = len (relation['columns'])
        columnsTotal += relationColumns
        okColumnsTotal += relationOkColumns
        
        overwrittenConcepts = CountPercentage (len (relationOverwrittenConceptList), APISupport.to_percentage (len (relationOverwrittenConceptList), relationColumns))
        okColumns = CountPercentage (relationOkColumns, APISupport.to_percentage (relationOkColumns, relationColumns))
        combinedErrors = len (relationTypeErrorList) + len (relationDocsErrorList)
        errors = CountPercentage (combinedErrors, APISupport.to_percentage (combinedErrors, relationColumns))
        typeErrors = CountPercentage (len (relationTypeErrorList), APISupport.to_percentage (len (relationTypeErrorList), relationColumns))
        docErrors = CountPercentage (len (relationDocsErrorList), APISupport.to_percentage (len (relationDocsErrorList), relationColumns))
        relationStats = StatsRelation (schemaName, relationName, relationColumns, overwrittenConcepts, okColumns, errors, typeErrors, docErrors)
        apiHealth.stats.relation.append (relationStats)
        
        apiHealth.overwritten_concepts.extend (relationOverwrittenConceptList)
        apiHealth.errors.type.extend (relationTypeErrorList)
        apiHealth.errors.documentation.extend (relationDocsErrorList)
        
    oaOverwrittenConcepts = CountPercentage (len (apiHealth.overwritten_concepts), APISupport.to_percentage (len (apiHealth.overwritten_concepts), columnsTotal))
    oaOkColumns = CountPercentage (okColumnsTotal, APISupport.to_percentage (okColumnsTotal, columnsTotal))
    oaErrors = CountPercentage (len (apiHealth.errors.type) + len (apiHealth.errors.documentation), APISupport.to_percentage (len (apiHealth.errors.type) + len (apiHealth.errors.documentation), columnsTotal))
    oaTypeErrors = CountPercentage (len (apiHealth.errors.type), APISupport.to_percentage (len (apiHealth.errors.type), columnsTotal))
    oaDocErrors = CountPercentage (len (apiHealth.errors.documentation), APISupport.to_percentage (len (apiHealth.errors.documentation), columnsTotal))
    apiHealth.stats.total = StatsTotal (relationsTotal, oaOverwrittenConcepts, columnsTotal, oaOkColumns, oaErrors, oaTypeErrors, oaDocErrors)

    return apiHealth

def run() -> int:
    workingDirectory = os.getcwd()
    with open (APISupport.enriched_dbt_catalog_file_info.qualified_name, encoding="utf-8") as json_file:
        enrichedCatalogJson = json.load(json_file)
    apiHealth = generate_health_data (enrichedCatalogJson)
    jsonData = json.dumps (apiHealth, indent=4, cls=APISupport.EnhancedJSONEncoder)
    APISupport.write_file (jsonData, APISupport.api_definition_health_report_data_file_info.qualified_name) 
    return 0

def main():
    return run()

if __name__ == '__main__':
    main()