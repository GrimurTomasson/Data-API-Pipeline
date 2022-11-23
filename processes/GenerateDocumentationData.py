import os
import json

import APISupport
from SharedDataClasses import CountPercentage
from DocumentationClasses import Documentation, Relation, Column, ColumnType, ColumnDescription


def get_column_description (columnData) -> ColumnDescription:
    if len (columnData['glossary_info']['description']) > 0:
        return ColumnDescription (columnData['glossary_info']['description'], "Hugtök")
    if len (columnData['description']) > 0:
        return ColumnDescription (columnData['description'], "Athugasemd við dálk")
    return ColumnDescription (None, None, True)

def generate_documentation (enrichedCatalogJson) -> Documentation:
    docs = Documentation()
    for relationKey in enrichedCatalogJson['sources']:
        relationData = enrichedCatalogJson['sources'][relationKey]
        schemaName = relationData['metadata']['schema']
        relationName = relationData['metadata']['name']
        APISupport.print_v (f"Schema: {schemaName} - Relation: {relationName}")
        
        if not schemaName in APISupport.config['public-schemas']: # Það koma með öðrum orðum hvorki öll vensl né dálkar inn
            APISupport.print_v (f"Non public schema: {schemaName}")
            continue
        
        relation = Relation (schemaName, relationName)
        for columnKey in relationData['columns']:
            columnData = relationData['columns'][columnKey]
            columnType = ColumnType (columnData['database_info']['type_name'], APISupport.targetDatabaseInterface.get_type_length(columnData))
            relation.columns.append (Column(columnData['name'], columnType, get_column_description (columnData)))
        
        docs.relations.append (relation)
    return docs

def run ():
    with open (APISupport.enriched_dbt_catalog_file_info.qualified_name, encoding="utf-8") as json_file:
        enrichedCatalogJson = json.load (json_file)
    
    documentation = generate_documentation (enrichedCatalogJson)

    jsonData = json.dumps (documentation, indent=4, cls=APISupport.EnhancedJSONEncoder)
    APISupport.write_file (jsonData, APISupport.api_documentation_data_file_info.qualified_name)
    return 0

def main ():
    return run ()

if __name__ == '__main__':
    main ()