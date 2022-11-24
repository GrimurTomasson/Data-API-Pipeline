import os
import json
import shutil

import Decorators
import APISupport

class MetadataCatalog:
    def __init__ (self) -> None:
        APISupport.initialize ()

    def __get_model_description (self, jsonData, modelName) -> str:
        APISupport.print_v(f"getModelDescription - modelName: {modelName}")
        if not modelName in jsonData['sources']:
            return 'There is no model for this relation!'
        model = jsonData['sources'][modelName]
        if 'description' in model:
            desc = model['description']
            APISupport.print_v(f"\tModel description found: {desc}")
            return desc
        else:
            APISupport.print_v("\tNo model description found !!!")
            return ''

    def __get_column_description (self, jsonData, modelName, columnName) -> str:
        APISupport.print_v(f"\tgetColumnDescription - modelName: {modelName}, columnName: {columnName}")
        if not columnName in jsonData['sources'][modelName]['columns']:
            return 'This column is not in the model for this relation!'
        column = jsonData['sources'][modelName]['columns'][columnName]
        if 'description' in column:
            desc = column['description']
            APISupport.print_v(f"\t\tColumn description found: {desc}")
            return desc
        else:
            APISupport.print_v("\t\tNo column description found !!!")
            return ''

    @Decorators.output_headers
    @Decorators.execution_time
    def enrich (self) -> None:
        """Enriching dbt test result data with Concept Glossary and Data Dicationary data, along with DB type info"""
        targetDatabase = APISupport.get_target_database_interface ()

        dbt_output_path = f"{APISupport.latest_path}/target/"
        source_manifest_file = os.path.join (dbt_output_path, "manifest.json")
        source_catalog_file = os.path.join (dbt_output_path, "catalog.json")

        target_manifest_file = os.path.join (APISupport.runFileDirectory, "3_dbt_manifest.json")
        target_catalog_file = os.path.join (APISupport.runFileDirectory, "4_dbt_catalog.json")

        APISupport.print_v (f"Manifest - Source: {source_manifest_file} - Target: {target_manifest_file}")
        APISupport.print_v (f"Catalog - Source: {source_catalog_file} - Target: {target_catalog_file}")

        shutil.copy2 (source_manifest_file, target_manifest_file)
        shutil.copy2 (source_catalog_file, target_catalog_file)
        
        with open (target_catalog_file, encoding="utf-8") as json_file:
            catalogJson = json.load (json_file)
        #
        with open (target_manifest_file, encoding="utf-8") as json_file:
            manifestJson = json.load (json_file)
        #
        for relationKey in catalogJson['sources']:
            relation = catalogJson['sources'][relationKey]
            relation['metadata']['description'] = self.__get_model_description(manifestJson, relationKey)
            for columnKey in relation['columns']:
                column = relation['columns'][columnKey]
                column['description'] = self.__get_column_description(manifestJson, relationKey, columnKey)
                schemaName = relation['metadata']['schema']
                tableName = relation['metadata']['name']
                columnName = column ['name']
                APISupport.print_v(f"\tSchema name: {schemaName} - Table name: {tableName} - Column name: {columnName}")
                
                typeInfoData = targetDatabase.get_type_info_column_data (schemaName, tableName, columnName)
                column['database_info'] = typeInfoData
                
                glossaryData = targetDatabase.get_glossary_column_data (schemaName, tableName, columnName)
                column['glossary_info'] = glossaryData
        
        jsonData = json.dumps(catalogJson, indent=4)
        APISupport.write_file (jsonData, APISupport.enriched_dbt_catalog_file_info.qualified_name)
        return

def main():
    return MetadataCatalog ().enrich ()

if __name__ == '__main__':
    main()