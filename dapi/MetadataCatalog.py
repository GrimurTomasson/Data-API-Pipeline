import os
import json
import shutil

from .Shared.Decorators import output_headers, execution_time
from .Shared.Config import Config
from .Shared.Utils import Utils
from .Shared.Logger import Logger
from .TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory, TargetDatabase
from .ConceptGlossary.ConceptGlossaryFactory import ConceptGlossaryFactory, ConceptGlossary

class MetadataCatalog:
    def __init__ (self) -> None:
        self._targetDatabase = TargetDatabaseFactory ().get_target_database()
        self._conceptGlossary = ConceptGlossaryFactory ().get_concept_glossary()
        return

    def __get_model_description (self, jsonData, modelName) -> str:
        Logger.debug (f"\tgetModelDescription - modelName: {modelName}")
        if not modelName in jsonData['nodes']:
            return 'There is no model for this relation!'
        model = jsonData['nodes'][modelName]
        if 'description' in model:
            desc = model['description']
            Logger.debug(f"\t\tModel description found: {desc}\n")
            return desc
        else:
            Logger.warning("\t\tNo model description found !!!\n")
            return ''

    def __get_column_description (self, jsonData, modelName, columnName) -> str:
        Logger.debug (f"\tgetColumnDescription - modelName: {modelName}, columnName: {columnName}")
        if not columnName in jsonData['nodes'][modelName]['columns']:
            return 'This column is not in the model for this relation!'
        column = jsonData['nodes'][modelName]['columns'][columnName]
        if 'description' in column:
            desc = column['description']
            if len (desc) > 0:
                Logger.debug(f"\t\tColumn description found: {desc}\n")
                return desc
        Logger.debug("\t\tNo column description found !!!\n")
        return ''

    @output_headers
    @execution_time
    def enrich (self) -> None:
        """Enriching dbt test result data with Concept Glossary and Data Dicationary data, along with DB type info"""

        # Generate catalog data
        dbtOperation = Utils.add_dbt_profile_location (["dbt", "docs", "generate"]) #  --fail-fast fjarlægt þar sem dbt rakti dependencies ekki nógu vel
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)

        dbt_output_path = os.path.join (Config.latestPath, "target")
        source_manifest_file = os.path.join (dbt_output_path, "manifest.json")
        source_catalog_file = os.path.join (dbt_output_path, "catalog.json")

        Logger.info (f"\tManifest \n\t\tSource: {source_manifest_file} \n\t\tTarget: {Config.dbtManifestFileInfo.qualified_name}\n")
        Logger.info (f"\tCatalog \n\t\tSource: {source_catalog_file} \n\t\tTarget: {Config.dbtCatalogFileInfo.qualified_name}\n")

        shutil.copy2 (source_manifest_file, Config.dbtManifestFileInfo.qualified_name)
        shutil.copy2 (source_catalog_file, Config.dbtCatalogFileInfo.qualified_name)
        
        with open (Config.dbtCatalogFileInfo.qualified_name, encoding="utf-8") as json_file:
            catalogJson = json.load (json_file)
        #
        with open (Config.dbtManifestFileInfo.qualified_name, encoding="utf-8") as json_file:
            manifestJson = json.load (json_file)
        #
        for relationKey in catalogJson['nodes']:
            relation = catalogJson['nodes'][relationKey]
            relation['metadata']['description'] = self.__get_model_description(manifestJson, relationKey)
            for columnKey in relation['columns']:
                column = relation['columns'][columnKey]
                column['description'] = self.__get_column_description(manifestJson, relationKey, columnKey)
                schemaName = relation['metadata']['schema']
                tableName = relation['metadata']['name']
                columnName = column ['name']
                Logger.debug(f"\tSchema name: {schemaName} - Table name: {tableName} - Column name: {columnName}")
                
                typeInfoData = self._targetDatabase.get_type_info_column_data (schemaName, tableName, columnName)
                column['database_info'] = typeInfoData
                
                try:
                    glossaryData = self._conceptGlossary.get_glossary_column_data (schemaName, tableName, columnName).as_dictionary()
                    column['glossary_info'] = glossaryData
                except Exception as error:
                    Logger.error (f"Error while retrieving glossary information for column: {schemaName}-{tableName}-{columnName}. Error message: {error}")
        
        jsonData = json.dumps(catalogJson, indent=4)
        Utils.write_file (jsonData, Config.enrichedDbtCatalogFileInfo.qualified_name)
        return

def main():
    return MetadataCatalog ().enrich ()

if __name__ == '__main__':
    main()