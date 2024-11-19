import os
import json
import shutil

from .Shared.Decorators import post_execution_output
from .Shared.Config import Config
from .Shared.Utils import Utils
from .Shared.PrettyPrint import Pretty
from .Shared.LogLevel import LogLevel
from .Shared.Logger import Logger
from .TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory, TargetDatabase
from .ConceptGlossary.ConceptGlossaryFactory import ConceptGlossaryFactory, ConceptGlossary
from .Shared.AuditDecorators import audit

class MetadataCatalog:
    def __init__ (self) -> None:
        self._enabled = True if Config['documentation']['data-health-report']['generate'] == True or Config['documentation']['definition-health-report']['generate'] == True or Config['documentation']['user-documentation']['generate'] == True else False
        if self._enabled == False:
            return
        
        self._targetDatabase = TargetDatabaseFactory ().get_target_database()
        self._conceptGlossary = ConceptGlossaryFactory ().get_concept_glossary()
        return

    def __get_model_description (self, jsonData, modelName) -> str:
        message = f"getModelDescription - {modelName} - "
        try:
            if not modelName in jsonData['nodes']:
                return 'There is no model for this relation!'
            model = jsonData['nodes'][modelName]
            if 'description' in model:
                desc = model['description']
                Logger.debug (Pretty.assemble(value=f"{message}Yes", tabCount=Pretty.Indent+1))
                return desc
            else:
                Logger.warning (Pretty.assemble (value=f"{message}No", tabCount=Pretty.Indent+1))
                return ''
        except Exception as error:
            Logger.error (Pretty.assemble_simple (f"Error while retrieving model information for model: {modelName}. Error message: {error}"))
            raise

    def __get_column_description (self, jsonData, modelName, columnName) -> str:
        message = f" - {modelName}.{columnName}"
        try:
            if not columnName in jsonData['nodes'][modelName]['columns']:
                return 'This column is not in the model for this relation!'
            column = jsonData['nodes'][modelName]['columns'][columnName]
            if 'description' in column:
                desc = column['description']
                if len (desc) > 0:
                    Logger.debug (Pretty.assemble (value=f"Yes{message}", tabCount=Pretty.Indent+3))
                    return desc
            Logger.debug (Pretty.assemble (value=f"No{message}", tabCount=Pretty.Indent+3))
            return ''
        except Exception as error:
            Logger.error (Pretty.assemble_simple (f"Error while retrieving model information for {modelName}.{columnName}. Error message: {error}"))
            raise

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def enrich (self) -> None:
        """Enriching dbt test result data with Concept Glossary and Data Dicationary data, along with DB type info"""

        if self._enabled == False:
            Logger.info (Pretty.assemble_simple ("No documentation is enabled so metadata enrichment is not performed."))
            return

        # Generate catalog data
        dbtOperation = Utils.add_dbt_profile_location (["dbt", "docs", "generate"]) #  --fail-fast fjarlægt þar sem dbt rakti dependencies ekki nógu vel
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)

        dbt_output_path = os.path.join (Config.latestPath, "target")
        source_manifest_file = os.path.join (dbt_output_path, "manifest.json")
        source_catalog_file = os.path.join (dbt_output_path, "catalog.json")

        Logger.debug (Pretty.assemble_simple (f"Manifest"))
        Logger.debug (Pretty.assemble (value=f"Source: {source_manifest_file}", tabCount=Pretty.Indent+1))
        Logger.debug (Pretty.assemble (value=f"Target: {Config.dbtManifestFileInfo.qualified_name}", tabCount=Pretty.Indent+1))
                     
        Logger.debug (Pretty.assemble_simple (f"Catalog"))
        Logger.debug (Pretty.assemble (value=f"Source: {source_catalog_file}", tabCount=Pretty.Indent+1))
        Logger.debug (Pretty.assemble (value=f"Target: {Config.dbtCatalogFileInfo.qualified_name}", tabCount=Pretty.Indent+1))        
        
        shutil.copy2 (source_manifest_file, Config.dbtManifestFileInfo.qualified_name)
        shutil.copy2 (source_catalog_file, Config.dbtCatalogFileInfo.qualified_name)
        
        with open (Config.dbtCatalogFileInfo.qualified_name, encoding="utf-8") as json_file:
            catalogJson = json.load (json_file)
        #
        with open (Config.dbtManifestFileInfo.qualified_name, encoding="utf-8") as json_file:
            manifestJson = json.load (json_file)
        
        for relationKey in catalogJson['nodes']:
            relation = catalogJson['nodes'][relationKey]
            schemaName = relation['metadata']['schema']
            tableName = relation['metadata']['name']
            Logger.debug (Pretty.assemble (f"Processing relation: {schemaName}.{tableName}", tabCount=Pretty.Indent))
            relation['metadata']['description'] = self.__get_model_description(manifestJson, relationKey)
            for columnKey in relation['columns']:
                column = relation['columns'][columnKey]
                columnName = column ['name']
                Logger.debug (Pretty.assemble (value=columnName, tabCount=Pretty.Indent+2))
                column['description'] = self.__get_column_description(manifestJson, relationKey, columnKey)
                typeInfoData = self._targetDatabase.get_type_info_column_data (schemaName, tableName, columnName)
                column['database_info'] = typeInfoData
                
                message = f" - {schemaName}.{tableName}.{columnName}"
                try:
                    workingColumnName = "id" if columnName.endswith ("_id") else columnName # Við viljum ekki þurfa að yfirskrifa DD fyrir alla FK.
                    glossaryData = self._conceptGlossary.get_glossary_column_data (schemaName, tableName, workingColumnName).as_dictionary()
                    column['glossary_info'] = glossaryData
                    Logger.debug (Pretty.assemble (value=f"Yes{message}", tabCount=Pretty.Indent+3))
                except Exception as error:
                    Logger.debug (Pretty.assemble (f"No{message}", tabCount=Pretty.Indent+3)) # villan er svæfð!
        
        jsonData = json.dumps(catalogJson, indent=4)
        Utils.write_file (jsonData, Config.enrichedDbtCatalogFileInfo.qualified_name)
        return

def main():
    return MetadataCatalog ().enrich ()

if __name__ == '__main__':
    main()