import os
import json
import copy
from dataclasses import dataclass, field

import Decorators
import APISupport
from SharedDataClasses import CountPercentage

def default_field(obj):
        return field(default_factory=lambda: copy.copy(obj))

@dataclass 
class StatsTotal:
    number_of_relations: int = 0
    overwritten_concepts: CountPercentage = None
    number_of_columns: int = 0
    ok_columns: CountPercentage = None
    errors: CountPercentage = None
    type_errors: CountPercentage = None
    documentation_errors: CountPercentage = None

@dataclass
class StatsRelation:
    schema_name: str
    relation_name: str
    number_of_columns: int = 0
    overwritten_concepts: CountPercentage = None
    ok_columns: CountPercentage = None
    errors: CountPercentage = None
    type_errors: CountPercentage = None
    documentation_errors: CountPercentage = None

@dataclass
class Stats:
    total: StatsTotal = StatsTotal()
    relation: list[StatsRelation] = default_field([])

@dataclass
class Concept:
    schema_name: str
    relation_name: str
    column_name: str
    concept_name: str

@dataclass
class Error:
    schema_name: str
    relation_name: str
    column_name: str
    message: str

@dataclass
class Errors:
    type: list[Error] = default_field([])
    documentation: list[Error] = default_field([])

@dataclass
class HealthReport: # Root 
    api_name: str
    stats: Stats = Stats()
    overwritten_concepts: list[Concept] = default_field([])
    errors: Errors = Errors()

class DefinitionHealthReport:

    def __init__ (self) -> None:
        APISupport.initialize ()
        self._reportFilename = "api_definition_health_report.md"
        return

    def __check_for_documentation_error (self, schemaName, relationName, columnName, column) -> Error:
        if len (column['description']) == 0 and len (column['glossary_info']['description']) == 0:
            return Error (schemaName, relationName, columnName, 'Skjölun vantar!')
        return None

    def __check_for_concept_overwrite (self, schemaName, relationName, columnName, column) -> Concept:
        if len (column['description']) > 0 and len (column['glossary_info']['description']) > 0:
            return Concept (schemaName, relationName, columnName, column['glossary_info']['concept_name'])
        return None

    def __check_for_type_error (self, schemaName, relationName, columnName, column) -> Error:
        if len (column['glossary_info']['data_type']) == 0: # Ekki hugtak úr CG/DD
            return None

        glossaryType = column['glossary_info']['data_type']
        databaseType = column['database_info']['type_name']
        if glossaryType != databaseType:
            return Error (schemaName, relationName, columnName, f"Gagnatýpa í skilgreiningu hugtaks: {glossaryType} - Gagnatýpa í grunni: {databaseType}")
        if glossaryType == databaseType and glossaryType in ['char', 'nchar', 'varchar', 'nvarchar'] and column['glossary_info']['max_length'] != column['database_info']['max_length']:
            return Error (schemaName, relationName, columnName, f"Lengd gagnatýpu í skilgreiningu hugtaks: {column['glossary_info']['max_length']} - Lengd í grunni: {column['database_info']['max_length']}")
        return None

    def __generate_health_data (self, enrichedCatalogJson) -> HealthReport:
        apiHealth = HealthReport (api_name = APISupport.config['database']['name'])
        relationsTotal = 0 
        columnsTotal = 0
        okColumnsTotal = 0
        
        for relationKey in enrichedCatalogJson['sources']:
            relation = enrichedCatalogJson['sources'][relationKey]
            schemaName = relation['metadata']['schema']
            relationName = relation['metadata']['name']
            APISupport.print_v (f"\tSchema: {schemaName} - Relation: {relationName}")
            if not schemaName in APISupport.config['public-schemas']: # Það koma með öðrum orðum hvorki öll vensl né dálkar inn
                APISupport.print_v (f"\tNon public schema: {schemaName}")
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
                
                docError = self.__check_for_documentation_error (schemaName, relationName, columnName, column)
                if docError is not None:
                    relationDocsErrorList.append (docError)
                
                conceptOverwrite = self.__check_for_concept_overwrite (schemaName, relationName, columnName, column)
                if conceptOverwrite is not None:
                    relationOverwrittenConceptList.append (conceptOverwrite)
                
                typeError = self.__check_for_type_error (schemaName, relationName, columnName, column) # Skrifa þetta fall, og líka föll fyrir hinar villurnar!
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

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def generate_data (self) -> None:
        """Generating definition health report data"""
        with open (APISupport.enriched_dbt_catalog_file_info.qualified_name, encoding="utf-8") as json_file:
            enrichedCatalogJson = json.load(json_file)

        apiHealth = self.__generate_health_data (enrichedCatalogJson)
        jsonData = json.dumps (apiHealth, indent=4, cls=APISupport.EnhancedJSONEncoder)
        APISupport.write_file (jsonData, APISupport.api_definition_health_report_data_file_info.qualified_name) 
        return

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def generate_report (self) -> None:
        """Generating definition health report"""
        APISupport.generate_markdown_document ("api_definition_health_report_template.md", APISupport.api_definition_health_report_data_file_info.name, self._reportFilename)
        return

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def publish (self) -> None:
        """Publishing definition health report"""
        APISupport.get_target_knowledge_base_interface ().publish (self._reportFilename, 'definition-health-report')
        return

    @Decorators.output_headers
    @Decorators.execution_time
    def generate (self) -> None:
        """Producing a definition health report"""
        self.generate_data ()
        self.generate_report ()
        self.publish ()
        return

def main():
    return DefinitionHealthReport ().generate ()

if __name__ == '__main__':
    main()

