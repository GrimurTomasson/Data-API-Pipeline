import json
import copy
from dataclasses import dataclass, field

import Decorators
import APISupport
from SharedDataClasses import CountPercentage

def default_field(obj):
    return field(default_factory=lambda: copy.copy(obj))

@dataclass
class ColumnType:
    name: str = ''
    length: str = ''

@dataclass
class ColumnDescription:
    text: str = ''
    origin: str = ''
    missing: bool = False

@dataclass
class Column:
    name: str = ''
    type: ColumnType = ColumnType()
    description: ColumnDescription = ColumnDescription()

@dataclass 
class Relation:
        schema_name: str = ''
        relation_name: str = ''
        columns: list[Column] = default_field([])

@dataclass
class DocumentationData:
    relations: list[Relation] = default_field([])

class Documentation:

    def __init__ (self) -> None:
        APISupport.initialize ()
        self._docFilename = "api_documentation.md"

    def __get_column_description (self, columnData) -> ColumnDescription:
        if len (columnData['glossary_info']['description']) > 0:
            return ColumnDescription (columnData['glossary_info']['description'], "Hugtök")
        if len (columnData['description']) > 0:
            return ColumnDescription (columnData['description'], "Athugasemd við dálk")
        return ColumnDescription (None, None, True)

    def __generate_documentation (self, enrichedCatalogJson) -> DocumentationData:
        docs = DocumentationData()
        for relationKey in enrichedCatalogJson['sources']:
            relationData = enrichedCatalogJson['sources'][relationKey]
            schemaName = relationData['metadata']['schema']
            relationName = relationData['metadata']['name']
            APISupport.print_v (f"\tSchema: {schemaName} - Relation: {relationName}")
            
            if not schemaName in APISupport.config['public-schemas']: # Það koma með öðrum orðum hvorki öll vensl né dálkar inn
                APISupport.print_v (f"\tNon public schema: {schemaName}")
                continue
            
            relation = Relation (schemaName, relationName)
            for columnKey in relationData['columns']:
                columnData = relationData['columns'][columnKey]
                columnType = ColumnType (columnData['database_info']['type_name'], APISupport.targetDatabaseInterface.get_type_length(columnData))
                relation.columns.append (Column(columnData['name'], columnType, self.__get_column_description (columnData)))
            
            docs.relations.append (relation)
        return docs

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def generate_data (self) -> None:
        """Generating user documentation data"""
        with open (APISupport.enriched_dbt_catalog_file_info.qualified_name, encoding="utf-8") as json_file:
            enrichedCatalogJson = json.load (json_file)
        
        documentation = self.__generate_documentation (enrichedCatalogJson)

        jsonData = json.dumps (documentation, indent=4, cls=APISupport.EnhancedJSONEncoder)
        APISupport.write_file (jsonData, APISupport.api_documentation_data_file_info.qualified_name)
        return 

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def generate_documentation (self) -> None:
        """Generating user documentation"""
        APISupport.generate_markdown_document ("api_documentation_template.md", APISupport.api_documentation_data_file_info.name, self._docFilename, True)
        return

    @Decorators.output_headers(tabCount=1)
    @Decorators.execution_time(tabCount=1)
    def publish (self) -> None:
        """Publishing user documentation"""
        APISupport.get_target_knowledge_base_interface ().publish (self._docFilename, 'user-documentation')
        return

    @Decorators.output_headers
    @Decorators.execution_time
    def generate (self) -> None:
        """Producing user documentation"""
        self.generate_data ()
        self.generate_documentation ()
        self.publish ()
        return

def main ():
    return Documentation ().generate ()

if __name__ == '__main__':
    main ()