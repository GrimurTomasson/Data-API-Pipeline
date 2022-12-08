import json
import copy
from dataclasses import dataclass, field

from Shared.Decorators import output_headers, execution_time
from Shared.Config import Config
from Shared.Utils import Utils
from Shared.Logger import Logger
from Shared.Json import EnhancedJSONEncoder
from Shared.DataClasses import CountPercentage
from TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory
from TargetKnowledgeBase.TargetKnowledgeBaseFactory import TargetKnowledgeBaseFactory

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
class DocumentationData: # Naming collision without the Data postfix
    relations: list[Relation] = default_field([])

class Documentation:

    def __init__ (self) -> None:
        self._targetDatabase = TargetDatabaseFactory ().get_target_database()
        self._targetKnowledgeBase = TargetKnowledgeBaseFactory ().get_target_knowledge_base()
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
            Logger.debug (f"\tSchema: {schemaName} - Relation: {relationName}")
            
            if not schemaName in Config['public-schemas']: # Það koma með öðrum orðum hvorki öll vensl né dálkar inn
                Logger.debug (f"\tNon public schema: {schemaName}")
                continue
            
            relation = Relation (schemaName, relationName)
            for columnKey in relationData['columns']:
                columnData = relationData['columns'][columnKey]
                columnType = ColumnType (columnData['database_info']['type_name'], self._targetDatabase.get_type_length(columnData))
                relation.columns.append (Column(columnData['name'], columnType, self.__get_column_description (columnData)))
            
            docs.relations.append (relation)
        return docs

    @output_headers(tabCount=1)
    @execution_time(tabCount=1)
    def generate_data (self) -> None:
        """Generating user documentation data"""
        with open (Config.enrichedDbtCatalogFileInfo.qualified_name, encoding="utf-8") as json_file:
            enrichedCatalogJson = json.load (json_file)
        
        documentation = self.__generate_documentation (enrichedCatalogJson)

        jsonData = json.dumps (documentation, indent=4, cls=EnhancedJSONEncoder)
        Utils.write_file (jsonData, Config.apiDocumentationDataFileInfo.qualified_name)
        return 

    @output_headers(tabCount=1)
    @execution_time(tabCount=1)
    def generate_documentation (self) -> None:
        """Generating user documentation"""
        Utils.generate_markdown_document ("api_documentation_template.md", Config.apiDocumentationDataFileInfo.name, self._docFilename, True)
        return

    @output_headers(tabCount=1)
    @execution_time(tabCount=1)
    def publish (self) -> None:
        """Publishing user documentation"""
        self._targetKnowledgeBase.publish (self._docFilename, 'user-documentation')
        return

    @output_headers
    @execution_time
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