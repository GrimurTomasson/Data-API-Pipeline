import json
from dataclasses import dataclass, field

from .Shared.Decorators import post_execution_output
from .Shared.Config import Config
from .Shared.Environment import Environment
from .Shared.Utils import Utils
from .Shared.PrettyPrint import Pretty
from .Shared.LogLevel import LogLevel
from .Shared.Logger import Logger
from .Shared.Json import EnhancedJSONEncoder
from .Shared.DataClasses import CountPercentage, KeyValue
from .TargetKnowledgeBase.TargetKnowledgeBaseFactory import TargetKnowledgeBaseFactory, TargetKnowledgeBase
from .Shared.AuditDecorators import audit
from .Shared.EnvironmentVariable import EnvironmentVariable

@dataclass 
class StatsTotal:
    number_of_relations: int = Utils.default_field (0)
    overwritten_concepts: CountPercentage = None
    number_of_columns: int = Utils.default_field (0)
    ok_columns: CountPercentage = None
    errors: CountPercentage = None
    type_errors: CountPercentage = None
    documentation_errors: CountPercentage = None
    test_coverage_errors: CountPercentage = None

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
    test_coverage_errors: CountPercentage = None

@dataclass
class Stats:
    total: StatsTotal = Utils.default_field (StatsTotal ())
    relation: list[StatsRelation] = Utils.default_field ([])

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
    type: list[Error] = Utils.default_field ([])
    documentation: list[Error] = Utils.default_field ([])
    test_coverage: list[Error] = Utils.default_field ([])
    
@dataclass
class TestCoverage:
    relation: Utils.default_field({})
    column: Utils.default_field({})

@dataclass
class HealthReport: # Root 
    api_name: str
    stats: Stats = Utils.default_field (Stats ())
    overwritten_concepts: list[Concept] = Utils.default_field([])
    errors: Errors = Utils.default_field (Errors ())
    test_coverage: TestCoverage = None

class DefinitionHealthReport:
    def __init__ (self) -> None:
        self._targetKnowledgeBase = TargetKnowledgeBaseFactory ().get_target_knowledge_base ()
        self._reportFilename = "api_definition_health_report.md"
        return

    def __check_for_documentation_error (self, schemaName, relationName, columnName, column) -> Error:
        if len (column['description']) > 0:
            return None
        if 'glossary_info' in column and 'description' in column['glossary_info'] and len (column['glossary_info']['description']) > 0:
            return None
        return Error (schemaName, relationName, columnName, 'Skjölun vantar!')

    def __check_for_concept_overwrite (self, schemaName, relationName, columnName, column) -> Concept:
        if len (column['description']) > 0 and 'glossary_info' in column and 'description' in column['glossary_info'] and len (column['glossary_info']['description']) > 0:
            return Concept (schemaName, relationName, columnName, column['glossary_info']['name'])
        return None

    def __check_for_type_error (self, schemaName, relationName, columnName, column) -> Error:
        if 'glossary_info' not in column or 'data_type' not in column['glossary_info'] or len (column['glossary_info']['data_type']) == 0: # Ekki hugtak úr CG/DD
            return None

        glossaryType = column['glossary_info']['data_type']
        databaseType = column['database_info']['type_name'] if 'database_info' in column and 'type_name' in column['database_info'] else 'UNKNOWN'
        
        if glossaryType != databaseType:
            return Error (schemaName, relationName, columnName, f"Gagnatýpa í skilgreiningu hugtaks: {glossaryType} - Gagnatýpa í grunni: {databaseType}")
        if glossaryType == databaseType and glossaryType in ['char', 'nchar', 'varchar', 'nvarchar'] and column['glossary_info']['max_length'] != column['database_info']['max_length']:
            return Error (schemaName, relationName, columnName, f"Lengd gagnatýpu í skilgreiningu hugtaks: {column['glossary_info']['max_length']} - Lengd í grunni: {column['database_info']['max_length']}")
        return None
    
    def __check_for_column_test_coverage_error (self, database, schema, relation, column) -> Error:
        coverage = 0
        try:
            coverage = self._columnTestMap[database][schema][relation][column]
        except Exception as ex:
            None 
        if coverage > 0:
            return None
        return Error (schema, relation, column, "Engar prófanir skilgreindar fyrir dálk!")

    def __generate_health_data (self, enrichedCatalogJson) -> HealthReport:
        apiHealth = HealthReport (api_name = Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name'))
        relationsTotal = 0 
        columnsTotal = 0
        okColumnsTotal = 0
        
        for relationKey in enrichedCatalogJson['nodes']:
            relation = enrichedCatalogJson['nodes'][relationKey]
            databaseName = relation['metadata']['database']
            schemaName = relation['metadata']['schema']
            relationName = relation['metadata']['name']
            Logger.debug (Pretty.assemble_simple (f"Schema: {schemaName} - Relation: {relationName}"))
            if not schemaName in Config['public-schemas']: # Það koma með öðrum orðum hvorki öll vensl né dálkar inn
                Logger.debug (Pretty.assemble_simple (f"Non public schema: {schemaName}"))
                continue
            # Per relation stats
            relationTypeErrorList = []
            relationDocsErrorList = []
            relationTestCoverageErrorList = []
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
                    
                testError = self.__check_for_column_test_coverage_error (databaseName, schemaName, relationName, columnName)
                if testError is not None:
                    relationTestCoverageErrorList.append (testError)
                
                relationOkColumns += 1 if docError is None and typeError is None and testError is None else 0
                
            relationColumns = len (relation['columns'])
            columnsTotal += relationColumns
            okColumnsTotal += relationOkColumns
            
            overwrittenConcepts = CountPercentage (len (relationOverwrittenConceptList), Utils.to_percentage (len (relationOverwrittenConceptList), relationColumns))
            okColumns = CountPercentage (relationOkColumns, Utils.to_percentage (relationOkColumns, relationColumns))
            combinedErrors = len (relationTypeErrorList) + len (relationDocsErrorList) + len (relationTestCoverageErrorList)
            errors = CountPercentage (combinedErrors, Utils.to_percentage (combinedErrors, relationColumns))
            typeErrors = CountPercentage (len (relationTypeErrorList), Utils.to_percentage (len (relationTypeErrorList), relationColumns))
            docErrors = CountPercentage (len (relationDocsErrorList), Utils.to_percentage (len (relationDocsErrorList), relationColumns))
            testErrors = CountPercentage (len (relationTestCoverageErrorList), Utils.to_percentage (len (relationTestCoverageErrorList), relationColumns))
            relationStats = StatsRelation (schemaName, relationName, relationColumns, overwrittenConcepts, okColumns, errors, typeErrors, docErrors, testErrors)
            apiHealth.stats.relation.append (relationStats) 
            
            apiHealth.overwritten_concepts.extend (relationOverwrittenConceptList)
            apiHealth.errors.type.extend (relationTypeErrorList)
            apiHealth.errors.documentation.extend (relationDocsErrorList)
            apiHealth.errors.test_coverage.extend (relationTestCoverageErrorList)
        
        # Röðun niðurstaða
        apiHealth.overwritten_concepts = sorted (apiHealth.overwritten_concepts, key=lambda x: x.schema_name + x.relation_name + x.column_name)
        apiHealth.errors.type = sorted (apiHealth.errors.type, key=lambda x: x.schema_name + x.relation_name + x.column_name)
        apiHealth.errors.documentation = sorted (apiHealth.errors.documentation, key=lambda x: x.schema_name + x.relation_name + x.column_name)
        apiHealth.errors.test_coverage = sorted (apiHealth.errors.test_coverage, key=lambda x: x.schema_name + x.relation_name + x.column_name)
        apiHealth.stats.relation = sorted (apiHealth.stats.relation, key=lambda x: x.schema_name + x.relation_name)
        
        oaOverwrittenConcepts = CountPercentage (len (apiHealth.overwritten_concepts), Utils.to_percentage (len (apiHealth.overwritten_concepts), columnsTotal))
        oaOkColumns = CountPercentage (okColumnsTotal, Utils.to_percentage (okColumnsTotal, columnsTotal))
        
        oaErrorCount = len (apiHealth.errors.type) + len (apiHealth.errors.documentation) + len (apiHealth.errors.test_coverage)
        oaErrors = CountPercentage (oaErrorCount, Utils.to_percentage (oaErrorCount, columnsTotal))
        
        oaTypeErrors = CountPercentage (len (apiHealth.errors.type), Utils.to_percentage (len (apiHealth.errors.type), columnsTotal))
        oaDocErrors = CountPercentage (len (apiHealth.errors.documentation), Utils.to_percentage (len (apiHealth.errors.documentation), columnsTotal))
        oaTestErrors = CountPercentage (len (apiHealth.errors.test_coverage), Utils.to_percentage (len (apiHealth.errors.test_coverage), columnsTotal))
        
        apiHealth.stats.total = StatsTotal (relationsTotal, oaOverwrittenConcepts, columnsTotal, oaOkColumns, oaErrors, oaTypeErrors, oaDocErrors, oaTestErrors)

        return apiHealth
    
    def __get_test_relation_name (self, node) -> str:
        if len (node["refs"]) == 0:
            Logger.error (Pretty.assemble_simple (f"No refs in test node: {node}"))
            return None
        
        refs = node["refs"]
        #print (f"{refs} - len: {len(refs)} - unique_id: {node['unique_id']}")
        
        if len (refs) == 1:
            return refs[0][0]
        unique_id = node["unique_id"]
        
        if len (refs) > 1:
            name_index = {}
            for ref in refs:
                name_index[ref[0]] = unique_id.index (ref[0])
            return min (name_index, key=name_index.get)
        
    def __get_test_schema_name (self, database: str, relationName: str, node) -> str:
        if not 'compiled_code' in node or database is None or relationName is None:
            return None
        
        code = node["compiled_code"]
        #print (Pretty.Separator)
        #print (node)
        end = code.find ('."' + relationName) 
        while end < 0 and relationName.find ("_") > -1: # Vensl hafa verið endurskýrð, við höfum ekki endanlegt nafn en almennt er það án kerfis forskeytis módels. Þetta hangir á nafnahefð :(
            tempRelationName = relationName[relationName.find ("_") + 1 :]
            #print (f"name: {relationName} - cut down name: {tempRelationName}")
            end = code.find ('."' + tempRelationName)

        codeTemp = code[:end]
        start = codeTemp.rfind (database + '".') + len (database) + 1 # The one closest to our table name
        schema = codeTemp[start:end].strip (".\"")
        #print (f"relation: {relationName} - schema: {schema} - start: {start} - end: {end}")
        #if schema.find(".") > 0:
         #   print (f"code: {code}")
        return schema
    
    def __init_dictionary_map(self, map: {}, keys: [], base_value) -> None: # Ætti ekki að vera hér, almennt fall
        curr_map = map
        lastIdx = len (keys) - 1
        for idx, key in enumerate (keys):
            if key not in curr_map:
                if idx != lastIdx:
                    curr_map[key] = {}
                else:
                    curr_map[key] = base_value        
            if idx != lastIdx:
                curr_map = curr_map[key]
            
    @post_execution_output
    def generate_test_data (self, dbtManifest) -> None:
        relationTestMap = {}
        columnTestMap = {} 
        for relationKey in dbtManifest['nodes']:
            node = dbtManifest['nodes'][relationKey]
            if node["resource_type"] != "test":
                continue

            print (node["name"])
            
            database = node["database"]
            relation_name = self.__get_test_relation_name (node)
            schema_name = self.__get_test_schema_name (database, relation_name, node)
            column_name = node["column_name"] # multi-column tests at least have None as a value.

            print (f"   {relation_name}")
            print (f"   {schema_name}")
            print (f"   {column_name}")
            
            if column_name is not None: 
                self.__init_dictionary_map (columnTestMap, [database, schema_name, relation_name, column_name], 0)
                columnTestMap[database][schema_name][relation_name][column_name] += 1
            
            self.__init_dictionary_map (relationTestMap, [database, schema_name, relation_name], 0)
            relationTestMap[database][schema_name][relation_name] += 1
            
        self._relationTestMap = relationTestMap
        self._columnTestMap = columnTestMap

    @post_execution_output (logLevel=LogLevel.INFO)
    def generate_data (self) -> None:
        """Generating definition health report data"""
        with open (Config.enrichedDbtCatalogFileInfo.qualified_name, encoding="utf-8") as json_file:
            enrichedCatalogJson = json.load(json_file)
            
        with open (Config.dbtManifestFileInfo.qualified_name, encoding="utf-8") as json_file:
            dbtManifest = json.load(json_file)

        self.generate_test_data (dbtManifest)
        apiHealth = self.__generate_health_data (enrichedCatalogJson)
        apiHealth.test_coverage = TestCoverage (self._relationTestMap, self._columnTestMap)
        jsonData = json.dumps (apiHealth, indent=4, cls=EnhancedJSONEncoder)
        Utils.write_file (jsonData, Config.apiDefinitionHealthReportDataFileInfo.qualified_name) 
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    def generate_report (self) -> None:
        """Generating definition health report"""
        metadata = Utils.retrieve_variable ('Definition metadata', EnvironmentVariable.knowledgebaseDefinitionHealthReportMetadata, Config['documentation']['definition-health-report'], 'metadata', optional=True)
        Utils.generate_markdown_document ("api_definition_health_report_template.md", Config.apiDefinitionHealthReportDataFileInfo.name, self._reportFilename, metadata)
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    def publish (self) -> None:
        """Publishing definition health report"""
        self._targetKnowledgeBase.publish (self._reportFilename, 'definition-health-report')
        return

    @post_execution_output (logLevel=LogLevel.INFO)
    @audit
    def generate (self) -> None:
        """Producing a definition health report"""

        if Config['documentation']['definition-health-report']['generate'] != True:
            return
            
        self.generate_data ()
        self.generate_report ()
        self.publish ()
        return

def main():
    return DefinitionHealthReport ().generate ()

if __name__ == '__main__':
    main()

