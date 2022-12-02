from ConceptGlossary.ConceptGlossary import ConceptGlossary, ConceptGlossaryDefinition
from Shared.Logger import Logger
from Shared.Config import Config
from Shared.Decorators import execution_time
from TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory

class ConceptGlossaryRvk (ConceptGlossary):
    glossaryQuery = """
        SELECT
            TABLE_SCHEMA AS schema_name
            ,TABLE_NAME AS relation_name
            ,COLUMN_NAME AS column_name
            ,COALESCE (bg.heiti, '') AS concept_name
            ,COALESCE (bg.lysing, '') AS description
            ,LOWER (COALESCE (dd.typu_id, '')) AS data_type
            ,COALESCE (dd.hamarkslengd, -1) AS max_length
        FROM
            INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN [RVK-DATA-MASTER-API].Nustada.hugtak_utfaersla_V1 dd ON dd.gagnaform_id = 'SQL_SERVER' AND dd.heiti = c.COLUMN_NAME
            LEFT JOIN [RVK-DATA-MASTER-API].Nustada.hugtak_V1 bg ON bg.id = dd.hugtak_id
        WHERE
            c.TABLE_CATALOG = ?
        ORDER BY 
            c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
    """

    def __init__ (self):
        self._databaseName = Config['database']['name']

    @execution_time
    def load_glossary_data (self):
        ConceptGlossaryRvk._glossary = {}
        targetDatabase = TargetDatabaseFactory ().get_target_database ()
        conn = targetDatabase.get_connection ()
        rows = conn.cursor().execute(ConceptGlossaryRvk.glossaryQuery, self._databaseName).fetchall()

        for row in rows:
            schemaName = row.schema_name
            tableName = row.relation_name
            columnName = row.column_name
            
            if not schemaName in ConceptGlossaryRvk._glossary:
                ConceptGlossaryRvk._glossary[schemaName] = { }
            if not tableName in ConceptGlossaryRvk._glossary[schemaName]:
                ConceptGlossaryRvk._glossary[schemaName][tableName] = { }
            ConceptGlossaryRvk._glossary[schemaName][tableName][columnName] = ConceptGlossaryDefinition (row.concept_name, row.description, row.data_type, row.max_length)
        
        Logger.debug (f"\t\tNumber of concept glossary entries: {len (rows)}")
        return
    
    def get_glossary_column_data (self, schemaName, tableName, columnName) -> ConceptGlossaryDefinition:
        if not hasattr (ConceptGlossaryRvk, '_glossary'):
            self.load_glossary_data ()
        return ConceptGlossaryRvk._glossary[schemaName][tableName][columnName] 