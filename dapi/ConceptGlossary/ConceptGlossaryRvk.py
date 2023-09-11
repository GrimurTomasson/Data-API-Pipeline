from .ConceptGlossary import ConceptGlossary, ConceptGlossaryDefinition
from ..Shared.Environment import Environment
from ..Shared.Utils import Utils
from ..Shared.PrettyPrint import Pretty
from ..Shared.Logger import Logger
from ..Shared.Config import Config
from ..Shared.Decorators import post_execution_output
from ..TargetDatabase.TargetDatabaseFactory import TargetDatabaseFactory

class ConceptGlossaryRvk (ConceptGlossary):
    glossaryQuery = """
WITH hugtak AS (
    SELECT
		bg.heiti AS heiti_hugtaks
		,bg.lysing
		,dd.hugtak_id
		,dd.heiti AS heiti_dalks
		,dd.typu_id
		,dd.hamarkslengd
	FROM
		[MASTER-API].Nustada@Hugtak.hugtak_utfaersla dd 
		JOIN [MASTER-API].Nustada@Hugtak.hugtak bg ON bg.id = dd.hugtak_id
	WHERE
		dd.gagnaform_id = 'SQL_SERVER'
)
,dalkar AS (
	SELECT 
		TABLE_SCHEMA AS schema_name
		,TABLE_NAME AS relation_name
		,COLUMN_NAME AS column_name
	FROM
		INFORMATION_SCHEMA.COLUMNS 
)
,moguleikar AS (
	SELECT 1 AS radnumer, * FROM hugtak h JOIN dalkar d ON d.column_name = h.heiti_dalks -- Rétt hugtak
	UNION ALL
	SELECT 2, * FROM hugtak h JOIN dalkar d ON d.column_name = h.heiti_dalks + '_id' -- Rétt hugtak með lykilsendingu
	UNION ALL
	SELECT 3, * FROM hugtak h JOIN dalkar d ON d.column_name LIKE h.heiti_dalks + '%' AND d.column_name != h.heiti_dalks AND LEN (h.heiti_dalks) > 2 -- Hugtak er forskeyti dálks, lengdartakmörkun er út af ID
	UNION ALL
	SELECT 4, * FROM hugtak h JOIN dalkar d ON d.column_name LIKE '%' + h.heiti_dalks AND d.column_name != h.heiti_dalks AND LEN (h.heiti_dalks) > 2  -- Hugtak er viðskeyti dálks
	UNION ALL
	SELECT 5, * FROM hugtak h JOIN dalkar d ON d.column_name LIKE '%' + h.heiti_dalks + '_id' AND d.column_name != h.heiti_dalks AND LEN (h.heiti_dalks) > 2  -- Hugtak er viðskeyti dálks, sem hefur id endingu (lykill)
)
,besti_moguleiki AS (
	SELECT DISTINCT
		FIRST_VALUE (s.schema_name) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer) AS schema_name
		,FIRST_VALUE (s.relation_name) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer) AS relation_name
		,FIRST_VALUE (s.column_name) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer) AS column_name
		,FIRST_VALUE (s.heiti_hugtaks) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer) AS concept_name
		,FIRST_VALUE (s.lysing) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer) AS description
		,LOWER (FIRST_VALUE (s.typu_id) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer)) AS data_type
		,FIRST_VALUE (s.hamarkslengd) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer) AS max_length
		,FIRST_VALUE (s.radnumer) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer) AS match_type_id
		,FIRST_VALUE (s.heiti_dalks) OVER (PARTITION BY s.schema_name, s.relation_name, s.column_name ORDER BY s.radnumer) AS matched_column_name
	FROM
		moguleikar s
)
SELECT
	d.schema_name, d.relation_name, d.column_name
	,COALESCE (b.concept_name, '') AS concept_name
	,COALESCE (b.description, '') AS description
	,COALESCE (b.data_type, '') AS data_type
	,COALESCE (b.max_length, -1) AS max_length
	,COALESCE (b.match_type_id, '') AS match_type_id
	,COALESCE (b.matched_column_name, '') AS matched_column_name
FROM 
	dalkar d
	LEFT JOIN besti_moguleiki b ON b.schema_name = d.schema_name AND b.relation_name = d.relation_name AND b.column_name = d.column_name
ORDER BY
	1, 2, 3

    """

    def __init__ (self):
        self._databaseName = Utils.retrieve_variable ('Database name', Environment.databaseName, Config['database'], 'name')

    @post_execution_output
    def load_glossary_data (self):
        ConceptGlossaryRvk._glossary = {}
        targetDatabase = TargetDatabaseFactory ().get_target_database ()
        conn = targetDatabase.get_connection ()
        rows = conn.cursor().execute(ConceptGlossaryRvk.glossaryQuery).fetchall()

        for row in rows:
            schemaName = row.schema_name
            tableName = row.relation_name
            columnName = row.column_name
            
            if not schemaName in ConceptGlossaryRvk._glossary:
                ConceptGlossaryRvk._glossary[schemaName] = { }
            if not tableName in ConceptGlossaryRvk._glossary[schemaName]:
                ConceptGlossaryRvk._glossary[schemaName][tableName] = { }
            ConceptGlossaryRvk._glossary[schemaName][tableName][columnName] = ConceptGlossaryDefinition (row.concept_name, row.description, row.data_type, row.max_length)
        
        Logger.debug (Pretty.assemble_simple (f"Number of concept glossary entries: {len (rows)}"))
        return
    
    def get_glossary_column_data (self, schemaName, tableName, columnName) -> ConceptGlossaryDefinition:
        if not hasattr (ConceptGlossaryRvk, '_glossary'):
            self.load_glossary_data ()
        return ConceptGlossaryRvk._glossary[schemaName][tableName][columnName] 