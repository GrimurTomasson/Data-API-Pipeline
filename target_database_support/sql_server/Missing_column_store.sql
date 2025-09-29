-- Tables that are not column stores
with cstore AS (
	SELECT
		t.name as table_name
		,s.name AS schema_name
	from   
		sys.indexes i
		join sys.tables t on i.object_id = t.object_id
		join sys.schemas s ON s.schema_id = t.schema_id
	where  
		i.type in (5, 6)
)
SELECT 
	*
	,'CREATE CLUSTERED COLUMNSTORE INDEX ' + LOWER (table_schema) + '_' + LOWER (table_name) + '_cci ON [' + table_schema + '].[' + table_name + ']' AS command
FROM 
	INFORMATION_SCHEMA.TABLES t
WHERE 
	NOT EXISTS (SELECT 1 FROM cstore c WHERE c.schema_name = t.TABLE_SCHEMA AND c.table_name = t.TABLE_NAME)
	AND t.TABLE_TYPE = 'BASE TABLE'
	AND t.TABLE_NAME NOT LIKE '%_stg'