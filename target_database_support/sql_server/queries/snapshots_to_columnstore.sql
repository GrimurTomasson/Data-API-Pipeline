WITH column_stores AS (
	SELECT 
		OBJECT_SCHEMA_NAME(OBJECT_ID) SchemaName
		,OBJECT_NAME(OBJECT_ID) TableName
		,i.name AS IndexName
		,i.type_desc IndexType
	FROM 
		sys.indexes AS i 
	WHERE 
		is_hypothetical = 0 
		AND i.index_id <> 0 
		AND i.type_desc IN ('CLUSTERED COLUMNSTORE','NONCLUSTERED COLUMNSTORE')
)
SELECT 
	s.*
	,'CREATE CLUSTERED COLUMNSTORE INDEX ' + LOWER(s.TABLE_SCHEMA) + '_' + LOWER(s.TABLE_NAME) + '_cci ON [' + s.TABLE_SCHEMA + '].[' + s.TABLE_NAME + ']'
FROM 
	INFORMATION_SCHEMA.TABLES s
WHERE
	s.TABLE_SCHEMA LIKE 'Private_snapshot%'
	AND NOT EXISTS (SELECT 1 FROM column_stores c WHERE c.SchemaName = s.TABLE_SCHEMA AND c.TableName = s.TABLE_NAME)
