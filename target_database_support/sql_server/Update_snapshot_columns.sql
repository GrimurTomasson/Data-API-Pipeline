-- breytilegt
DECLARE @public_db nvarchar(100) = '[SFS-API]'
DECLARE @snapshot_db nvarchar(100) = '[SFS-API-PRIVATE]'
DECLARE @relation nvarchar(100) = 'adili_v1' 

-- mögulega breytilegt
DECLARE @source_schema nvarchar(100) = 'Nustada' 

-- óbreytilegt
DECLARE @snapshot_schema nvarchar(100) = 'dbt_snapshots' 
DECLARE @temp_snapshot_new nvarchar(100) = @relation + '_new'
DECLARE @temp_snapshot_old nvarchar(100) = @relation + '_old'

DECLARE @column_list nvarchar(500)
DECLARE @sql nvarchar(max)
DECLARE @out_param nvarchar(250) = '@column_list_out nvarchar(500) OUTPUT'

-- Sækjum dálkalista með nýjum dálkum
SET @sql = 'WITH missing_columns AS (
	SELECT c.*
	FROM ' + @public_db + '.INFORMATION_SCHEMA.COLUMNS c 
	WHERE c.TABLE_SCHEMA = ''' + @source_schema + ''' AND c.TABLE_NAME = ''' + @relation + '''
	AND c.COLUMN_NAME NOT IN (SELECT s.column_name FROM ' + @snapshot_db + '.INFORMATION_SCHEMA.COLUMNS s WHERE s.TABLE_SCHEMA = ''' + @snapshot_schema + ''' AND s.TABLE_NAME = c.TABLE_NAME)
),
samsett AS (
	SELECT s.column_name, s.ORDINAL_POSITION FROM ' + @public_db + '.INFORMATION_SCHEMA.COLUMNS s 
	WHERE s.TABLE_SCHEMA = ''' + @source_schema + ''' AND s.TABLE_NAME = ''' + @relation + ''' AND s.COLUMN_NAME NOT IN (SELECT column_name FROM missing_columns)
	
	UNION ALL
	
	SELECT ''CAST (NULL AS '' + c.data_type + (CASE WHEN c.data_type IN (''char'',''nchar'',''varchar'',''nvarchar'') THEN ''('' + CAST (c.character_maximum_length AS nvarchar(10)) + '')'' ELSE '''' END) + '') AS '' + c.column_name, C.ORDINAL_POSITION
	FROM missing_columns c

	UNION ALL 

	SELECT s.column_name, s.ORDINAL_POSITION + 200 AS ordinal_position
	FROM ' + @snapshot_db + '.INFORMATION_SCHEMA.COLUMNS s 
	WHERE s.TABLE_SCHEMA = ''' + @snapshot_schema + ''' AND s.TABLE_NAME = ''' + @relation + ''' AND s.COLUMN_NAME LIKE ''dbt_%''
)
SELECT 
	@column_list_out = STRING_AGG (column_name, '', '') WITHIN GROUP (ORDER BY ordinal_position)
FROM
	samsett'
EXEC sp_executesql @sql, @out_param, @column_list_out = @column_list OUTPUT

-- Command creation
SELECT 'Use snapshot db' AS description, 'USE ' + @snapshot_db AS command
UNION ALL
SELECT 'Create a new snapshot table, with data', 'SELECT ' + COALESCE (@column_list, 'COLUMN LIST ERROR') + ' INTO ' + @snapshot_schema + '.' + @temp_snapshot_new + ' FROM ' + @snapshot_schema + '.' + @relation
UNION ALL 
SELECT 'Check out the new snapshot', 'SELECT * FROM '+ @snapshot_schema + '.' + @temp_snapshot_new
UNION ALL
SELECT 'Rename current snapshot', 'EXEC sp_rename ''' + @snapshot_schema + '.' + @relation + ''', ''' + @temp_snapshot_old + ''''
UNION ALL
SELECT 'Swap the new snapshot in', 'EXEC sp_rename ''' + @snapshot_schema + '.' + @temp_snapshot_new + ''', ''' + @relation + ''''
UNION ALL
SELECT 'Drop the old snapshot, AFTER review', 'DROP TABLE ' + @snapshot_schema + '.' + @temp_snapshot_old
