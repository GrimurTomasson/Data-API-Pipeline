-- Relations with max length columns, all columns listed

SELECT 
	* 
	,'ALTER TABLE [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '] ALTER COLUMN [' + COLUMN_NAME + '] nvarchar(250)' AS command
	,'DROP INDEX snapshot_' + TABLE_NAME + '_cci on [' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' AS drop_command
	,'CREATE CLUSTERED COLUMNSTORE INDEX snapshot_' + TABLE_NAME + '_cci on [' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' as create_command
FROM 
	INFORMATION_SCHEMA.COLUMNS
WHERE 
	1=1
	--and table_name like '%_int'
	--and table_name not like '%_stg'
	and CHARACTER_MAXIMUM_LENGTH = -1 -- engin mörk, max
ORDER BY
	TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION


-- Max length of strings in a relation
-- Manually remove the last UNION ALL
DECLARE @db NVARCHAR(200) = 'MASTER-API-PRIVATE'
DECLARE @schema NVARCHAR(200) = 'lukr'
DECLARE @relation NVARCHAR(200) = 'stadfang_v2_int'

SELECT 
	'SELECT ''' + s.COLUMN_NAME + ''' AS column_name, MAX (LEN ([' + s.COLUMN_NAME +  '])) AS max_length FROM [' + @db + '].[' + @schema + '].[' + @relation + ']' + CHAR(9) + 'UNION ALL'
FROM 
	INFORMATION_SCHEMA.COLUMNS s
WHERE 
	s.TABLE_CATALOG = @db
	AND s.TABLE_SCHEMA = @schema
	AND s.TABLE_NAME = @relation
	AND s.DATA_TYPE IN ('nvarchar', 'varchar', 'nchar', 'char')
ORDER BY
	s.ORDINAL_POSITION



alter table Snapshot.adili_einstaklingur_v1 alter column id nvarchar(250)





-- drop index snapshot_adili_heimili_v1_cci on Snapshot.adili_heimili_v1
-- create clustered columnstore index snapshot_adili_heimili_v1_cci on Snapshot.adili_heimili_v1

