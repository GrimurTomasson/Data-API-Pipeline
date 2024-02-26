-- Drop skipun fyrir allar intermediate t�flur, notist sem hluti af deprecation ferli, ��r eru endurbygg�ar af dbt
SELECT 
	* 
	,CASE WHEN TABLE_TYPE = 'BASE TABLE'
		THEN 'DROP TABLE ' + table_schema + '.' + table_name 
		ELSE 'DROP VIEW ' + table_schema + '.' + table_name 
	END AS drop_cmd
FROM 
	INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%_int' -- Intermedate t�flur/m�del

-- Drop skipun fyrir snapshots sem eiga s�r ekki public t�flu
SELECT 
	* 
	,CASE WHEN TABLE_TYPE = 'BASE TABLE'
		THEN 'DROP TABLE ' + table_schema + '.' + table_name 
		ELSE 'DROP VIEW ' + table_schema + '.' + table_name 
	END AS drop_cmd
FROM 
	INFORMATION_SCHEMA.TABLES t
WHERE 
	t.TABLE_SCHEMA = 'Snapshot'
	AND t.TABLE_NAME NOT IN (SELECT TABLE_NAME FROM [MASTER-API].INFORMATION_SCHEMA.TABLES p ) -- Ath. skipta um grunn fyrir public!

-- Byggja dbt h�r!

-- Drop skipanir fyrir staging t�flur sem eru ekki lengur nota�ar!
SELECT 
	* 
	,CASE WHEN TABLE_TYPE = 'BASE TABLE'
		THEN 'DROP TABLE ' + table_schema + '.' + table_name 
		ELSE 'DROP VIEW ' + table_schema + '.' + table_name 
	END AS drop_cmd
FROM 
	INFORMATION_SCHEMA.TABLES t
WHERE 
	t.TABLE_NAME LIKE '%_stg'
	AND REPLACE (t.TABLE_NAME, '_stg', '_int') NOT IN (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES)

-- Drop skipanir fyrir skemu sem eru ekki lengur notu�. �a� munu koma villur � skemu sem innihalda f�ll
SELECT DISTINCT
	s.SCHEMA_NAME
	,'DROP SCHEMA ' + s.SCHEMA_NAME AS drop_cmd
FROM
	INFORMATION_SCHEMA.SCHEMATA s
	LEFT JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_SCHEMA = s.SCHEMA_NAME
WHERE
	t.TABLE_SCHEMA IS NULL
	AND s.SCHEMA_OWNER = 'dbo' -- B�ta vi� eftir ��rfum!
	AND s.SCHEMA_NAME != 'dbo'
	