-- Drop skipun fyrir allar intermediate töflur, notist sem hluti af deprecation ferli, þær eru endurbyggðar af dbt
SELECT 
	* 
	,CASE WHEN TABLE_TYPE = 'BASE TABLE'
		THEN 'DROP TABLE ' + table_schema + '.' + table_name 
		ELSE 'DROP VIEW ' + table_schema + '.' + table_name 
	END AS drop_cmd
FROM 
	INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%_int' -- Intermedate töflur/módel

-- Drop skipun fyrir snapshots sem eiga sér ekki public töflu
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

-- Byggja dbt hér!

-- Drop skipanir fyrir staging töflur sem eru ekki lengur notaðar!
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

-- Drop skipanir fyrir skemu sem eru ekki lengur notuð. Það munu koma villur á skemu sem innihalda föll
SELECT DISTINCT
	s.SCHEMA_NAME
	,'DROP SCHEMA ' + s.SCHEMA_NAME AS drop_cmd
FROM
	INFORMATION_SCHEMA.SCHEMATA s
	LEFT JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_SCHEMA = s.SCHEMA_NAME
WHERE
	t.TABLE_SCHEMA IS NULL
	AND s.SCHEMA_OWNER = 'dbo' -- Bæta við eftir þörfum!
	AND s.SCHEMA_NAME != 'dbo'
	