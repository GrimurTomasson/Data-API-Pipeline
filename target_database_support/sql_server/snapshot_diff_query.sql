USE [REFERENCE-API-PRIVATE]

DECLARE @schema VARCHAR(100) = 'dbt_snapshots'
DECLARE @snapshot VARCHAR(100) = 'adili_einstaklingur_v1'

DECLARE @columnName VARCHAR(100)
DECLARE @columnLength int

DECLARE @leadOrderBy VARCHAR(300) = 'ORDER BY s.dbt_valid_from'
DECLARE @commaPrefix VARCHAR(1) = ''

DECLARE columnCursor CURSOR FOR
	SELECT
		c.COLUMN_NAME, COALESCE (c.CHARACTER_MAXIMUM_LENGTH, 100)
	FROM 
		INFORMATION_SCHEMA.COLUMNS c 
	WHERE 
		c.TABLE_SCHEMA = @schema
		AND c.TABLE_NAME = @snapshot
	ORDER BY 
		ORDINAL_POSITION

PRINT (
'WITH gogn AS (
	SELECT')

OPEN columnCursor
WHILE 1=1 BEGIN
	FETCH NEXT FROM columnCursor INTO @columnName, @columnLength
	IF @@FETCH_STATUS <> 0
		BREAK
	--PRINT (CHAR(9) + CHAR(9) + @columnName)
	PRINT (
'		'+ @commaPrefix + 'COALESCE (CAST (s.' + @columnName + ' AS VARCHAR(' + CAST (COALESCE (@columnLength, 100) AS varchar(6)) + ')), ''NULL'') AS ' + @columnName + CHAR(10) + 
'		,COALESCE (CAST (LAG (s.' + @columnName + ') OVER (' + @leadOrderBy + ') AS VARCHAR(' + CAST (COALESCE (@columnLength, 100) AS varchar(6)) + ')), ''NULL'') AS ' + @columnName + '_old')
	SET @commaPrefix = ','
END
CLOSE columnCursor
DEALLOCATE columnCursor

PRINT (
'	
		,CASE WHEN LAG (s.dbt_valid_from) OVER (ORDER BY s.dbt_valid_from) IS NULL THEN 1 ELSE 0 END AS oldest
	FROM
		[' + @schema + '].[' + @snapshot + '] s
	WHERE
		s.some_unique_id = ''''
	)
SELECT 
	s.dbt_valid_from
	,s.dbt_valid_from_old
	' )

DECLARE changeCursor CURSOR FOR
	SELECT
		c.COLUMN_NAME
	FROM 
		INFORMATION_SCHEMA.COLUMNS c 
	WHERE 
		c.TABLE_SCHEMA = @schema
		AND c.TABLE_NAME = @snapshot
		AND c.COLUMN_NAME NOT LIKE 'dbt_%'
	ORDER BY 
		ORDINAL_POSITION

OPEN changeCursor
WHILE 1=1 BEGIN
	FETCH NEXT FROM changeCursor INTO @columnName
	IF @@FETCH_STATUS <> 0
		BREAK
	PRINT (
--'	,CASE WHEN s.' + @columnName + '_old != ' + @columnName + '_new THEN s.' + @columnName + '_old' + ' + '' -> '' + ' + @columnName + '_new ELSE CASE WHEN s.oldest = 1 THEN s.' + @columnName + '_old ELSE '''' END END AS ' + @columnName)
'	,CASE WHEN s.dbt_valid_from_old = ''NULL'' THEN s.' + @columnName + ' ELSE CASE WHEN s.' + @columnName + ' != s.' + @columnName + '_old THEN s.' + @columnName + '_old + '' -> '' + s.' + @columnName + ' ELSE '''' END END AS ' + @columnName)
-- ,CASE WHEN s.dbt_valid_from_old = 'NULL' THEN s.stadfang_id ELSE CASE WHEN s.stadfang_id != s.stadfang_id_old THEN s.stadfang_id_old + ' -> ' + s.stadfang_id ELSE '' END END AS stadfang_id
END
CLOSE changeCursor
DEALLOCATE changeCursor

PRINT ('FROM 
	gogn s
ORDER BY
	s.dbt_valid_from DESC -- Nýjast fyrst') 
