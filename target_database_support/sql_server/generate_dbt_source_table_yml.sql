DECLARE @db VARCHAR(MAX) = 'THON-API-PRIVATE'
DECLARE @schema VARCHAR(MAX) = 'umsoknastjorinn'
DECLARE @table VARCHAR(MAX) = 'applications_stg'

DECLARE @newline CHAR(1) = CHAR(10)
DECLARE @scopeOne VARCHAR(20) = '  ' 
DECLARE @scopeTwo VARCHAR(20) = @scopeOne + @scopeOne
DECLARE @scopeThree VARCHAR(20) = @scopeTwo + @scopeOne
DECLARE @scopeFour VARCHAR(20) = @scopeTwo + @scopeTwo
DECLARE @scopeFive VARCHAR(20) = @scopeThree + @scopeTwo
DECLARE @scopeSix VARCHAR(20) = @scopeThree + @scopeThree

DECLARE @definition VARCHAR(MAX) = 
'version: 2' + @newline + @newline + 
'sources:' + @newline + 
@scopeOne + '- name: ' + @schema + @newline +
@scopeTwo + 'database: "{{ var (''private-database'') }}"' + @newline +
@scopeTwo + 'schema: ' + '"{{ var (''' + @schema + '-schema'') }}"' + @newline +
@scopeTwo + 'tables:' + @newline +
@scopeThree + '- name: ' + @table + @newline +
@scopeFour + 'description: |' + @newline +
@scopeFive + 'Lýsing...  ' + @newline + @newline +
@scopeFive + '**Uppruni**: ' + @newline +
@scopeFive + '**Keyrsla**: ' + @newline +
@scopeFour + 'columns:' + @newline

SET @definition = @definition + (
	SELECT 
		STRING_AGG (@scopeFive + '- name: ' + s.COLUMN_NAME + @newline + @scopeSix + 'description: ''''', @newline)
	FROM 
		INFORMATION_SCHEMA.COLUMNS s
	WHERE
		s.TABLE_CATALOG = @db 
		AND s.TABLE_SCHEMA = @schema
		AND s.TABLE_NAME = @table
)

SELECT @definition


