DECLARE @schema nvarchar(100) = 's5'
DECLARE @relation nvarchar(100) = 'fatladir_umsoknir_int'
SELECT 
	'WITH gogn AS (' + CHAR(10) 
	+ + CHAR(9) + 'SELECT CONCAT_WS (''||'', [' + STRING_AGG (column_name, '],[') WITHIN GROUP (ORDER BY ordinal_position) + ']) AS rod' + CHAR(10) 
	+ CHAR(9) + 'FROM [' + @schema + '].[' + @relation + ']' + CHAR(10)
	+ ')' + CHAR(10)
	+ 'SELECT rod FROM gogn WHERE' + CHAR(10)
	+ 'rod LIKE ''%smu%'''
FROM 
	INFORMATION_SCHEMA.COLUMNS 
WHERE 
	TABLE_SCHEMA = @schema 
	AND TABLE_NAME = @relation
