/*
SET STATISTICS IO OFF
SET STATISTICS TIME OFF
SET STATISTICS PROFILE OFF
*/

DECLARE @schema nvarchar(200) = 'ext_si_sveitarfelaga'
DECLARE @table nvarchar(200) = 'sveitarfelag_raw'

DECLARE @header nvarchar(max) = (SELECT STRING_AGG( c.COLUMN_NAME, ',')	WITHIN GROUP (ORDER BY c.ordinal_position ASC) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table)
DECLARE @columns nvarchar(max) = '''"'' + CAST (' + (SELECT STRING_AGG( c.COLUMN_NAME, ' AS NVARCHAR(MAX)) + ''","'' + CAST (')	WITHIN GROUP (ORDER BY c.ordinal_position ASC) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table) + ' AS NVARCHAR(MAX)) + ''"'''
DECLARE @sql nvarchar(max) = 'SELECT ' + @columns + ' FROM [' + @schema + '].[' + @table + ']'

SELECT @header
EXEC (@sql)
