DECLARE @currentSchema nvarchar(100) = 'Nustada'
DECLARE @snapshotSchema nvarchar(100) = 'Private_snapshot'

;with comparison as (
	select 
		c.TABLE_NAME
		,c.ORDINAL_POSITION
		,c.COLUMN_NAME as c_name
		,s.COLUMN_NAME as s_name
		,c.IS_NULLABLE as c_nullable
		,s.IS_NULLABLE AS s_nullable
		,c.DATA_TYPE AS c_datatype
		,s.DATA_TYPE as s_datatype
		,c.CHARACTER_MAXIMUM_LENGTH as c_length
		,s.CHARACTER_MAXIMUM_LENGTH as s_length
		,c.NUMERIC_PRECISION as c_precision
		,s.NUMERIC_PRECISION as s_precision
		,c.COLLATION_NAME as c_collation
		,s.COLLATION_NAME as s_collation
	from 
		INFORMATION_SCHEMA.COLUMNS c 
		LEFT JOIN INFORMATION_SCHEMA.COLUMNS s ON s.COLUMN_NAME = c.COLUMN_NAME AND s.TABLE_SCHEMA = @snapshotSchema AND s.TABLE_NAME = c.TABLE_NAME
	where 
		c.TABLE_SCHEMA = @currentSchema
),
curated AS (
	select
		c.c_name as column_name
		,CASE WHEN c.c_name != COALESCE (c.s_name, 'NULL') then c.c_name + ' - ' + COALESCE (c.s_name, 'NULL') else '' end as name
		,case when c.c_nullable != COALESCE (c.s_nullable, 'NULL') AND c.s_nullable != 'YES' then c.c_nullable + ' - ' + COALESCE (c.s_nullable, 'NULL') else '' end as nullable -- Snapshot columns should always be nullable
		,case when c.c_datatype != COALESCE (c.s_datatype, 'NULL') then c.c_datatype + ' - ' + COALESCE (c.s_datatype, 'NULL') else '' end as datatype
		,case when coalesce (c.c_length, 0) != coalesce(c.s_length, 0) then cast (coalesce (c.c_length, 0) as nvarchar(10)) + ' - ' + CAST (coalesce(c.s_length, 0) AS nvarchar(10)) else '' end as string_length
		,case when coalesce (c.c_precision, 0) != coalesce (c.s_precision, 0) then CAST (coalesce(c.c_precision, 0) AS nvarchar(10)) + ' - ' + cast (coalesce (c.s_precision, 0) as nvarchar(10)) else '' end as precision
		,case when coalesce (c.c_collation, 'null') != coalesce (c.s_collation, 'null') then coalesce (c.c_collation, 'NULL') + ' - ' + coalesce (c.s_collation, 'NULL') else '' end as collation
		,c.*
	FROM
		comparison c
)
SELECT
	c.TABLE_NAME
	,c.column_name
	,c.name
	,c.nullable
	,c.datatype
	,c.string_length
	,c.precision
	,c.collation
	,'ALTER TABLE ' + @snapshotSchema + '.' + c.TABLE_NAME + ' ALTER COLUMN ' + c.column_name + ' ' + c.c_datatype + '(' + CAST (c.c_length AS NVARCHAR(20)) + ') COLLATE ' + c.c_collation + ' NULL' as alter_string
FROM
	curated c
WHERE
	LEN (c.name + c.nullable + c.datatype + c.string_length + c.precision + c.collation) > 0
ORDER BY
	c.TABLE_NAME
	,c.ORDINAL_POSITION
/*
	Breyta snapshot ef:
		char týpa breytist (char, varchar, nvarchar)
		lengd á char týpu eykst
		collation er ekki eins og í uppruna, ef týpur eru eins!
*/

