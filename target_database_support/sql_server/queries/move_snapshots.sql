WITH
	snapshot_base AS (
		SELECT 
			'SFS-API' AS source_database
			,'SFS-API-PRIVATE' AS target_database
			,'Private_snapshot' AS source_schema
			,'Snapshot' AS target_schema
	)
	,snapshots AS (
	SELECT
		b.source_database
		,b.target_database
		,b.source_schema
		,b.target_schema
		,t.TABLE_NAME
		,'[' + b.source_database + '].[' + b.source_schema + '].[' + t.TABLE_NAME + ']' AS source_name
		,'[' + b.target_database + '].[' + b.target_schema + '].[' + t.TABLE_NAME + ']' AS target_name
	FROM 
		[SFS-API-PRIVATE].INFORMATION_SCHEMA.TABLES t 
		JOIN snapshot_base b ON b.target_schema = t.TABLE_SCHEMA
		JOIN [SFS-API].INFORMATION_SCHEMA.TABLES s ON s.TABLE_SCHEMA = b.source_schema AND s.TABLE_NAME = t.TABLE_NAME -- Pössum að töflurnar séu til báðu megin.
)
SELECT TOP 1
'-- Simple insert' + char(13) + char(13) +
'INSERT INTO '+ s.target_name + char(13) +
'SELECT o.*' + char(13) +
'FROM ' + s.source_name + ' o' + char(13) +
'WHERE o.sogu_dagur < (SELECT COALESCE (MIN (sogu_dagur), GETDATE() +1) FROM ' + s.target_name + ')' + CHAR(13) + char(13)
AS simple_insert_sql
,
'-- Larger data insert' + char(13) + char(13) +
'--CREATE TABLE dbo.temp_log_table(tafla varchar(50), sogu_dagur date, radir int, timi datetime )' + char(13) + char(13) +
'DECLARE @rows INT = 1' + char(13) +
'DECLARE @sogu_dagur DATE' + char(13) +
'DECLARE @table VARCHAR(50) = ''' + s.table_name + '''' + char(13) +
'WHILE @rows > 0 BEGIN' + char(13) +
'	SET @sogu_dagur = (' + char(13) + -- Elsti dagur í source töflu sem ekki er til í target töflu
'		SELECT COALESCE (MIN (s.sogu_dagur), GETDATE() + 1) FROM ' + s.source_name + ' s' + char(13) +
'		WHERE NOT EXISTS (SELECT 1 FROM ' + s.target_name + ' t WHERE t.sogu_dagur = s.sogu_dagur)'  + char(13) +
'	)'  + char(13) + 
'	BEGIN TRANSACTION' + char(13) + char(13) +
'	INSERT INTO ' + s.target_name + char(13) +
-- The target database is pinned here too
'	SELECT ' + (SELECT string_agg (column_name, ', ') FROM [SFS-API-PRIVATE].INFORMATION_SCHEMA.COLUMNS WHERE table_schema = s.target_schema and table_name = s.TABLE_NAME) + char(13) +
'	FROM ' + s.source_name  + char(13) +
'	WHERE sogu_dagur =  @sogu_dagur' + char(13) + char(13) +
'	SET @rows = @@ROWCOUNT' + char(13) +
'	INSERT INTO dbo.temp_log_table VALUES (@table, @sogu_dagur, @rows, GETDATE())' + char(13) +
'	COMMIT'  + char(13) +
'END' + char(13) + char(13)
AS complex_insert_sql
,
'-- Data comparison' + char(13) + char(13) +
'WITH dagur AS (' + char(13) +
'	SELECT DISTINCT sogu_dagur FROM ' + s.source_name + ' s' + char(13) +
'	UNION' + char(13) +
'	SELECT DISTINCT sogu_dagur FROM ' + s.target_name + ' t' + char(13) +
')' + char(13) +
', snap_source AS (' + char(13) +
'	SELECT sogu_dagur, COUNT(1) AS fjoldi FROM ' + s.source_name + ' GROUP BY sogu_dagur' + char(13) +
')' + char(13) +
', snap_target AS (' + char(13) +
'	SELECT sogu_dagur, COUNT(1) AS fjoldi FROM ' + s.target_name + ' GROUP BY sogu_dagur' + char(13) +
')' + char(13) +
',samantekid AS (' + char(13) +
'	SELECT' + char(13) +
'		d.sogu_dagur, COALESCE (s.fjoldi, 0) as s_fjoldi, COALESCE (t.fjoldi, 0) AS t_fjoldi' + char(13) +
'	FROM' + char(13) +
'		dagur d' + char(13) +
'		LEFT JOIN snap_source s ON s.sogu_dagur = d.sogu_dagur' + char(13) +
'		LEFT JOIN snap_target t ON t.sogu_dagur = d.sogu_dagur' + char(13) +
')' + char(13) +
'SELECT' + char(13) +
'	*' + char(13) +
'FROM' + char(13) +
'	samantekid' + char(13) +
'WHERE' + char(13) +
'	s_fjoldi > t_fjoldi' + char(13) + -- Það er í lagi að target innihaldi daga sem ekki eru til í source
'ORDER BY' + char(13) +
'	sogu_dagur'  + char(13) + char(13)
AS comparison_sql
,
'-- Drop source table' + char(13) + char(13) +
'-- DROP TABLE ' + s.source_name
AS drop_sql

FROM 
	snapshots s


/*
with loggur AS (
	SELECT 
		* 
		,LEAD (timi) OVER (PARTITION BY tafla ORDER BY timi DESC) AS sidasti_timi
	FROM 
		dbo.temp_log_table
)
SELECT 
	* 
	,DATEDIFF (SECOND, sidasti_timi, timi) AS keyrslutimi_sek
	,DATEDIFF (SECOND, timi, GETDATE ()) AS klarad_fyrir_sek
FROM 
	loggur
ORDER BY 
	timi desc
*/