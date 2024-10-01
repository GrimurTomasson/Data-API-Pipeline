-- Öll viðmið eru út frá núverandi keyrslu miðað við síðustu, neikvæðar tölur segja okkur að hún sé hraðari en sú síðasta
-- Keyrist á private grunni

WITH grunnupplysingar AS (
	SELECT 
		s.id
		,LAG (s.id) OVER (PARTITION BY s.[database], s.operation ORDER BY s.start_time) AS last_id
		,s.[database]
		,s.operation
		,s.execution_time_in_seconds
		,LAG (s.execution_time_in_seconds) OVER (PARTITION BY s.[database], s.operation ORDER BY s.start_time) AS last_execution_time_in_seconds
		,s.start_time
		,LAG (s.start_time) OVER (PARTITION BY s.[database], s.operation ORDER BY s.start_time) AS last_start_time -- Síðasta keyrsla sömu aðgerðar á sama grunni, getur verið innan dags.
	FROM 
		audit.dapi_invocation s
	WHERE
		s.status = 'success'
		and s.operation in ('Latest.refresh', 'MetadataCatalog.enrich', 'Latest.run_tests', 'Snapshot.create', 'Latest.snapshot', 'Latest.generate_docs')
		and s.start_time > GETDATE() - 2
), med_samtolum AS (
	SELECT
		s.[database]
		,s.operation
		,s.execution_time_in_seconds
		,s.last_execution_time_in_seconds
		,ROUND (s.execution_time_in_seconds - s.last_execution_time_in_seconds, 3) AS delta_time
		,ROUND (((s.execution_time_in_seconds * 100) / s.last_execution_time_in_seconds) - 100, 1) AS delta_percentage
		,SUM (s.execution_time_in_seconds) OVER (PARTITION BY s.[database]) AS db_total_time -- Ath. þetta er fyrir allt tímabilið sem verið er að skoða, ekki bara síðusta dag
		,SUM (s.last_execution_time_in_seconds) OVER (PARTITION BY s.[database]) AS last_db_total_time
		,s.start_time
		,s.last_start_time
	FROM
		grunnupplysingar s
	WHERE
		s.last_id IS NOT NULL -- Síðasta færsla innan tímabils á sér ekki eldri færslu, gagnlaus hér
		AND s.last_execution_time_in_seconds > 0 -- Gagnlausar upplýsingar sem skila okkur div 0 villu að auki
)
SELECT
	s.[database]
	,s.operation
	,s.execution_time_in_seconds
	,s.last_execution_time_in_seconds
	,s.delta_time
	,s.delta_percentage
	,s.db_total_time
	,s.last_db_total_time
	,ROUND (s.db_total_time - s.last_db_total_time, 3) AS db_delta_time
	,ROUND (((s.db_total_time * 100) / s.last_db_total_time) - 100, 1) AS db_delta_percentage
	,s.start_time
	,s.last_start_time
FROM
	med_samtolum s
ORDER BY
	s.start_time DESC


