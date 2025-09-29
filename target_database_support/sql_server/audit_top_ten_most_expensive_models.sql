-- Skila top 10 d�rustu m�delum �r s��ustu keyrslu sem t�kst.
WITH 
last_ok_run AS (
    SELECT TOP 1 id, execution_time_in_seconds from audit.dbt_invocation WHERE status = 'success' AND operation = 'Latest.refresh' AND parameters LIKE '%tag:private%' ORDER BY start_time DESC
)
SELECT TOP 10
	unique_id
	,ROUND (s.execution_time_in_seconds, 0) AS seconds
    ,ROUND ((s.execution_time_in_seconds * 100) / l.execution_time_in_seconds, 0) AS percentage_of_run_time
	,status
FROM 
	audit.dbt_action s
    JOIN last_ok_run l ON l.id = s.invocation_id
ORDER BY 
	s.execution_time_in_seconds DESC