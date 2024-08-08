-- Skila top 10 dýrustu módelum úr síðustu keyrslu sem tókst.
SELECT TOP 10
	unique_id
	,ROUND (execution_time_in_seconds, 0) AS seconds
	,status
FROM 
	audit.dbt_action
WHERE
	invocation_id = (select TOP 1 id from audit.dbt_invocation WHERE status = 'success' AND operation = 'Latest.refresh' ORDER BY start_time DESC)
ORDER BY 
	execution_time_in_seconds DESC