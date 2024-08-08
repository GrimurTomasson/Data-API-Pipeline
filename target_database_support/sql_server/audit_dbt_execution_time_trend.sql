WITH gogn AS (
	SELECT 
		operation
		,execution_time_in_seconds
		,CAST (start_time AS date) AS dags
	FROM 
		audit.dbt_invocation
	WHERE
		status = 'success'
)
SELECT DISTINCT
	operation
	,ROUND (AVG (execution_time_in_seconds) OVER (PARTITION BY operation, dags), 0) AS seconds
	,dags
FROM
	gogn
WHERE
	dags >= DATEADD (DAY, -7, GETDATE()) -- Síðasta vika
ORDER BY 
	operation, dags DESC