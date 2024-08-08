-- 10 tímafrekustu snapshot í síðustu keyrslu
SELECT TOP 10 
	id
	,parameters
	,execution_time_in_seconds
	,version
	,user
	,host
FROM 
	audit.dapi_invocation 
WHERE 
	id = (SELECT DISTINCT FIRST_VALUE (id) OVER (ORDER BY start_time DESC) FROM audit.dapi_invocation)
	AND operation = 'Snapshot.__create_snapshot'
ORDER BY 
	execution_time_in_seconds, parameters