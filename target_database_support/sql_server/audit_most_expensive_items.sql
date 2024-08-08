-- Dýrustu módel/prófanir/snapshot úr síðustu keyrslu (public & private).
-- Keyrist á private grunnum, þar sem audit gögnin eru.
WITH 
dapi_last_run AS (
	SELECT 
		(SELECT DISTINCT FIRST_VALUE (id) OVER (ORDER BY start_time DESC) FROM audit.dapi_invocation WHERE [database] LIKE '%-API-PRIVATE') AS private_id
		,(SELECT DISTINCT FIRST_VALUE (id) OVER (ORDER BY start_time DESC) FROM audit.dapi_invocation WHERE [database] LIKE '%-API') AS public_id
),		
tests AS (
	-- Skila top 10 dýrustu prófunum úr síðustu keyrslu sem tókst.
	SELECT TOP 10
		invocation_id
		,unique_id
		,relation_name
		,ROUND (execution_time_in_seconds, 0) AS seconds
		,status
	FROM 
		audit.dbt_action
	WHERE
		invocation_id = (SELECT dbt.id FROM audit.dbt_invocation dbt JOIN dapi_last_run lr ON lr.public_id = dbt.dapi_invocation_id WHERE dbt.status = 'success' AND dbt.operation = 'Latest.run_tests' )
	ORDER BY 
		execution_time_in_seconds DESC
),
models AS (
	-- Skila top 10 dýrustu módelum úr síðustu keyrslu sem tókst.
	SELECT TOP 10
		invocation_id
		,unique_id
		,relation_name
		,ROUND (execution_time_in_seconds, 0) AS seconds
		,status
	FROM 
		audit.dbt_action
	WHERE
		invocation_id IN (SELECT dbt.id FROM audit.dbt_invocation dbt JOIN dapi_last_run lr ON lr.private_id = dbt.dapi_invocation_id OR lr.public_id = dbt.dapi_invocation_id WHERE dbt.status = 'success' AND dbt.operation = 'Latest.refresh')
	ORDER BY 
		execution_time_in_seconds DESC
),
snapshots AS (
	-- Skila top 10 dýrustu módelum úr síðustu keyrslu sem tókst.
	SELECT TOP 10
		invocation_id
		,unique_id
		,relation_name
		,ROUND (execution_time_in_seconds, 0) AS seconds
		,status
	FROM 
		audit.dbt_action
	WHERE
		invocation_id IN (SELECT dbt.id FROM audit.dbt_invocation dbt JOIN dapi_last_run lr ON lr.public_id = dbt.dapi_invocation_id WHERE dbt.status = 'success' AND dbt.operation = 'Latest.snapshot')
	ORDER BY 
		execution_time_in_seconds DESC
),
samsett AS (
	SELECT 'Test' AS type, invocation_id, unique_id, relation_name, seconds FROM tests
	UNION ALL
	SELECT 'Model' AS type, invocation_id, unique_id, relation_name, seconds FROM models
	UNION ALL
	SELECT 'Snapshot' AS type, invocation_id, unique_id, relation_name, seconds FROM snapshots
)
SELECT
	s.*
	,d.version
	,d.[user]
	,d.host
	,d.[database]
	,d.start_time
	,d.parameters
FROM
	samsett s
	JOIN audit.dbt_invocation di ON di.id = s.invocation_id
	JOIN audit.dapi_invocation d ON d.id = di.dapi_invocation_id AND d.operation = di.operation
WHERE
	seconds > 0
ORDER BY
	seconds DESC

/*
select * from audit.dapi_invocation where id = 'b5dbf44c-f457-4a1c-aa77-e31c291b4b5c' and operation = 'Latest.refresh'
select * from audit.dbt_invocation where id = '99d89860-e5a0-4393-aa5c-ed14e3428df8'
select * from audit.dbt_action 
*/

