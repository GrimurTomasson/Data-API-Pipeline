WITH timi AS (
	SELECT 
		DATEADD (SECOND, i.counted_value - 1, CAST ('00:00:00' AS time)) AS time_key 		
	FROM (
		SELECT 
			ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS counted_value
		FROM   
			[master].[sys].[columns] sc1 -- Just to get a large number
			CROSS JOIN [master].[sys].[columns] sc2
		) i
	WHERE 
		i.counted_value <= (60*60*24)
), 
timi_extra AS (
	SELECT
		t.time_key AS id
		,DATEPART (HOUR, t.time_key) AS klukkutimi
		,DATEPART (MINUTE, t.time_key) AS minuta
		,DATEPART (SECOND, t.time_key) AS sekunda
	FROM
		timi t
)
SELECT
	t.id
	,t.klukkutimi
	,t.minuta
	,t.sekunda
	,CASE WHEN t.klukkutimi BETWEEN 8 AND 17 THEN 1 ELSE 0 END AS dagtimi
	,CASE WHEN t.klukkutimi BETWEEN 0 AND 5 THEN 1 ELSE 0 END AS nott
	,CASE WHEN t.klukkutimi BETWEEN 6 AND 9 THEN 1 ELSE 0 END AS morgun
	,CASE WHEN t.klukkutimi	BETWEEN 18 AND 23 THEN 1 ELSE 0 END AS kvold
FROM
	timi_extra t


	
	
		