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
)

	


	
	
		