WITH files as (
    SELECT 
		database_id, type, size * 8.0 / 1024 / 1024 as size_gb
    from 
		sys.master_files
),
filesize AS (
	SELECT 
		d.name AS database_name,
		CAST (SUM (f.size_gb) AS numeric(10,1)) AS total_size_gb
		,CAST (SUM (CASE WHEN f.type = 0 THEN f.size_gb ELSE 0 END) AS numeric(10,1)) AS data_file_size_gb
		,CAST (SUM (CASE WHEN f.type = 1 THEN f.size_gb ELSE 0 END) AS numeric(10,1)) AS log_file_size_gb
	FROM 
		files f
		JOIN sys.databases d ON d.database_id = f.database_id
	GROUP BY
		d.name
),
filesize_2 AS (
	SELECT
		database_name
		,total_size_gb
		,data_file_size_gb
		,log_file_size_gb
		,(SELECT SUM (total_size_gb) FROM filesize) AS total_server_size_gb
	FROM
		filesize
)
SELECT
	database_name
	,total_size_gb
	,data_file_size_gb
	,log_file_size_gb
	,CAST (CAST (total_size_gb * 100 AS numeric(10,1)) / total_server_size_gb AS numeric(10,1)) AS percentage_of_total_server_size
	,total_server_size_gb
FROM 
	filesize_2
ORDER BY
	total_size_gb DESC
