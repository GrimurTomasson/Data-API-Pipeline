-- Uppruni lausnar: https://www.mssqltips.com/sqlservertip/6519/valid-email-address-check-with-tsql/
{% test email_ok(model, column_name) %}
WITH grunngogn AS (
	SELECT 
		*
		,TRIM (COALESCE ({{ column_name }}, '')) AS snyrt_bodleid
	FROM 
		{{ model }}
), 
grunngogn_siud AS (
SELECT
	*
	,CASE 
		WHEN snyrt_bodleid = '' THEN 0
		WHEN snyrt_bodleid LIKE '% %' THEN 0
		WHEN snyrt_bodleid LIKE ('%["(),:;<>\]%') THEN 0
		WHEN SUBSTRING (snyrt_bodleid, CHARINDEX ('@', snyrt_bodleid), LEN (snyrt_bodleid)) LIKE ('%[!#$%&*+/=?^`_{|]%') THEN 0
		WHEN (LEFT (snyrt_bodleid, 1) LIKE ('[-_.+]') OR RIGHT (snyrt_bodleid, 1) LIKE ('[-_.+]')) THEN 0
		WHEN (snyrt_bodleid LIKE '%[%' OR snyrt_bodleid LIKE '%]%') THEN 0
		WHEN snyrt_bodleid LIKE '%@%@%' THEN 0
		WHEN snyrt_bodleid NOT LIKE '_%@_%._%' THEN 0
		ELSE 1
	END AS i_lagi
FROM
	grunngogn
)
SELECT
	*
FROM
	grunngogn_siud
WHERE
	i_lagi = 0
{% endtest %}