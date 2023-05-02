{% test not_always_null_or_empty(model, column_name) %}

SELECT 
	*
FROM
(
	SELECT  
		COUNT(1) as empty_rows
	FROM 
		{{ model }} 
	WHERE 
		[{{ column_name }}] IS NULL
		OR [{{ column_name }}] = ''
) no_value
CROSS JOIN 
(
	SELECT COUNT(1) AS all_rows FROM {{ model }}
) all_rows
WHERE 
	no_value.empty_rows = all_rows.all_rows

{% endtest %}