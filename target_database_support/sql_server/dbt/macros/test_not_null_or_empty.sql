{% test not_null_or_empty(model, column_name) %}

SELECT 
	*
FROM
	{{ model }} 
WHERE 
	[{{ column_name }}] IS NULL
	OR [{{ column_name }}] = ''

{% endtest %}