{% test not_null(model, column_name, filter='1=1') %}

SELECT 
	* 
FROM 
	{{ model }} 
WHERE 
	[{{ column_name }}] IS NULL
	AND {{ filter }}

{% endtest %}