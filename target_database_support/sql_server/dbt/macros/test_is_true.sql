{% test is_true(model, column_name, predicate='1=1') %}

SELECT 
	* 
FROM 
	{{ model }} 
WHERE 
	NOT {{ predicate }}

{% endtest %}