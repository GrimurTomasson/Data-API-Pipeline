{% test equals(model, column_name, predicate) %}

SELECT 
    * 
FROM 
    {{ model }} 
WHERE 
    [{{ column_name }}] != {{ predicate }}

{% endtest %}