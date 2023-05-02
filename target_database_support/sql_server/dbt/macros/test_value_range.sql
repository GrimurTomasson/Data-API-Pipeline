{% test value_range(model, column_name, from=0, to=2147483647) %}

SELECT 
    * 
FROM 
    {{ model }} 
WHERE 
    [{{ column_name }}] NOT BETWEEN {{ from }} AND {{ to }}

{% endtest %}