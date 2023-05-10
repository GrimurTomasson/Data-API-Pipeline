{% test relationship_defined(model, column_name, to_model, to_column, existence='NOT') %}

SELECT 
    * 
FROM 
    {{ model }} 
WHERE 
    [{{ column_name }}] {{ existence }} IN (
        SELECT 
            [{{ to_column }}]
        FROM 
            {{ to_model }}
    )

{% endtest %}