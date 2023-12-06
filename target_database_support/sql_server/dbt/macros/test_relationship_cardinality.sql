{% test relationship_cardinality(model, column_name, to_model, to_column, cardinality_from=1, cardinality_to=1, target_predicate='1=1', existence='NOT') %}

WITH lykill_fjoldi AS (
	SELECT
		{{to_column}} AS id, COUNT (1) AS fjoldi
	FROM
		{{ to_model }}
	WHERE
		{{ target_predicate }}
	GROUP BY
		{{to_column}}
    HAVING
        COUNT (1) BETWEEN {{ cardinality_from }} AND {{ cardinality_to }}
)
SELECT
	*
FROM
	{{ model }}
WHERE
	{{ column_name }} {{ existence }} IN ( SELECT id FROM lykill_fjoldi )

{% endtest %}