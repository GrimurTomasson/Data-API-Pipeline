-- Ath. Þetta er svona út af hringhæðismálum.
{%- set relation = load_relation(this) -%}
{%- set date_limit = retrieve_max_history_date (this, relation, var('history-date-column-name'))  -%}

-- Eftirfarandi tvær breytur eru það eina sem við breytum per vídd.
{%- set history_table = ref('Address_dim_history')  -%}
{%- set key_column_name = 'id'  %}

{{ generate_unified_dimension_v2 (
	history_table
    ,date_limit
    ,key_column_name
    ,var('history-date-column-name') )
}}

{% if relation is not none -%} -- Einungis ef venslin eru til
SELECT -- Það sem kom ekki í viðbótargögnum
    s.*
FROM
    {{ this }} s
WHERE 
    NOT EXISTS (SELECT 1 FROM delta d WHERE d.{{ key_column_name }} = s.{{ key_column_name }})

UNION ALL
{% endif -%}

SELECT -- Það sem kom í viðbótargögnum
    d.*
FROM
    delta d
