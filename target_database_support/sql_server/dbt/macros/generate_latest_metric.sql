{% macro generate_latest_metric (source_table, history_date_column_name) -%}

{% do log("source_table: " ~ source_table, info=true) %}
{% do log("History date column name: " ~ history_date_column_name, info=true) %}

{%- set all_columns = adapter.get_columns_in_relation (source_table) -%}
{%- set except_col_names=[history_date_column_name] -%}

SELECT 
{%- for col in all_columns if col.name not in except_col_names %}
{%- if loop.index > 1 -%}  
    ,
{%- endif %}
    m.{{ col.name }}
{%- endfor %}
FROM
    {{ source_table }} m
WHERE
    m.{{ history_date_column_name }} = (SELECT MAX ({{ history_date_column_name }}) FROM {{ source_table }})

{%- endmacro %}