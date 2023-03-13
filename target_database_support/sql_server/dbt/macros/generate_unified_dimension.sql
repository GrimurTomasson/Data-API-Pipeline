{% macro generate_unified_dimension (source_table, key_column_name, history_date_column_name) -%}

{# {% do log("source_table: " ~ source_table, info=true) %} #}
{# {% do log("Key column name: " ~ key_column_name, info=true) %} #}
{# {% do log("History date column name: " ~ history_date_column_name, info=true) %} #}

{%- set all_columns = adapter.get_columns_in_relation (source_table) -%}
{%- set except_col_names=[history_date_column_name] -%}

SELECT DISTINCT 
{%- for col in all_columns if col.name not in except_col_names %}
{%- if loop.index > 1 -%}  
    ,
{%- endif %}
    FIRST_VALUE(s.{{ col.name }}) OVER (PARTITION BY s.{{ key_column_name }} ORDER BY s.{{ history_date_column_name }} DESC) AS {{ col.name }}
{%- endfor %}
    ,FIRST_VALUE(s.{{ history_date_column_name }}) OVER (PARTITION BY s.{{ key_column_name }} ORDER BY s.{{ history_date_column_name }} DESC) AS {{ history_date_column_name }}
FROM
    {{ source_table }} s

{%- endmacro %}