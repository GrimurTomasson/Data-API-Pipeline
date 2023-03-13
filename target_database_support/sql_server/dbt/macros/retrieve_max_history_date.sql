{% macro retrieve_max_history_date (source_table, relation, history_date_column) -%}

{%- do log("retrieve_max_history_date -> source_table: " ~ source_table, info=true) -%}
{%- do log("retrieve_max_history_date -> relation: " ~ relation, info=true) -%}

{%- if relation is none -%}
CONVERT (DATE, '1900-01-01', 20)
{%- else -%}

{%- set date_query = "(SELECT COALESCE (MAX ( " ~ history_date_column ~ " ), CAST('1950-01-01' AS date)) FROM " ~ source_table ~ ")" -%}
{# {% do log("source_query: " ~ date_query, info=true) %} #}

{%- set results = run_query( date_query ) -%}
{%- if results|length > 0 -%}
    {%- set date_limit = results.columns[0].values()[0]  -%}
{%- else -%}
    {%- set date_limit = 'No max date results retrieved!!! ERROR'  -%}
{%- endif -%}
CONVERT (DATE, '{{ date_limit }}', 20)
{%- endif -%}

{%- endmacro %}
