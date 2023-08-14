{% test unique_combination_of_columns_v2(model, columns, quote_columns=false, max_days_back = 0, date_column = '') %}
  
{% if not quote_columns %}
    {%- set column_list=columns %}
{% elif quote_columns %}
    {%- set column_list=[] %}
        {% for column in columns -%}
            {% set column_list = column_list.append( adapter.quote(column) ) %}
        {%- endfor %}
{% else %}
    {{ exceptions.raise_compiler_error(
        "`quote_columns` argument for unique_combination_of_columns test must be one of [True, False] Got: '" ~ quote ~"'.'"
    ) }}
{% endif %}

{%- set columns_csv=column_list | join(', ') %}


SELECT i.* FROM 
(
    SELECT 
        {{ columns_csv }}
    FROM 
        {{ model }}
{% if max_days_back > 0 %}
    {%- set date_query = "SELECT DATEADD (DAY, -" ~ max_days_back ~ ", MAX ( " ~ date_column ~ " )) FROM " ~ model -%}
    {# {% do log("source_query: " ~ date_query, info=true) %} #}
    {%- set results = run_query( date_query ) -%}
    
    {%- if results|length > 0 -%}
        {%- set date_limit = results.columns[0].values()[0]  -%}
    {%- else -%}
        {%- set date_limit = 'No max date results retrieved!!! ERROR'  -%}
    {%- endif -%}
    WHERE
        {{ date_column }} > '{{ date_limit }}'
{%- endif %}
    GROUP BY {{ columns_csv }}
    HAVING COUNT(*) > 1
) i

{% endtest %}