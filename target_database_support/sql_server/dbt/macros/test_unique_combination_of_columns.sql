{% test unique_combination_of_columns(model, columns, quote_columns=false) %}
  
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
    SELECT {{ columns_csv }}
    FROM {{ model }}
    GROUP BY {{ columns_csv }}
    HAVING COUNT(*) > 1
) i

{% endtest %}