{#
    Overwrite of the tsql_utils version, which is bugged for dbt 1.4.x
#}

{%- macro generate_surrogate_key(field_list, col_type=None) -%}

    {%- if col_type == None -%}
        {%- set col_type = var("tsql_utils_surrogate_key_col_type", "varchar(8000)") -%}
    {%- endif -%}

    {%- if field_list is string -%}
        {%- set key = dbt.hash("coalesce(cast(" ~ field_list ~ " as " ~ col_type ~ "), '')") -%} 
    {% else %}
        {%- set fields = [] -%}
        {%- for field in field_list -%}
            {%- set _ = fields.append("coalesce(cast(" ~ field ~ " as " ~ col_type ~ "), '')") -%}
            {%- if not loop.last %}
                {%- set _ = fields.append("'-'") -%}
            {%- endif -%}
        {%- endfor -%}
        {%- set key = dbt.hash(dbt.concat(fields)) -%}
    {%- endif -%}

    {{ key }}

{%- endmacro -%}