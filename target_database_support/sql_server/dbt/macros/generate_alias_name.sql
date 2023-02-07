{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- set postfix_to_remove = node.unrendered_config['remove_relation_postfix'] -%}

    {%- if custom_alias_name is none -%}
        {%- set default_alias = node.name -%}
    {%- else -%}
        {%- set default_alias = custom_alias_name | trim -%}
    {%- endif -%}

    {%- if postfix_to_remove is none -%}
        {{ default_alias }}
    {%- else -%}
        {{ default_alias | replace(postfix_to_remove, "")}}
    {%- endif -%}

{%- endmacro %}