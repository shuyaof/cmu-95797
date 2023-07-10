-- Macro to convert flag column's data type from varchar to boolean
{% macro get_flag_schema(custom_schema_name) %}
    (case {{ custom_schema_name }}
        when 'Y' then '1'
        when 'N' then '0'
        else null
    end)::boolean
{% endmacro %}