{% macro fetch_meta_data(full_path,db_name,table_name) %}
    {% set variable_types = var('variable_types') %}
    {% set variable_types_dict = {
        'text': variable_types['text']
        , 'date': variable_types['date']
        , 'numeric': variable_types['numeric']
        , 'boolean': variable_types['boolean']
        , 'time': variable_types['time']
    } %}
    meta_data AS (
        -- Need to explicitly cast the type before transposing the data
        SELECT
        column_name
        , data_type
        , COUNT(*) OVER (){{':: STRING' if db_name == 'snowflake' else ''}} AS nbr_of_columns

        {% for key, value in variable_types_dict.items() %}
            {% if  key != 'time' %}
                , {{'COUNT_IF' if db_name=='snowflake' else 'COUNTIF'}}(DATA_TYPE IN {{ value }}) OVER () {{':: STRING' if db_name=='snowflake' else ''}}  AS nbr_of_{{key}}_columns
            {% elif key == 'time'%}  -- time is a special case, as it is not an array
                , {{'COUNT_IF' if db_name=='snowflake' else 'COUNTIF'}}(DATA_TYPE = '{{ value }}') OVER () {{':: STRING' if db_name=='snowflake' else ''}}  AS nbr_of_{{key}}_columns
            {% endif %}

        {% endfor %}

        FROM {{full_path}}.TABLES t
        INNER JOIN {{full_path}}.COLUMNS c ON
                c.table_schema = t.table_schema AND c.table_name = t.table_name
                WHERE t.table_name = '{{table_name}}'
    )

{% endmacro %}
