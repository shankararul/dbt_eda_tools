{% set variable_types_dict = {
        'text': ('VARCHAR','CHAR', 'CHARACTER', 'STRING', 'TEXT')
        , 'date': ('DATE','DATETIME','TIMESTAMP','TIMESTAMP_LTZ','TIMESTAMP_NTZ','TIMESTAMP_TZ')
        , 'numeric': ('NUMBER','DECIMAL','NUMERIC','INT','INTEGER','BIGINT','SMALLINT','FLOAT','FLOAT4','FLOAT8','DOUBLE','DOUBLE PRECISION','REAL','INT64', 'TINYINT','BYETEINT', 'BIGDECIMAL','FLOAT64')
        , 'boolean': ('BOOLEAN','BOOL')
        , 'time': ('TIME','NEEDSTOBEARRAY')
        , 'to_implement_binary': ('BINARY', 'VARBINARY')
        , 'to_implement_semistructured': ('ARRAY', 'OBJECT', 'VARIANT')
        , 'to_implement_geospatial': ('GEOGRAPHY','GEOMETRY')
        , 'to_implement_vector': ('VECTOR','NEEDSTOBEARRAY')
    } %}

        SELECT
        column_name
        , data_type
        , COUNT(*) OVER (){{':: STRING' if db_name == 'snowflake' else ''}} AS nbr_of_columns
        , CASE
                {% for key, value in variable_types_dict.items() %}
                    {% if not key.startswith('to_implement_') %}
                        WHEN DATA_TYPE IN {{ value }} THEN '{{key}}'
                    {% endif %}
                {% endfor %}
        END AS data_type_input

        {% for key, value in variable_types_dict.items() %}
            {% if not key.startswith('to_implement_') %}
                {% if db_name in ('snowflake','bigquery') %}
                    , {{'COUNT_IF' if db_name in ('snowflake','duckdb') else 'COUNTIF'}}(DATA_TYPE IN {{ value }}) OVER () {{':: STRING' if db_name=='snowflake' else ''}}  AS nbr_of_{{key}}_columns
                {% elif db_name == 'duckdb' %}
                    , COUNT(CASE WHEN DATA_TYPE IN ({{ value }}) THEN 1 END) OVER ():: STRING AS nbr_of_{{key}}_columns
                {% endif %}
            {% endif %}
        {% endfor %}

        FROM INFORMATION_SCHEMA.TABLES t
        INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON
                c.table_schema = t.table_schema AND c.table_name = t.table_name
                WHERE t.table_name = 'data_generator'
