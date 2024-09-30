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
    {% set db_name = 'duckdb' %}

    WITH meta_data AS (

        SELECT
        column_name
        , data_type
        , COUNT(*) OVER (){{':: STRING' if db_name in ('snowflake','duckdb') else ''}} AS nbr_of_columns
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
                    , {{'COUNT_IF' if db_name =='snowflake' else 'COUNTIF'}}(DATA_TYPE IN {{ value }}) OVER () {{':: STRING' if db_name=='snowflake' else ''}}  AS nbr_of_{{key}}_columns
                {% elif db_name == 'duckdb' %}
                    , COUNT(CASE WHEN DATA_TYPE IN {{ value }} THEN 1 END) OVER ():: STRING AS nbr_of_{{key}}_columns
                {% endif %}
            {% endif %}
        {% endfor %}

        FROM INFORMATION_SCHEMA.TABLES t
        INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON
                c.table_schema = t.table_schema AND c.table_name = t.table_name
                WHERE t.table_name = 'data_generator'
    ),
    meta_data_unique AS (
            SELECT
                    DISTINCT
                    'dataset' AS identifier
                    , {{'CAST(NULL AS VARCHAR)' if db_name=='duckdb' else NULL}} AS detail
                    , nbr_of_columns
                    , nbr_of_text_columns
                    , nbr_of_date_columns
                    , nbr_of_numeric_columns
                    , nbr_of_boolean_columns
                    , nbr_of_time_columns
                FROM meta_data
        )
        , dataset_info AS (
            SELECT
                    CASE LOWER(meta_data_key)
                            WHEN 'nbr_of_columns' THEN 1
                            WHEN 'nbr_of_text_columns' THEN 2
                            WHEN 'nbr_of_date_columns' THEN 3
                            WHEN 'nbr_of_numeric_columns' THEN 4
                            WHEN 'nbr_of_boolean_columns' THEN 5
                            WHEN 'nbr_of_time_columns' THEN 6
                    END AS index_pos
                    , meta_data_key
                    , identifier
                    , detail
                    , {{'meta_data_value:: STRING' if db_name in ('snowflake','duckdb') else 'CAST(meta_data_value AS STRING)'}} AS meta_data_value
                FROM meta_data_unique
                UNPIVOT (meta_data_value FOR meta_data_key IN (nbr_of_columns, nbr_of_text_columns, nbr_of_date_columns, nbr_of_numeric_columns,nbr_of_boolean_columns,nbr_of_time_columns))
        )

        , rowcount_info AS (
            SELECT
                20 + ROW_NUMBER() OVER (ORDER BY data_type) AS index_pos
                , column_name AS meta_data_key
                , '{{key}}' AS identifier
                , {{'CAST(NULL AS VARCHAR)' if db_name=='duckdb' else NULL}} AS detail
                , {{'data_type:: STRING' if db_name=='snowflake' else 'CAST(data_type AS STRING)'}} AS meta_data_value
            FROM meta_data
        )
        , column_info AS (
            SELECT
                    0 AS index_pos
                    , {{'UPPER' if db_name=='snowflake' else ''}}('nbr_of_rows') AS meta_data_key
                    , 'dataset' AS identifier
                    , {{'CAST(NULL AS VARCHAR)' if db_name=='duckdb' else NULL}} AS detail
                    , {{'COUNT(*):: STRING' if db_name=='snowflake' else 'CAST(COUNT(*) AS STRING)'}} AS meta_data_value
            FROM meta_data
            GROUP BY ALL
        )

    SELECT * from column_info
