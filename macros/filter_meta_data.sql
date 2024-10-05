{% macro filter_meta_data(output_name,key,table_name,db_name) %}
    {{ return(adapter.dispatch('filter_meta_data', 'dbt_eda_tools')(output_name,key,table_name,db_name)) }}
{% endmacro %}

{% macro default__filter_meta_data(output_name,key,table_name,db_name) %}

    {% if key == 'dataset' %}

        meta_data_unique AS (
            SELECT
                    DISTINCT
                    '{{key}}' AS identifier
                    , '' AS detail
                    , nbr_of_columns
                    , nbr_of_text_columns
                    , nbr_of_date_columns
                    , nbr_of_numeric_columns
                    , nbr_of_boolean_columns
                    , nbr_of_time_columns
                FROM {{table_name}}
        )
        , {{output_name}} AS (
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
                    , CAST(meta_data_value AS STRING) AS meta_data_value
                FROM meta_data_unique
                UNPIVOT (meta_data_value FOR meta_data_key IN (nbr_of_columns, nbr_of_text_columns, nbr_of_date_columns, nbr_of_numeric_columns,nbr_of_boolean_columns,nbr_of_time_columns))
        )

    {% elif key =='column' %}
        {{output_name}} AS (
            SELECT
                20 + ROW_NUMBER() OVER (ORDER BY data_type) AS index_pos
                , column_name AS meta_data_key
                , '{{key}}' AS identifier
                , '' AS detail
                , CAST(data_type AS STRING) AS meta_data_value
            FROM {{table_name}}
        )
    {% elif key =='rowcount'%}
        {{output_name}} AS (
            SELECT
                    0 AS index_pos
                    , {{'UPPER' if db_name=='snowflake' else ''}}('nbr_of_rows') AS meta_data_key
                    , 'dataset' AS identifier
                    , '' AS detail
                    , CAST(COUNT(*) AS STRING) AS meta_data_value
            FROM {{ table_name }}
            GROUP BY ALL
        )
    {% endif %}

{% endmacro %}
