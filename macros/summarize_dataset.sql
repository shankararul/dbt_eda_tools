{% macro summarize_dataset(db_name, dtype_loop=none) %}
        {{ return(adapter.dispatch('summarize_dataset', 'dbt_eda_tools')(db_name, dtype_loop)) }}
{% endmacro %}

{% macro default__summarize_dataset(db_name, dtype_loop) %}

{% set json_keys = ('count','count_null','mean','percentile_25','percentile_50','percentile_75','unique_values','value_counts_top10', 'estimated_granularity','estimated_granularity_confidence','min','max') %}


{% if dtype_loop %}
    , unioned_data AS (
            {% for dtype in dtype_loop %}
                SELECT
                   '{{dtype}}' AS dtype
                    , * FROM
                column_detail_info_{{dtype}}
                WHERE column_name != ''
                {% if not loop.last %}
                    UNION ALL
                {% endif %}
            {% endfor %}
        )
        {% if db_name in ('bigquery','duckdb') %}
            , flatten_data AS (
                SELECT
                    column_name
                    , dtype
                    {% for pivot_key in json_keys%}
                        , json_extract(detail, '$.{{pivot_key}}') AS {{pivot_key}}
                    {% endfor %}
                    FROM unioned_data
            )
        {% endif %}
        , unpivoted_data AS (
                SELECT
                    column_name
                    , dtype
                    , key AS pivot_key
                    , {{ 'value' if db_name == 'snowflake' else 'TO_JSON_STRING(value)' if db_name == 'bigquery' else 'JSON(value)' }} AS pivot_value ,
                FROM
            {% if db_name =='snowflake' %}
                unioned_data
                , LATERAL FLATTEN(input => detail) f
            {% elif db_name in ('bigquery','duckdb') %}
                flatten_data AS f
                UNPIVOT (
                    value FOR key IN {{json_keys|replace("'",'')}}
                )
            {% endif %}

        )
        SELECT
            column_name
            , dtype
            , {{'"\'count\'"' if db_name=='snowflake' else 'count'}}
            , {{'"\'count_null\'"' if db_name=='snowflake' else 'count_null'}}
            , ROUND({{'DIV0NULL' if db_name=='snowflake' else 'SAFE_DIVIDE' if db_name =='bigquery' else ''}}(
                {{'"\'count_null\'"::double' if db_name == 'snowflake' else 'SAFE_CAST(count_null AS NUMERIC)' if db_name =='bigquery' else 'TRY_CAST(count_null AS NUMERIC)'}}
                {{',' if db_name in ('bigquery','snowflake') else '/' }} {{('"\'count\'"::double+"\'count_null\'"::double') if db_name == 'snowflake' else '(SAFE_CAST(count AS NUMERIC)+SAFE_CAST(count_null AS NUMERIC))' if db_name =='bigquery' else '(TRY_CAST(count AS NUMERIC)+TRY_CAST(count_null AS NUMERIC))'}}
            ),3)

            AS percent_null
            , * {{'EXCLUDE' if db_name in ('snowflake','duckdb') else 'EXCEPT'}}(column_name, dtype
                , {{'"\'count\'"' if db_name=='snowflake' else 'count'}}
                , {{'"\'count_null\'"' if db_name=='snowflake' else 'count_null'}}
            )
        FROM
        unpivoted_data
        PIVOT(MIN(pivot_value) FOR pivot_key IN {{'(ANY)' if db_name == 'snowflake' else json_keys|replace("'",'"')}})
        ORDER BY dtype
{% else %}
    SELECT
        assembed_result.meta_data_key
        , assembed_result.meta_data_value
        , assembed_result.identifier
        , COALESCE(text_detail.detail, numeric_detail.detail,date_detail.detail, boolean_detail.detail) AS detail
    FROM assembled_result AS assembed_result
    LEFT JOIN column_detail_info_text AS text_detail
    ON assembed_result.meta_data_key = text_detail.column_name
    LEFT JOIN column_detail_info_numeric AS numeric_detail
    ON assembed_result.meta_data_key = numeric_detail.column_name
    LEFT JOIN column_detail_info_date AS date_detail
    ON assembed_result.meta_data_key = date_detail.column_name
    LEFT JOIN column_detail_info_boolean AS boolean_detail
    ON assembed_result.meta_data_key = boolean_detail.column_name

    ORDER BY index_pos ASC
{% endif %}
{% endmacro %}
