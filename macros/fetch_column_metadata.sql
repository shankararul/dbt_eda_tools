{% macro fetch_column_metadata(model_name, output_name, full_path, db_name, table_name) %}

    {# filter the metadata table to only include columns with a data type of 'text'. #}
    {% set meta_data_query %}
        WITH
        {{fetch_meta_data('meta_data', full_path, db_name, table_name)}}
        SELECT column_name FROM meta_data WHERE data_type_input = 'text'
    {% endset %}

    {% set model_ref = model_name %}

    {% set conditional_col_name = 'COLUMN_NAME' if db_name == 'snowflake' else 'column_name' %}

    {# execute the SQL and fetch the results #}
    {% set results = dbt_utils.get_query_results_as_dict(meta_data_query) %}

    {# construct the column detail results #}
    {% for col_name in results[conditional_col_name]%}
        column_detail_{{col_name}} AS (
            SELECT
                {{col_name}}
                , count(*) AS cnt
                , SUM(COUNT({{col_name}})) OVER () AS cnt_total
                , SUM(COUNT(DISTINCT {{col_name}})) OVER () AS cnt_unique
                , {{'COUNT_IF' if db_name=='snowflake' else 'COUNTIF'}}({{col_name}} IS NULL) AS cnt_null
            FROM {{model_ref}}

            GROUP BY 1
            LIMIT 10
        ) {{ ',' if not loop.last else ''}}
    {% endfor %}

    {# turn the results into a json object #}
    , {{output_name}} AS (
        {% for col_name in results[conditional_col_name]%}
        SELECT
            '{{col_name}}' AS column_name
            , {{'OBJECT_CONSTRUCT' if db_name == 'snowflake' else 'JSON_OBJECT'}}(
                'column_name', '{{col_name}}'
                , 'count' , MIN(cnt_total)
                , 'unique' , MIN(cnt_unique)
                , 'count_null' , MIN(cnt_null)
                , 'value_counts_top10',{{'OBJECT_AGG(' if db_name == 'snowflake' else 'ARRAY_AGG('}}{{'' if db_name == 'snowflake' else 'JSON_OBJECT('}}{{col_name}}, cnt){{'' if db_name == 'snowflake' else ')'}}
            ) AS detail
        FROM column_detail_{{col_name}}
        {{ 'UNION ALL' if not loop.last else ''}}
    {% endfor %}

    )

{% endmacro %}
