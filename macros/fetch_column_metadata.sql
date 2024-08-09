{% macro fetch_column_metadata(full_path, db_name, table_name) %}

{# {{full_path}}, {{db_name}}, {{table_name}} #}

    {% set meta_data_query %}
        WITH
        {{fetch_meta_data(full_path, db_name, table_name)}}
        SELECT column_name FROM meta_data WHERE data_type_input = 'text'
    {% endset %}

    {# {{meta_data_query}} #}
    {% set results = dbt_utils.get_query_results_as_dict(meta_data_query) %}

        WITH
        {% for col_name in results['COLUMN_NAME']%}
            column_detail_{{col_name}} AS (
                SELECT
                    {{col_name}}
                    , count(*) AS cnt
                    , SUM(COUNT({{col_name}})) OVER () AS cnt_total
                    , SUM(COUNT(DISTINCT {{col_name}})) OVER () AS cnt_unique
                    , COUNT_IF({{col_name}} IS NULL) AS cnt_null
                FROM
                {{ ref('data_generator_enriched_describe') }}
                GROUP BY 1
                LIMIT 10
            ) {{ ',' if not loop.last else ''}}
        {% endfor %}
        {% for col_name in results['COLUMN_NAME']%}
            SELECT
                '{{col_name}}' AS column_name
                , OBJECT_CONSTRUCT(
                    'column_name', '{{col_name}}'
                    , 'count' , MIN(cnt_total)
                    , 'unique' , MIN(cnt_unique)
                    , 'count_null' , MIN(cnt_null)
                    , 'value_counts_top10',OBJECT_AGG({{col_name}}, cnt)
                ) AS detail
            FROM column_detail_{{col_name}}
            {{ 'UNION ALL' if not loop.last else ''}}
        {% endfor %}
{% endmacro %}
