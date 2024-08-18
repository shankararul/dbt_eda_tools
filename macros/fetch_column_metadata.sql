{% macro fetch_column_metadata(model_name, output_name, data_type, full_path, db_name, table_name) %}

    {# filter the metadata table to only include columns with a data type of 'text'. #}
    {% set meta_data_query %}
        WITH
        {{fetch_meta_data('meta_data', full_path, db_name, table_name)}}
        SELECT column_name FROM meta_data WHERE data_type_input = '{{data_type}}'
    {% endset %}

    {% set conditional_col_name = 'COLUMN_NAME' if db_name == 'snowflake' else 'column_name' %}
    {% set conditional_estimated_granularity_name = 'ESTIMATED_GRANULARITY' if db_name == 'snowflake' else 'estimated_granularity' %}
    {% set conditional_estimated_granularity_confidence_name = 'ESTIMATED_GRANULARITY_CONFIDENCE' if db_name == 'snowflake' else 'estimated_granularity_confidence' %}

    {# execute the SQL and fetch the results #}
    {% set results = dbt_utils.get_query_results_as_dict(meta_data_query) %}
    {% if results[conditional_col_name] %}
        {# construct the column detail results #}
        {% for col_name in results[conditional_col_name]%}

            {% if data_type == 'date'%}
                {% set results_granuality = dbt_utils.get_query_results_as_dict(estimated_granularity(model_name, col_name) ) %}
            {% endif %}


            column_detail_{{col_name}} AS (
                SELECT
                    1
                    {% if data_type in ('text','boolean') %}
                        , {{col_name}}
                        , COUNT(*) AS cnt
                    {%  elif data_type == 'numeric' %}
                        , AVG({{col_name}}) AS avg
                        {% if db_name == 'snowflake'%}
                        , APPROX_PERCENTILE({{col_name}}, 0.25) AS percentile_25
                        , APPROX_PERCENTILE({{col_name}}, 0.5) AS percentile_50
                        , APPROX_PERCENTILE({{col_name}}, 0.75) AS percentile_75
                        {% elif db_name == 'bigquery' %}
                        , APPROX_QUANTILES(str_length, 100)[OFFSET(25)] AS percentile_25
                        , APPROX_QUANTILES(str_length, 100)[OFFSET(50)] AS percentile_50
                        , APPROX_QUANTILES(str_length, 100)[OFFSET(75)] AS percentile_75
                        {% endif %}
                    {%  elif data_type in ('numeric','date') %}
                        , MIN({{col_name}}) AS min
                        , MAX({{col_name}}) AS max
                        {%  if data_type == 'date' %}
                            , MIN('{{results_granuality[conditional_estimated_granularity_name][0]}}') AS estimated_granularity
                            , MIN({{results_granuality[conditional_estimated_granularity_confidence_name][0]}}) AS estimated_granularity_confidence
                        {% endif %}
                    {% endif %}


                    , SUM(COUNT({{col_name}})) OVER () AS cnt_total
                    , SUM(COUNT(DISTINCT {{col_name}})) OVER () AS cnt_unique
                    , {{'COUNT_IF' if db_name=='snowflake' else 'COUNTIF'}}({{col_name}} IS NULL) AS cnt_null

                FROM {{ref(model_name)}}

                GROUP BY ALL

                {% if data_type == 'text' %}
                    LIMIT 10
                {% endif %}

            ) {{ ',' if not loop.last else ''}}
        {% endfor %}

        {# turn the results into a json object #}
        , {{output_name}} AS (
            {% for col_name in results[conditional_col_name] %}
            SELECT
                '{{col_name}}' AS column_name
                , {{'OBJECT_CONSTRUCT' if db_name == 'snowflake' else 'JSON_OBJECT'}}(
                    'column_name', '{{col_name}}'
                    , 'count' , MIN(cnt_total)
                    , 'count_null' , MIN(cnt_null)
                    {% if data_type in ('text', 'boolean') %}
                        {% if data_type == 'text' %}
                            , 'unique' , MIN(cnt_unique)
                        {% endif %}
                        , 'value_counts_top10',{{'OBJECT_AGG(' if db_name == 'snowflake' else 'ARRAY_AGG('}}{{'' if db_name == 'snowflake' else 'JSON_OBJECT('}}{{col_name}}, cnt){{'' if db_name == 'snowflake' else ')'}}
                    {% elif data_type == 'numeric' %}
                        , 'mean' , MIN(avg)
                        , 'percentile_25' , MIN(percentile_25)
                        , 'percentile_50' , MIN(percentile_50)
                        , 'percentile_75' , MIN(percentile_75)
                    {% elif data_type in ('numeric','date') %}
                        , 'min' , MIN(min)
                        , 'max' , MIN(max)
                        {%  if data_type == 'date' %}
                            , 'estimated_granularity' , MIN(estimated_granularity)
                            , 'estimated_granularity_confidence' , MIN(estimated_granularity_confidence)
                        {% endif %}
                    {% endif %}
                ) AS detail
            FROM column_detail_{{col_name}}
            {{ 'UNION ALL' if not loop.last else ''}}
        {% endfor %}

        )
    {% else %}
        {{output_name}} AS (
            SELECT
                '{{col_name}}' AS column_name
                , {{'' if db_name == 'snowflake' else 'TO_JSON'}}(NULL) AS detail
        )
    {% endif %}

{% endmacro %}
