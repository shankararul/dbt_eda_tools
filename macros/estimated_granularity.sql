{% macro estimated_granularity(model_name, date_col) %}
    {{ return(adapter.dispatch('estimated_granularity', 'dbt_eda_tools')(model_name, date_col)) }}
{% endmacro %}

{% macro default__estimated_granularity(model_name, date_col) %}

    SELECT
        lag_bucketed AS estimated_granularity
        , {{dbt_eda_tools.percent_of_total('count_total','sum',3)}} AS estimated_granularity_confidence
        FROM (
            SELECT
                CASE
                    WHEN lags_day BETWEEN 28 AND 31 THEN 'Monthly'
                    WHEN lags_day BETWEEN 0 AND 3 THEN 'Daily'
                    WHEN lags_day BETWEEN 363 AND 366 THEN 'Yearly'
                    ELSE 'Unknown'
                END AS lag_bucketed
                , lags_day
                , COUNT(*) AS count_total
            FROM (
                SELECT
                    {{date_col}}
                    , {{datediff( 'LAG(' + date_col +',1) OVER (ORDER BY '+date_col+')', date_col, 'day')}} AS lags_day
                FROM (
                    SELECT DISTINCT {{date_col}}
                    FROM {{ ref(model_name) }}
                )
            )
            WHERE lags_day IS NOT NULL
            GROUP BY ALL
        )
    GROUP BY ALL
    ORDER BY estimated_granularity_confidence DESC
    LIMIT 1

{% endmacro %}
