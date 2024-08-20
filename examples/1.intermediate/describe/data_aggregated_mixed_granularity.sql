{{
    config(
        materialized = 'view' if var('dbt_eda_tools_developer',false) else 'ephemeral'
    )
}}

SELECT
    company_name
    , country
    , CASE
        WHEN date_day <= DATE('2022-01-01')  THEN CAST({{ date_trunc("year", "date_day") }} AS Date)
        WHEN date_day BETWEEN DATE('2022-01-01') AND DATE('2023-01-01') THEN CAST({{ date_trunc("month", "date_day") }} AS Date)
        {# ELSE date_day #}
    END AS date_mixed_granularity
FROM {{ ref('data_generator') }}
WHERE company_name = 'FB' AND country = 'US'
GROUP BY 1,2,3
