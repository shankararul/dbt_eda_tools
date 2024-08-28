{{
    config(
        materialized = 'view' if env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1 else 'ephemeral'
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
