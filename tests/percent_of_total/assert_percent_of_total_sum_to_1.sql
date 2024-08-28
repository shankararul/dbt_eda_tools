{{
    config(
        enabled =  env_var('DBT_EDA_TOOLS_DEVELOPER',0)| int == 1
    )
}}

WITH
percent_of_total AS (
    SELECT
    country
    -- precision needs to be atleast 4 for the sum to be 100%
    , {{dbt_eda_tools.percent_of_total('str_length','sum',4)}} AS sum_percent
    , {{dbt_eda_tools.percent_of_total('company_name', 'count', 4)}} AS count_percent
    , {{dbt_eda_tools.percent_of_total('company_name','countdistinct',4)}} AS count_distinct_percent
    , {{dbt_eda_tools.percent_of_total('company_name','incorrectagg')}} AS incorrect_returns_null

FROM {{ ref('data_aggregated') }}
GROUP BY 1
)
, sum_percent_of_total AS (
    SELECT
        sum(sum_percent) AS sum_percent
        , sum(count_percent) AS count_percent
        , sum(count_distinct_percent) AS count_distinct_percent
        , sum(incorrect_returns_null) AS incorrect_returns_null
    FROM percent_of_total
)
SELECT * FROM sum_percent_of_total
WHERE
    sum_percent<>1
    OR count_percent<>1
    OR count_distinct_percent<>1
    OR incorrect_returns_null IS NOT NULL
