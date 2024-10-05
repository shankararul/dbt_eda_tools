SELECT
    country
    , SUM(str_length) AS sum_2_str_length
    -- example: sum
    , {{dbt_eda_tools.percent_of_total('str_length','sum',3)}} AS sum_percent

    , COUNT(company_name) AS count_company_name
    -- example: count
    -- defaults to count if no aggregation function is specified and 1 decimal if no precision is specified

    , {{dbt_eda_tools.percent_of_total('company_name', precision=3)}} AS count_percent

    , COUNT(DISTINCT company_name) AS count_distinct_company_name
    -- example: countdistinct
    , {{dbt_eda_tools.percent_of_total('company_name','countdistinct', precision=3)}} AS count_distinct_percent

FROM {{ ref('data_aggregated') }}
GROUP BY 1
ORDER BY sum(str_length)  DESC
