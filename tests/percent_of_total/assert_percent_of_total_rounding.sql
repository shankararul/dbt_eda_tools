WITH
percent_of_total AS (
    SELECT
    country
    -- rounding defaults to 2 if unspecified
    , {{dbt_eda_tools.percent_of_total('str_length','sum')}} AS sum_percent
    , {{dbt_eda_tools.percent_of_total('str_length','sum',2)}} AS sum_percent_3

FROM {{ ref('data_aggregated') }}
GROUP BY 1
)
SELECT * FROM percent_of_total
WHERE
    country = 'GB'
    AND sum_percent <> 0.2
    AND sum_percent <> 0.24
