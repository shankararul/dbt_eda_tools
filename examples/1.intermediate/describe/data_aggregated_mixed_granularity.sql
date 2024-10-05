SELECT
    company_name
    , country
    , CASE
        WHEN date_day <= CAST('2022-01-01' AS DATE)  THEN CAST({{ date_trunc("year", "date_day") }} AS Date)
        WHEN date_day BETWEEN CAST('2022-01-01' AS DATE) AND CAST('2023-01-01' AS DATE) THEN CAST({{ date_trunc("month", "date_day") }} AS DATE)
        {# ELSE date_day #}
    END AS date_mixed_granularity
FROM {{ ref('data_generator') }}
WHERE company_name = 'FB' AND country = 'US'
GROUP BY 1,2,3
