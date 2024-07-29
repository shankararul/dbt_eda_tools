SELECT
    company_name
    , country
    , EXTRACT(YEAR FROM date_day) AS date_year
    , SUM(str_length) AS sum_str_length
FROM {{ ref('data_generator') }}
GROUP BY 1,2,3
