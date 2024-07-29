SELECT
company_name
    , SUM(sum_str_length) AS sum_2_str_length
    , {{percentage_of_total('sum_str_length')}} AS percent_2_str_length
    , SUM(sum_str_length)/SUM(SUM(sum_str_length)) OVER () AS total_sum
FROM {{ ref('data_aggregated') }}
GROUP BY 1
