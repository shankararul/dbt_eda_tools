SELECT
    *
FROM {{ ref('data_aggregated_mixed_granularity') }}
WHERE date_mixed_granularity <= CAST('2022-01-01' AS DATE)
