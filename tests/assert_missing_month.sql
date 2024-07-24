WITH
missing_values AS (
    {{get_missing_date('missing_month','date_month', [], {}, 'MONTH')}}
)
, row_count_missing_values AS (
    SELECT COUNT(missing_month) AS row_count
    FROM missing_values
)
SELECT * FROM row_count_missing_values WHERE row_count <> 1
