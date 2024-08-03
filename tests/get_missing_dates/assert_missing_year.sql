WITH
missing_values AS (
    {{get_missing_date('missing_year','date_year', [], {}, 'YEAR')}}
)
, row_count_missing_values AS (
    SELECT COUNT(missing_year) AS row_count
    FROM missing_values
)
SELECT * FROM row_count_missing_values WHERE row_count <> 1
