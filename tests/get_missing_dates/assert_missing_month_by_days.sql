WITH
missing_values AS (
    {{dbt_eda_tools.get_missing_date('missing_month','date_month', [], {}, 'DAY')}}
)
, row_count_missing_values AS (
    SELECT COUNT(missing_day) AS row_count
    FROM missing_values
)
SELECT * FROM row_count_missing_values WHERE row_count <> 67
