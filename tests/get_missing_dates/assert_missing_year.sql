{{
    config(
        enabled = env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1
    )
}}

WITH
missing_values AS (
    {{dbt_eda_tools.get_missing_date('missing_year','date_year', [], {}, 'YEAR')}}
)
, row_count_missing_values AS (
    SELECT COUNT(missing_year) AS row_count
    FROM missing_values
)
SELECT * FROM row_count_missing_values WHERE row_count <> 1
