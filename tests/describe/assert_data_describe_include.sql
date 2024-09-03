{{
    config(
        enabled = env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1
    )
}}

WITH
describe_dataframe AS (
    {{dbt_eda_tools.describe('data_generator_enriched_describe', include=['text','boolean','numeric'])}}
)
SELECT * FROM describe_dataframe
WHERE
    1=1
    -- filtered out the date column
    AND
    (
        (
        column_name NOT IN ('IS_SHORT_STRING', 'STR_LENGTH', 'COMPANY_NAME', 'COUNTRY')
        )
    )
