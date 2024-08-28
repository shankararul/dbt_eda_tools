{{
    config(
        enabled = env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1
    )
}}

WITH
describe_dataframe AS (
    {{dbt_eda_tools.describe('data_generator_enriched_describe')}}
)
SELECT * FROM describe_dataframe
WHERE
    1=1
    -- Columns = 4
    AND
    (
        (LOWER(meta_data_key) = 'nbr_of_columns' AND meta_data_value <> 5)
        -- Rows = 54800
        OR (LOWER(meta_data_key) = 'nbr_of_rows' AND meta_data_value <> 54800)
        -- Date Columns = 1
        OR (LOWER(meta_data_key) = 'nbr_of_date_columns' AND meta_data_value <> 1)
        -- Time Columns = 1
        OR (LOWER(meta_data_key) = 'nbr_of_time_columns' AND meta_data_value <> 0)
        -- Boolean Columns = 1
        OR (LOWER(meta_data_key) = 'nbr_of_boolean_columns' AND meta_data_value <> 1)
        -- Text Columns = 1
        OR (LOWER(meta_data_key) = 'nbr_of_text_columns' AND meta_data_value <> 2)
        -- Numeric Columns = 1
        OR (LOWER(meta_data_key) = 'nbr_of_numeric_columns' AND meta_data_value <> 1)
    )
