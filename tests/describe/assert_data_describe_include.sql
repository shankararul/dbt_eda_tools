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
        column_name NOT IN ('is_short_string', 'str_length', 'company_name', 'country')
        )
    )
