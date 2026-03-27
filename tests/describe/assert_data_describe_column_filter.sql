WITH
describe_dataframe AS (
    {{dbt_eda_tools.describe('data_generator_enriched_describe', column_filter=['is_short_string','country','str_length'])}}
)
SELECT * FROM describe_dataframe
WHERE
    1=1
    -- doesnt return columns that are not requested
    AND
    (
        (
        column_name NOT IN ('is_short_string','country','str_length')
        )
    )
