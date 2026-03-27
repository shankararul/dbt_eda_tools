WITH
describe_dataframe AS (
    {{dbt_eda_tools.describe('data_generator_enriched_describe', column_filter=['is_short_string','country','str_length'])}}
)
SELECT * FROM (
    SELECT count(*) AS row_count_needs_to_be_3 FROM describe_dataframe
)
WHERE row_count_needs_to_be_3 != 3
