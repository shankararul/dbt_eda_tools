-- Asserts that columns with no nulls or empty strings do not appear in results.
-- date_day, company_name, country, str_length, is_short_string have no nulls/empties
-- in the base data_generator_enriched_describe source.
WITH result AS (
    {{ dbt_eda_tools.find_null_columns('set_null_values_columns') }}
)
SELECT column_name
FROM result
WHERE column_name IN ('date_day', 'company_name', 'country', 'str_length', 'is_short_string')
