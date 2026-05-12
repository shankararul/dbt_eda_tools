-- Asserts the result has the expected 4 output columns with correct names.
-- Returns rows on failure (any unexpected column name present).
WITH result AS (
    {{ dbt_eda_tools.find_null_columns('set_null_values_columns') }}
)
SELECT *
FROM result
WHERE
    column_name IS NULL
    OR null_or_empty_count IS NULL
    OR pct_null_or_empty IS NULL
    OR total_rows IS NULL
