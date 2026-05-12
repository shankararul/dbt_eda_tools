-- Asserts that columns with null values are detected.
-- col1_as_null is 100% null, col3_as_some_null has partial nulls.
-- Returns rows on failure (expected column missing from results).
WITH result AS (
    {{ dbt_eda_tools.find_null_columns('set_null_values_columns') }}
)
, expected AS (
    SELECT 'col1_as_null' AS column_name
    UNION ALL
    SELECT 'col3_as_some_null'
)
SELECT expected.column_name
FROM expected
LEFT JOIN result USING (column_name)
WHERE result.column_name IS NULL
