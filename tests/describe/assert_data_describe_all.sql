WITH
describe_dataframe AS (
    {{dbt_eda_tools.describe('data_generator_enriched_describe', include='all')}}
)
SELECT * FROM describe_dataframe
WHERE
    1=1
    -- 5 column names as rows
    AND
    (
        (
        column_name NOT IN ('is_short_string', 'date_day', 'str_length', 'company_name', 'country')
        )
        OR column_name = 'is_short_string' AND NOT (
            dtype = 'boolean' AND CAST("count" AS INTEGER)=54800 AND CAST("unique_values" AS INTEGER)=5
            AND CAST(value_counts_top10->>'false' AS INTEGER) = 43840 AND CAST(value_counts_top10->>'true' AS INTEGER) = 10960
        )
        OR column_name = 'company_name' AND NOT (
            dtype = 'text' AND CAST("count" AS INTEGER)=54800 AND CAST("unique_values" AS INTEGER)=5
            AND CAST(value_counts_top10->>'AMZN' AS INTEGER) = 10960 AND CAST(value_counts_top10->>'A' AS INTEGER) = 10960 AND CAST(value_counts_top10->>'FB' AS INTEGER) = 10960 AND CAST(value_counts_top10->>'MSFT' AS INTEGER) = 10960 AND CAST(value_counts_top10->>'GOG' AS INTEGER) = 10960
        )
        OR column_name = 'country' AND NOT (
            dtype = 'text' AND CAST("count" AS INTEGER)=54800 AND CAST("unique_values" AS INTEGER)=5
            AND CAST(value_counts_top10->>'CA' AS INTEGER) = 10960 AND CAST(value_counts_top10->>'GB' AS INTEGER) = 10960 AND CAST(value_counts_top10->>'FR' AS INTEGER) = 10960 AND CAST(value_counts_top10->>'DE' AS INTEGER) = 10960 AND CAST(value_counts_top10->>'US' AS INTEGER) = 10960

        )
        OR column_name = 'str_length' AND NOT (
            dtype = 'numeric' AND CAST("count" AS INTEGER)=54800 AND CAST("unique_values" AS INTEGER)=5
            AND "max" = 6 AND "min" = 3 AND "mean" = 4.8
            AND "percentile_25" = 4 AND "percentile_50" = 5 AND "percentile_75" = 6
        )
        OR column_name = 'date_day' AND NOT (
            dtype = 'date' AND CAST("count" AS INTEGER)=54800 AND "estimated_granularity"='"Daily"' AND CAST("estimated_granularity_confidence" AS INTEGER)= 1
            AND "max" = '"2024-12-31 00:00:00"' AND "min" = '"2019-01-01 00:00:00"'
        )
    )
