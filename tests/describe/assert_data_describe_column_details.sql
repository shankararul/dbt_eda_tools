WITH
describe_dataframe AS (
    {{dbt_eda_tools.describe('data_generator_enriched_describe')}}
)
SELECT * FROM describe_dataframe
WHERE
    identifier = 'column'
    AND
    (
        (
            LOWER(meta_data_value) = 'text'

            AND (

                detail IS NULL

                OR (
                    meta_data_key = 'COMPANY_NAME' AND (
                    CAST(detail->>'column_name' AS STRING) <> 'COMPANY_NAME'
                    OR CAST(detail->>'count' AS INTEGER) <> 54800
                    OR CAST(detail->>'count_null' AS INTEGER) <> 0
                    OR CAST(detail->>'unique_values' AS INTEGER) <> 5
                    OR CAST(detail->>'value_counts_top10'->>'A' AS INTEGER) <> 10960
                    OR CAST(detail->>'value_counts_top10'->>'AMZN' AS INTEGER) <> 10960
                    OR CAST(detail->>'value_counts_top10'->>'FB' AS INTEGER) <> 10960
                    OR CAST(detail->>'value_counts_top10'->>'GOG' AS INTEGER) <> 10960
                    OR CAST(detail->>'value_counts_top10'->>'MSFT' AS INTEGER) <> 10960
                    )
                )

                OR (
                    meta_data_key = 'COUNTRY' AND (
                    CAST(detail->>'column_name' AS STRING) <> 'COUNTRY'
                    OR CAST(detail->>'count' AS INTEGER) <> 54800
                    OR CAST(detail->>'count_null' AS INTEGER) <> 0
                    OR CAST(detail->>'unique_values' AS INTEGER) <> 5
                    OR CAST(detail->>'value_counts_top10'->>'GB' AS INTEGER) <> 10960
                    OR CAST(detail->>'value_counts_top10'->>'US' AS INTEGER) <> 10960
                    OR CAST(detail->>'value_counts_top10'->>'FR' AS INTEGER) <> 10960
                    OR CAST(detail->>'value_counts_top10'->>'DE' AS INTEGER) <> 10960
                    OR CAST(detail->>'value_counts_top10'->>'CA' AS INTEGER) <> 10960
                    )
                )
            )
        )
        OR (
            LOWER(meta_data_value) = 'number'

            AND (

                detail IS NULL

                OR (
                    meta_data_key = 'STR_LENGTH' AND (
                    CAST(detail->>'column_name' AS STRING) <> 'STR_LENGTH'
                    OR CAST(detail->>'count' AS INTEGER) <> 54800
                    OR CAST(detail->>'count_null' AS INTEGER) <> 0
                    OR CAST(detail->>'max' AS INTEGER) <> 6
                    OR CAST(detail->>'min' AS INTEGER) <> 3
                    OR ROUND(CAST(detail->>'mean' AS FLOAT),1) <> 4.8
                    OR ROUND(CAST(detail->>'percentile_25' AS FLOAT),0) <> 4
                    OR ROUND(CAST(detail->>'percentile_50' AS FLOAT),0) <> 5
                    OR ROUND(CAST(detail->>'percentile_75' AS FLOAT),0) <> 6
                    )
                )
            )
        )
        OR (
            LOWER(meta_data_value) = 'date'

            AND (

                detail IS NULL

                OR (
                    meta_data_key = 'DATE_DAY' AND (
                    CAST(detail->>'column_name' AS STRING) <> 'DATE_DAY'
                    OR CAST(detail->>'estimated_granularity' AS STRING) <> 'Daily'
                    OR CAST(detail->>'estimated_granularity_confidence' AS INTEGER) <> 1
                    OR CAST(detail->>'count' AS INTEGER) <> 54800
                    OR CAST(detail->>'count_null' AS INTEGER) <> 0
                    OR CAST(detail->>'max' AS DATE) <> CAST('2019-01-01' AS DATE)
                    OR CAST(detail->>'min' AS DATE) <> CAST('2024-12-31' AS DATE)
                    )
                )
            )
        )
        OR (
            LOWER(meta_data_value) = 'boolean'

            AND (

                detail IS NULL

                OR (
                    meta_data_key = 'IS_SHORT_STRING' AND (
                    CAST(detail->>'column_name' AS STRING) <> 'IS_SHORT_STRING'
                    OR CAST(detail->>'count' AS INTEGER) <> 54800
                    OR CAST(detail->>'count_null' AS INTEGER) <> 0
                    OR CAST(detail->>'value_counts_top10'->>'false' AS INTEGER) <> 43840
                    OR CAST(detail->>'value_counts_top10'->>'true' AS INTEGER) <> 10960
                    )
                )
            )
        )
    )
