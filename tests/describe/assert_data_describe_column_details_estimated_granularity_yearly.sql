WITH
describe_dataframe AS (
    {{dbt_eda_tools.describe('data_aggregated_yearly_granularity')}}
)
SELECT * FROM describe_dataframe
WHERE
    identifier = 'column'
    AND
    (
        (
            LOWER(meta_data_value) = 'date'

            AND (

                detail IS NULL

                OR (
                    meta_data_key = 'DATE_MIXED_GRANULARITY' AND (
                    CAST(detail->>'column_name' AS STRING) <> 'DATE_MIXED_GRANULARITY'
                    OR CAST(detail->>'estimated_granularity' AS STRING)  <> '"Yearly"'
                    OR CAST(detail->>'estimated_granularity_confidence' AS INTEGER) <> 1
                    OR CAST(detail->>'count' AS INTEGER) <> 4
                    OR CAST(detail->>'count_null' AS INTEGER) <> 0
                    OR CAST(detail->>'min' AS DATE) <> CAST('2019-01-01' AS DATE)
                    OR CAST(detail->>'max' AS DATE)<> CAST('2022-01-01' AS DATE)
                    )
                )
            )
        )
    )
