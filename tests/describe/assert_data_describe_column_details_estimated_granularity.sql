WITH
describe_dataframe AS (
    {{describe('data_aggregated_mixed_granularity')}}
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
                    DETAIL:column_name::string <> 'DATE_MIXED_GRANULARITY'
                    OR DETAIL:estimated_granularity::string <> 'Monthly'
                    OR DETAIL:estimated_granularity_confidence::float <> 0.8
                    OR DETAIL:count::integer <> 16
                    OR DETAIL:count_null::integer <> 1
                    OR DETAIL:min::date <> DATE('2019-01-01')
                    OR DETAIL:max::date <> DATE('2023-01-01')
                    )
                )
            )
        )
    )
