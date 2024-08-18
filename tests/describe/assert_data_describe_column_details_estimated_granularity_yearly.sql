WITH
describe_dataframe AS (
    {{describe('data_aggregated_yearly_granularity')}}
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
                    OR DETAIL:estimated_granularity::string <> 'Yearly'
                    OR DETAIL:estimated_granularity_confidence::float <> 1
                    OR DETAIL:count::integer <> 4
                    OR DETAIL:count_null::integer <> 0
                    OR DETAIL:min::date <> DATE('2019-01-01')
                    OR DETAIL:max::date <> DATE('2022-01-01')
                    )
                )
            )
        )
    )
