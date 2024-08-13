WITH
describe_dataframe AS (
    {{describe('data_generator_enriched_describe')}}
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
                    DETAIL:column_name::string <> 'COMPANY_NAME'
                    OR DETAIL:count::integer <> 54800
                    OR DETAIL:count_null::integer <> 0
                    OR DETAIL:unique::integer <> 5
                    OR DETAIL:value_counts_top10:A::int <> 10960
                    OR DETAIL:value_counts_top10:AMZN::int <> 10960
                    OR DETAIL:value_counts_top10:FB::int <> 10960
                    OR DETAIL:value_counts_top10:GOG::int <> 10960
                    OR DETAIL:value_counts_top10:MSFT::int <> 10960
                    )
                )

                OR (
                    meta_data_key = 'COUNTRY' AND (
                    DETAIL:column_name::string <> 'COUNTRY'
                    OR DETAIL:count::integer <> 54800
                    OR DETAIL:count_null::integer <> 0
                    OR DETAIL:unique::integer <> 5
                    OR DETAIL:value_counts_top10:A::int <> 10960
                    OR DETAIL:value_counts_top10:AMZN::int <> 10960
                    OR DETAIL:value_counts_top10:FB::int <> 10960
                    OR DETAIL:value_counts_top10:GOG::int <> 10960
                    OR DETAIL:value_counts_top10:MSFT::int <> 10960
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
                    DETAIL:column_name::string <> 'STR_LENGTH'
                    OR DETAIL:count::integer <> 54800
                    OR DETAIL:count_null::integer <> 0
                    OR DETAIL:max::int <> 6
                    OR DETAIL:min::int <> 3
                    OR DETAIL:mean::float <> 4.8
                    OR ROUND(DETAIL:percentile_25::float,0) <> 4
                    OR ROUND(DETAIL:percentile_50::float,0) <> 5
                    OR ROUND(DETAIL:percentile_75::float,0) <> 6
                    )
                )
            )
        )
    )
