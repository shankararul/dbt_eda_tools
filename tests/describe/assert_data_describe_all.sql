{{
    config(
        enabled = env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1
    )
}}

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
        column_name NOT IN ('IS_SHORT_STRING', 'DATE_DAY', 'STR_LENGTH', 'COMPANY_NAME', 'COUNTRY')
        )
        OR column_name = 'IS_SHORT_STRING' AND NOT (
            dtype = 'boolean' AND "'count'"=54800 AND "'unique'"=5
            AND "'value_counts_top10'" = PARSE_JSON('{"false": 43840, "true": 10960}')
        )
        OR column_name = 'COMPANY_NAME' AND NOT (
            dtype = 'text' AND "'count'"=54800 AND "'unique'"=5
            AND "'value_counts_top10'" = PARSE_JSON('{"A": 10960,"AMZN": 10960,"FB": 10960,"GOG": 10960,"MSFT": 10960}')
        )
        OR column_name = 'COUNTRY' AND NOT (
            dtype = 'text' AND "'count'"=54800 AND "'unique'"=5
            AND "'value_counts_top10'" = PARSE_JSON('{"CA": 10960,"DE": 10960,"FR": 10960,"GB": 10960,"US": 10960}')
        )
        OR column_name = 'STR_LENGTH' AND NOT (
            dtype = 'numeric' AND "'count'"=54800 AND "'unique'"=5
            AND "'max'" = 6 AND "'min'" = 3 AND "'mean'" = 4.8
            AND "'percentile_25'" = 4 AND "'percentile_50'" = 5 AND "'percentile_75'" = 6
        )
        OR column_name = 'DATE_DAY' AND NOT (
            dtype = 'date' AND "'count'"=54800 AND "'estimated_granularity'"='Daily' AND "'estimated_granularity_confidence'"= 1
            AND "'max'" = '2024-12-31' AND "'min'" = '2019-01-01'
        )
    )
