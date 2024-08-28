{{
    config(
        materialized = 'view' if env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1 else 'ephemeral'
    )
}}

SELECT
    company_name
    , country
    , count(str_length) AS count_str_length
    -- the percentages are caclulated at the aggregation of company_name and not entire column
    , {{dbt_eda_tools.percent_of_total('str_length','count',3, ['company_name'])}} AS count_percent_level_company_name
    , {{dbt_eda_tools.percent_of_total('str_length')}} AS count_percent_level_full_column

FROM {{ ref('data_aggregated') }}
GROUP BY 1,2
ORDER BY company_name, country, count(str_length)  DESC
