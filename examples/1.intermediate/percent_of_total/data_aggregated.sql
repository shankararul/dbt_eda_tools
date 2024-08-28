{{
    config(
        materialized = 'view' if env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1 else 'ephemeral'
    )
}}

SELECT
    company_name
    , country
    , MIN(str_length) AS str_length
FROM {{ ref('data_generator') }}
WHERE
    --exclude some rows. Keeps only 2 companies for France and 3 countries for MSFT
    NOT (
        (company_name = 'MSFT' AND country = 'DE')
        OR
        (country = 'FR' AND (company_name IN ('GOG','A','MSFT')))
    )
GROUP BY 1,2
