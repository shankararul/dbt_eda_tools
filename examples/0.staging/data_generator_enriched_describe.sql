{{
    config(
        materialized = 'view' if env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1 else 'ephemeral'
    )
}}

{% set db_name = fetch_db() | trim  %}

SELECT
    *
    , IF{{'F' if db_name=='snowflake' else ''}}(str_length<4,True,False)   AS is_short_string
FROM {{ ref('data_generator') }}
