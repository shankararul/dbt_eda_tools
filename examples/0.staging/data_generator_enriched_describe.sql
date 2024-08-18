{% set db_name = fetch_db() | trim  %}

SELECT
    *
    , IF{{'F' if db_name=='snowflake' else ''}}(str_length<4,True,False)   AS is_short_string
FROM {{ ref('data_generator') }}
