{% set information_metadata = ((fetch_information_metadata('data_generator')) | replace("'", "")| replace("[", " ")| replace("]", " ")  | trim).split(',') %}

{% set db_name = information_metadata[2] | trim | replace(" ", "") %}

SELECT
    *
    , IF{{'F' if db_name=='snowflake' else ''}}(str_length<4,True,False)   AS is_short_string
FROM {{ ref('data_generator') }}
