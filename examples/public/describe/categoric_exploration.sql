
-- depends_on: {{ ref('data_generator_enriched_describe') }}

{% set information_metadata = ((fetch_information_metadata('data_generator_enriched_describe')) | replace("'", "")| replace("[", " ")| replace("]", " ")  | trim).split(',') %}

{% set full_path = information_metadata[0] | trim%}
{% set table_name = information_metadata[1] | trim %}
{% set db_name = information_metadata[2] | trim | replace(" ", "") %}

{{fetch_column_metadata(full_path, db_name, table_name)}}
