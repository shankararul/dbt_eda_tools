
{% macro describe(model_name) %}



    {% set information_metadata = ((fetch_information_metadata(model_name)) | replace("'", "")| replace("[", " ")| replace("]", " ")  | trim).split(',') %}

    {% set full_path = information_metadata[0] | trim%}
    {% set table_name = information_metadata[1] | trim %}
    {% set db_name = information_metadata[2] | trim | replace(" ", "") %}

    WITH
    {# fetch meta data about the table from the information schema #}
    {{fetch_meta_data(full_path, db_name, table_name)}}
    {# filter and prepare the meta data for the dataset #}
    , {{filter_meta_data('dataset', 'meta_data','dataset_info',db_name)}}
    {# Add the row count for the dataset #}
    , {{filter_meta_data('rowcount', ref(model_name),'rowcount_info',db_name)}}
    {# filter and prepare the meta data for the column types #}
    , {{filter_meta_data('column', 'meta_data','column_info',db_name)}}
    {# Union the above results #}
    , {{assemble_data(['dataset_info','rowcount_info','column_info'],['index_pos','meta_data_key','meta_data_value','identifier','detail'],'assembled_result', db_name)}}
    SELECT
        meta_data_key
        , meta_data_value
        , identifier
        , detail
    FROM assembled_result
    ORDER BY index_pos ASC

{% endmacro %}
