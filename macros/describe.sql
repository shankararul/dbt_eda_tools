
{% macro describe(model_name) %}



    {% set information_metadata = ((fetch_information_metadata(model_name)) | replace("'", "")| replace("[", " ")| replace("]", " ")  | trim).split(',') %}

    {% set full_path = information_metadata[0] | trim%}
    {% set table_name = information_metadata[1] | trim %}
    {% set db_name = information_metadata[2] | trim | replace(" ", "") %}

    WITH
    {# fetch meta data about the table from the information schema #}
    {{fetch_meta_data('meta_data', full_path, db_name, table_name)}}
    {# filter and prepare the meta data for the dataset #}
    , {{filter_meta_data('dataset_info', 'dataset', 'meta_data', db_name)}}
    {# Add the row count for the dataset #}
    , {{filter_meta_data('rowcount_info', 'rowcount', ref(model_name), db_name)}}
    {# filter and prepare the meta data for the column types #}
    , {{filter_meta_data('column_info', 'column', 'meta_data', db_name)}}
    {# Union the above results #}
    , {{assemble_data(['dataset_info','rowcount_info','column_info'],['index_pos','meta_data_key','meta_data_value','identifier','detail'],'assembled_result', db_name)}}
    , {{fetch_column_metadata(ref(model_name),'column_detail_info', full_path, db_name, table_name)}}
    SELECT
        assembed_result.meta_data_key
        , assembed_result.meta_data_value
        , assembed_result.identifier
        , column_detail_info.detail
    FROM assembled_result AS assembed_result
    LEFT JOIN column_detail_info AS column_detail_info
    ON assembed_result.meta_data_key = column_detail_info.column_name
    ORDER BY index_pos ASC

{% endmacro %}
