{% macro describe(model_name) %}
    {{ return(adapter.dispatch('describe', 'dbt_eda_tools')(model_name)) }}
{% endmacro %}

{% macro default__describe(model_name) %}

    {% set information_metadata = ((dbt_eda_tools.fetch_information_metadata(model_name)) | replace("'", "")| replace("[", " ")| replace("]", " ")  | trim).split(',') %}

    {% set full_path = information_metadata[0] | trim%}
    {% set table_name = information_metadata[1] | trim %}
    {% set db_name = dbt_eda_tools.fetch_db() | trim %}

    WITH
    {# fetch meta data about the table from the information schema #}
    {{dbt_eda_tools.fetch_meta_data('meta_data', full_path, db_name, table_name)}}
    {# filter and prepare the meta data for the dataset #}
    , {{dbt_eda_tools.filter_meta_data('dataset_info', 'dataset', 'meta_data', db_name)}}
    {# Add the row count for the dataset #}
    , {{dbt_eda_tools.filter_meta_data('rowcount_info', 'rowcount', ref(model_name), db_name)}}
    {# filter and prepare the meta data for the column types #}
    , {{dbt_eda_tools.filter_meta_data('column_info', 'column', 'meta_data', db_name)}}
    {# Union the above results #}
    , {{dbt_eda_tools.assemble_data(['dataset_info','rowcount_info','column_info'],['index_pos','meta_data_key','meta_data_value','identifier','detail'],'assembled_result', db_name)}}
    , {{dbt_eda_tools.fetch_column_metadata(model_name,'column_detail_info_text', 'text', full_path, db_name, table_name)}}
    , {{dbt_eda_tools.fetch_column_metadata(model_name,'column_detail_info_numeric', 'numeric', full_path, db_name, table_name)}}
    , {{dbt_eda_tools.fetch_column_metadata(model_name,'column_detail_info_date', 'date', full_path, db_name, table_name)}}
    , {{dbt_eda_tools.fetch_column_metadata(model_name,'column_detail_info_bool', 'boolean', full_path, db_name, table_name)}}
    SELECT
        assembed_result.meta_data_key
        , assembed_result.meta_data_value
        , assembed_result.identifier
        , COALESCE(text_detail.detail, numeric_detail.detail,date_detail.detail, bool_detail.detail) AS detail
    FROM assembled_result AS assembed_result
    LEFT JOIN column_detail_info_text AS text_detail
    ON assembed_result.meta_data_key = text_detail.column_name
    LEFT JOIN column_detail_info_numeric AS numeric_detail
    ON assembed_result.meta_data_key = numeric_detail.column_name
    LEFT JOIN column_detail_info_date AS date_detail
    ON assembed_result.meta_data_key = date_detail.column_name
    LEFT JOIN column_detail_info_bool AS bool_detail
    ON assembed_result.meta_data_key = bool_detail.column_name

    ORDER BY index_pos ASC

{% endmacro %}
