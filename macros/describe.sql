
{% macro describe(model_name, include=None) %}
    -- depends_on: {{ ref(model_name) }}
    {% if execute and load_relation(ref(model_name)) %}
        {{ return(adapter.dispatch('describe', 'dbt_eda_tools')(model_name,include)) }}
    {% endif %}
{% endmacro %}

{% macro default__describe(model_name,include) %}

    {% set information_metadata = ((dbt_eda_tools.fetch_information_metadata(model_name)) | replace("'", "")| replace("[", " ")| replace("]", " ")  | trim).split(',') %}

    {% set full_path = information_metadata[0] | trim%}
    {% set table_name = information_metadata[1] | trim %}
    {% set db_name = dbt_eda_tools.fetch_db() | trim %}
    {% set model_ref = ref(model_name) %}

    {% set dtype_loop = ['numeric','text','date','boolean'] if include == 'all' or not include else include %}

    WITH
    {# fetch meta data about the table from the information schema #}
    dummy AS (SELECT 1)
    {% if not include %}
        , {{dbt_eda_tools.fetch_meta_data('meta_data', full_path, db_name, table_name)}}
        {# filter and prepare the meta data for the dataset #}
        , {{dbt_eda_tools.filter_meta_data('dataset_info', 'dataset', 'meta_data', db_name)}}
        {# Add the row count for the dataset #}
        , {{dbt_eda_tools.filter_meta_data('rowcount_info', 'rowcount', model_ref, db_name)}}
        {# filter and prepare the meta data for the column types #}
        , {{dbt_eda_tools.filter_meta_data('column_info', 'column', 'meta_data', db_name)}}
        {# Union the above results #}
        , {{dbt_eda_tools.assemble_data(['dataset_info','rowcount_info','column_info'],['index_pos','meta_data_key','meta_data_value','identifier','detail'],'assembled_result', db_name)}}
    {% endif %}
    {% for dtype in dtype_loop %}
        , {{dbt_eda_tools.fetch_column_metadata(model_name,'column_detail_info_'+dtype, dtype, full_path, db_name, table_name)}}
    {% endfor %}

    {% if not include %}
        {{ dbt_eda_tools.summarize_dataset(db_name)}}
    {% else %}
        {{ dbt_eda_tools.summarize_dataset(db_name, dtype_loop)}}
    {% endif %}

{% endmacro %}
