{% macro find_null_columns(model_name=None, column_filter=None) %}
    {% if model_name is none %}
        {# Post-hook mode: returns empty string (side-effect only) #}
        {{ return(adapter.dispatch('find_null_columns_posthook', 'dbt_eda_tools')(column_filter)) }}
    {% else %}
        {# Ad-hoc model body mode: renders SQL directly #}
        -- depends_on: {{ ref(model_name) }}
        {{ return(adapter.dispatch('find_null_columns', 'dbt_eda_tools')(model_name, column_filter)) }}
    {% endif %}
{% endmacro %}

{% macro default__find_null_columns(model_name, column_filter) %}
    {# Ad-hoc mode: renders SQL so dbt can materialize results as a view/table #}
    {% if not execute or not load_relation(ref(model_name)) %}
        SELECT CAST(NULL AS STRING) AS column_name, CAST(NULL AS STRING) AS issue_type, 0 AS null_or_empty_count, 0.0 AS pct_null_or_empty, 0 AS total_rows WHERE FALSE
    {% else %}
        {% set target_relation = ref(model_name) %}
        {% set information_metadata = ((dbt_eda_tools.fetch_information_metadata(model_name)) | replace("'", "") | replace("[", " ") | replace("]", " ") | trim).split(',') %}
        {% set full_path = information_metadata[0] | trim %}
        {% set table_name = information_metadata[1] | trim %}
        {% set db_name = dbt_eda_tools.fetch_db() | trim %}

        {% set column_query %}
            SELECT column_name, data_type
            FROM {{ full_path }}.COLUMNS
            WHERE table_name = '{{ table_name }}'
            {% if column_filter %}
                AND column_name IN (
                    {% for col in column_filter -%}
                    '{{ col }}'{% if not loop.last %},{% endif %}
                    {%- endfor %}
                )
            {% endif %}
            ORDER BY ordinal_position
        {% endset %}

        {% set col_key = 'COLUMN_NAME' if db_name == 'snowflake' else 'column_name' %}
        {% set dtype_key = 'DATA_TYPE' if db_name == 'snowflake' else 'data_type' %}
        {% set col_results = dbt_utils.get_query_results_as_dict(column_query) %}
        {% set columns = col_results[col_key] %}
        {% set data_types = col_results[dtype_key] %}

        {% if not columns or columns | length == 0 %}
            SELECT CAST(NULL AS STRING) AS column_name, CAST(NULL AS STRING) AS issue_type, 0 AS null_or_empty_count, 0.0 AS pct_null_or_empty, 0 AS total_rows WHERE FALSE
        {% else %}
            {% set string_types = ['STRING', 'VARCHAR', 'TEXT', 'CHAR', 'CHARACTER', 'NVARCHAR'] %}
            {% set countif_fn = 'COUNT_IF' if db_name in ('snowflake', 'duckdb') else 'COUNTIF' %}
            WITH null_counts AS (
                SELECT
                    COUNT(*) AS total_rows
                    {% for col in columns %}
                    {% set is_string = data_types[loop.index0] | upper in string_types %}
                    , {{ countif_fn }}(
                        `{{ col }}` IS NULL{% if is_string %} OR CAST(`{{ col }}` AS STRING) = ''{% endif %}
                    ) AS `{{ col }}__null_or_empty_count`
                    {% endfor %}
                FROM {{ target_relation }}
            )
            {% for col in columns %}
            SELECT
                '{{ col }}' AS column_name
                , `{{ col }}__null_or_empty_count` AS null_or_empty_count
                , ROUND(`{{ col }}__null_or_empty_count` * 100.0 / NULLIF(total_rows, 0), 2) AS pct_null_or_empty
                , total_rows
            FROM null_counts
            WHERE `{{ col }}__null_or_empty_count` > 0
            {{ 'UNION ALL' if not loop.last else '' }}
            {% endfor %}
            ORDER BY null_or_empty_count DESC
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro default__find_null_columns_posthook(column_filter) %}
    {# Post-hook mode: executes query and prints results, returns empty string #}
    {% if this is none or this.name is none %}
        {% do return('') %}
    {% endif %}
    {% if not var('dbt_eda_tools_log_enable', false) %}
        {% do return('') %}
    {% endif %}

    {% set information_metadata = ((dbt_eda_tools.fetch_information_metadata(this.name)) | replace("'", "") | replace("[", " ") | replace("]", " ") | trim).split(',') %}
    {% set full_path = information_metadata[0] | trim %}
    {% set table_name = information_metadata[1] | trim %}
    {% set db_name = dbt_eda_tools.fetch_db() | trim %}

    {% set column_query %}
        SELECT column_name, data_type
        FROM {{ full_path }}.COLUMNS
        WHERE table_name = '{{ table_name }}'
        {% if column_filter %}
            AND column_name IN (
                {% for col in column_filter -%}
                '{{ col }}'{% if not loop.last %},{% endif %}
                {%- endfor %}
            )
        {% endif %}
        ORDER BY ordinal_position
    {% endset %}

    {% set col_key = 'COLUMN_NAME' if db_name == 'snowflake' else 'column_name' %}
    {% set dtype_key = 'DATA_TYPE' if db_name == 'snowflake' else 'data_type' %}
    {% set col_results = dbt_utils.get_query_results_as_dict(column_query) %}
    {% set columns = col_results[col_key] %}
    {% set data_types = col_results[dtype_key] %}

    {% if not columns or columns | length == 0 %}
        {{ log("find_null_columns: no columns found for " ~ this.name, info=True) }}
        {% do return('') %}
    {% endif %}

    {% set string_types = ['STRING', 'VARCHAR', 'TEXT', 'CHAR', 'CHARACTER', 'NVARCHAR'] %}
    {% set countif_fn = 'COUNT_IF' if db_name in ('snowflake', 'duckdb') else 'COUNTIF' %}

    {% set null_query %}
        WITH null_counts AS (
            SELECT
                COUNT(*) AS total_rows
                {% for col in columns %}
                {% set is_string = data_types[loop.index0] | upper in string_types %}
                , {{ countif_fn }}(
                    `{{ col }}` IS NULL{% if is_string %} OR CAST(`{{ col }}` AS STRING) = ''{% endif %}
                ) AS `{{ col }}__null_or_empty_count`
                {% endfor %}
            FROM {{ this }}
        )
        {% for col in columns %}
        SELECT
            '{{ col }}' AS column_name
            , `{{ col }}__null_or_empty_count` AS null_or_empty_count
            , ROUND(`{{ col }}__null_or_empty_count` * 100.0 / NULLIF(total_rows, 0), 2) AS pct_null_or_empty
            , total_rows
        FROM null_counts
        WHERE `{{ col }}__null_or_empty_count` > 0
        {{ 'UNION ALL' if not loop.last else '' }}
        {% endfor %}
        ORDER BY null_or_empty_count DESC
    {% endset %}

    {% set null_results = run_query(null_query) %}

    {% if null_results and null_results.rows | length > 0 %}
        {% set headers = ['column_name', 'null_or_empty_count', 'pct_null_or_empty', 'total_rows'] %}
        {% set rows = [] %}
        {% for row in null_results.rows %}
            {% set formatted_row = [
                row[0] | string,
                row[1] | string,
                row[2] | string ~ '%',
                row[3] | string
            ] %}
            {% if rows.append(formatted_row) %}{% endif %}
        {% endfor %}
        {{ dbt_eda_tools.print_pretty_table(headers, rows) }}
    {% endif %}

    {% do return('') %}
{% endmacro %}
