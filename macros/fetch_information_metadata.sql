
{% macro fetch_information_metadata(model_name) %}
        {{ adapter.dispatch('fetch_information_metadata')(model_name) }}
{% endmacro %}

{% macro bigquery__fetch_information_metadata(model_name) %}

        {% set relation = ref(model_name) %}
        {% set full_path = (relation|replace(('.`'+relation.identifier+'`'),'')| replace("`", ""))+'.INFORMATION_SCHEMA' %}

        {% set return_value = [full_path,model_name,'bigquery'] %}
        {# {{ return (return_value) }} #}

        {% do return (return_value) %}


{% endmacro %}

{% macro snowflake__fetch_information_metadata(model_name) %}

        {% set full_path = 'INFORMATION_SCHEMA' %}
        {% set return_value = [full_path,model_name|upper,'snowflake']  %}

        {# {{ return (return_value) }} #}

        {% do return (return_value) %}

{% endmacro %}

{% macro default__fetch_information_metadata(model_name) %}

        {% set full_path = 'INFORMATION_SCHEMA' %}
        {% set return_value = [full_path, model_name,'rest']  %}
        {# {{ return (return_value) }} #}

        {% do return (return_value) %}

{% endmacro %}
