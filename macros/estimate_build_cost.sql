{% macro estimate_build_cost() %}
    {{ return(adapter.dispatch('estimate_build_cost', 'dbt_eda_tools')()) }}
{% endmacro %}

{% macro bigquery__estimate_build_cost() %}

       {%- set audit_query -%}
    WITH latest_invocation AS (
      SELECT value AS id
      FROM `{{ target.project }}`.`region-{{ target.location }}`.INFORMATION_SCHEMA.JOBS_BY_USER,
      UNNEST(labels) as label
      WHERE label.key = 'dbt_invocation_id'
        AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
      ORDER BY creation_time DESC
      LIMIT 1
    )
    SELECT
      ROUND(SUM(total_bytes_billed / pow(1024, 4)) * 6.25, 2) AS total_cost_usd,
      ROUND(SUM(total_bytes_billed / pow(1024, 3)), 2) AS total_gb_scanned
    FROM `{{ target.project }}`.`region-{{ target.location }}`.INFORMATION_SCHEMA.JOBS_BY_USER,
    latest_invocation
    CROSS JOIN UNNEST(labels) as label
    WHERE label.key = 'dbt_invocation_id' 
      AND label.value = latest_invocation.id
  {%- endset -%}

  {%- if execute -%}
    {%- set results = run_query(audit_query) -%}
    {%- if results and results.rows | length > 0 -%}
      {%- set cost = results.columns[0].values()[0] -%}
      {%- set gb = results.columns[1].values()[0] -%}
      {{ dbt_eda_tools.print_pretty_table(
          ['Estimated Build Cost (USD)'],
      ) }}
      {{ dbt_eda_tools.print_pretty_table(
          ['GB Scanned', 'Est. Cost (USD)'],
          [[gb | string, '$' ~ cost | string]]
      ) }}
    {%- endif -%}
  {%- endif -%}

{% endmacro %}
