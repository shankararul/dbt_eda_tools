# find_null_columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `find_null_columns` macro to `dbt_eda_tools` that reports columns with null values, callable both as a `dbt run-operation` and as a post-hook logger.

**Architecture:** A single macro in `macros/find_null_columns.sql` that detects its invocation mode by whether `model_name` is provided (ad-hoc) or absent (post-hook using `this`). It queries `INFORMATION_SCHEMA.COLUMNS` to discover column names, then executes a single-scan SQL statement computing null counts per column, filtered to only columns where `null_count > 0`. Results are rendered via `print_pretty_table`.

**Tech Stack:** dbt Jinja macros, BigQuery / Snowflake / DuckDB SQL, `dbt_utils.get_query_results_as_dict`, `run_query`, `print_pretty_table`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `macros/find_null_columns.sql` | Main macro — both invocation modes |
| Modify | `macros/schema.yml` | Document new macro arguments |

---

### Task 1: Write the core `find_null_columns` macro (ad-hoc mode)

**Files:**
- Create: `macros/find_null_columns.sql`

- [ ] **Step 1: Create the macro file with the ad-hoc entry point**

Create `macros/find_null_columns.sql` with this content:

```jinja
{% macro find_null_columns(model_name=None, column_filter=None) %}
    {{ return(adapter.dispatch('find_null_columns', 'dbt_eda_tools')(model_name, column_filter)) }}
{% endmacro %}

{% macro default__find_null_columns(model_name, column_filter) %}

    {# ── Post-hook mode: no model_name supplied, use `this` ── #}
    {% if model_name is none %}
        {% if this is none or this.name is none %}
            {% do return('') %}
        {% endif %}
        {% if not var('dbt_eda_tools_log_enable', false) %}
            {% do return('') %}
        {% endif %}
        {% set target_relation = this %}
        {% set display_name = this.name %}
        {% set information_metadata = ((dbt_eda_tools.fetch_information_metadata(this.name)) | replace("'", "") | replace("[", " ") | replace("]", " ") | trim).split(',') %}
    {# ── Ad-hoc mode: model_name supplied ── #}
    {% else %}
        {% if not execute or not load_relation(ref(model_name)) %}
            {% do return('') %}
        {% endif %}
        {% set target_relation = ref(model_name) %}
        {% set display_name = model_name %}
        {% set information_metadata = ((dbt_eda_tools.fetch_information_metadata(model_name)) | replace("'", "") | replace("[", " ") | replace("]", " ") | trim).split(',') %}
    {% endif %}

    {% set full_path = information_metadata[0] | trim %}
    {% set table_name = information_metadata[1] | trim %}
    {% set db_name = dbt_eda_tools.fetch_db() | trim %}

    {# ── Step 1: discover column names from INFORMATION_SCHEMA ── #}
    {% set column_query %}
        SELECT column_name
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
    {% set col_results = dbt_utils.get_query_results_as_dict(column_query) %}
    {% set columns = col_results[col_key] %}

    {% if not columns or columns | length == 0 %}
        {{ log("find_null_columns: no columns found for " ~ display_name, info=True) }}
        {% do return('') %}
    {% endif %}

    {# ── Step 2: single-scan null count query ── #}
    {% set countif_fn = 'COUNT_IF' if db_name in ('snowflake', 'duckdb') else 'COUNTIF' %}

    {% set null_query %}
        WITH null_counts AS (
            SELECT
                COUNT(*) AS total_rows
                {% for col in columns %}
                , {{ countif_fn }}({{ col }} IS NULL) AS {{ col }}__null_count
                {% endfor %}
            FROM {{ target_relation }}
        )
        {% for col in columns %}
        SELECT
            '{{ col }}' AS column_name
            , {{ col }}__null_count AS null_count
            , ROUND({{ col }}__null_count * 100.0 / NULLIF(total_rows, 0), 2) AS pct_null
            , total_rows
        FROM null_counts
        WHERE {{ col }}__null_count > 0
        {{ 'UNION ALL' if not loop.last else '' }}
        {% endfor %}
        ORDER BY null_count DESC
    {% endset %}

    {% set null_results = run_query(null_query) %}

    {# ── Step 3: render output ── #}
    {% if null_results and null_results.rows | length > 0 %}
        {% set headers = ['column_name', 'null_count', 'pct_null', 'total_rows'] %}
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
        {{ log("\033[1;32m " ~ display_name ~ " — null columns:\033[0m", info=True) }}
        {{ dbt_eda_tools.print_pretty_table(headers, rows) }}
    {% else %}
        {{ log("\033[1;32m✓ No null columns in " ~ display_name ~ "\033[0m", info=True) }}
    {% endif %}

{% endmacro %}
```

- [ ] **Step 2: Verify the file was created**

```bash
ls macros/find_null_columns.sql
```
Expected: file listed.

- [ ] **Step 3: Compile to catch Jinja syntax errors**

```bash
dbt compile
```
Expected: `Done. PASS=...` with no Jinja errors. Ignore any model errors — models are disabled in this package.

- [ ] **Step 4: Commit**

```bash
git add macros/find_null_columns.sql
git commit -m "feat: add find_null_columns macro (ad-hoc and post-hook modes)"
```

---

### Task 2: Document the macro in `schema.yml`

**Files:**
- Modify: `macros/schema.yml`

- [ ] **Step 1: Add the macro entry to `macros/schema.yml`**

Open `macros/schema.yml`. After the last `- name:` block (currently `estimate_build_cost`), append:

```yaml

  - name: find_null_columns
    description: >
      Finds columns containing null values in a dbt model or view.
      Can be called as a run-operation (ad-hoc) or used as a post-hook logger.
      Only columns with at least one null are reported. Results include column_name,
      null_count, pct_null (as percentage), and total_rows.
      Post-hook mode requires dbt_eda_tools_log_enable var to be true.
    arguments:
      - name: model_name
        type: string
        description: >
          The dbt model to inspect. Required for ad-hoc mode (dbt run-operation).
          Omit entirely when using as a post-hook — the macro will use `this` automatically.
      - name: column_filter
        type: list
        description: >
          Optional list of column names to restrict the analysis to.
          Example: ['order_id', 'shipped_at']. Only applies in ad-hoc mode.
```

- [ ] **Step 2: Compile to confirm schema.yml is valid**

```bash
dbt compile
```
Expected: `Done. PASS=...` with no errors.

- [ ] **Step 3: Commit**

```bash
git add macros/schema.yml
git commit -m "docs: document find_null_columns in schema.yml"
```

---

### Task 3: Manual smoke test — ad-hoc mode

> This package targets BigQuery. You need an active dbt profile (`dbt_eda_tools_bq`) and a compiled model available as a ref.

**Files:** none (testing only)

- [ ] **Step 1: Check what example models exist**

```bash
ls examples/
```
The `examples/` directory contains staging models. Pick any model name that resolves via `ref()` in your dev environment.

- [ ] **Step 2: Run ad-hoc against a model with known nulls**

```bash
dbt run-operation find_null_columns --args '{model_name: <your_model_name>}'
```
Expected output: a formatted table printed to console listing only columns where `null_count > 0`, e.g.:
```
 <model_name> — null columns:
-------------------------------------------------------
 column_name   | null_count | pct_null | total_rows
-------------------------------------------------------
 shipped_at    | 430        | 4.30%    | 10000
```

- [ ] **Step 3: Run with column_filter**

```bash
dbt run-operation find_null_columns --args '{model_name: <your_model_name>, column_filter: [<col1>, <col2>]}'
```
Expected: same table format, scoped only to the listed columns.

- [ ] **Step 4: Run against a model with no nulls**

```bash
dbt run-operation find_null_columns --args '{model_name: <clean_model_name>}'
```
Expected console output:
```
✓ No null columns in <clean_model_name>
```

---

### Task 4: Manual smoke test — post-hook mode

**Files:** none (testing only)

- [ ] **Step 1: Enable the log var and run a model**

In your project's `dbt_project.yml` (or via `--vars`), set `dbt_eda_tools_log_enable: true`, then run a model:

```bash
dbt run --select <your_model_name> --vars '{dbt_eda_tools_log_enable: true}'
```
Expected: after the model runs, null columns are printed to the console (or the "no nulls" message if clean).

- [ ] **Step 2: Confirm it is silent when var is false**

```bash
dbt run --select <your_model_name> --vars '{dbt_eda_tools_log_enable: false}'
```
Expected: no null column output printed — macro exits silently.

---

### Task 5: Final commit and version bump (optional)

**Files:**
- Modify: `dbt_project.yml`

- [ ] **Step 1: Bump the patch version in `dbt_project.yml`**

Open `dbt_project.yml`. Change:
```yaml
version: "1.4.0"
```
to:
```yaml
version: "1.5.0"
```

- [ ] **Step 2: Commit**

```bash
git add dbt_project.yml
git commit -m "chore: bump version to 1.5.0 for find_null_columns release"
```
