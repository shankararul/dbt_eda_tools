---
title: find_null_columns macro
date: 2026-05-11
status: approved
---

# Design: `find_null_columns` macro

## Summary

A new macro for `dbt_eda_tools` that identifies columns containing null values in a dbt model or view. Supports two invocation modes: ad-hoc query (via `dbt run-operation`) and post-hook logger (fires automatically after model build when `dbt_eda_tools_log_enable` is true).

---

## Invocation Modes

### Ad-hoc (run-operation)

```bash
dbt run-operation find_null_columns --args '{model_name: my_model}'
dbt run-operation find_null_columns --args '{model_name: my_model, column_filter: [col_a, col_b]}'
```

- `model_name` (string, required): the dbt model to inspect
- `column_filter` (list, optional): restrict analysis to specific columns
- Returns a result set filtered to only columns where `null_count > 0`
- Output columns: `column_name`, `null_count`, `pct_null`, `total_rows`

### Post-hook

```jinja
{{ dbt_eda_tools.find_null_columns() }}
```

- No arguments — uses `this` (the model currently being materialized)
- Only fires when `var('dbt_eda_tools_log_enable', false)` is `true`
- Prints a formatted table to the console via `print_pretty_table`
- If no nulls found, logs: `✓ No null columns in <model_name>`

---

## Architecture

### File

`macros/find_null_columns.sql`

### Single macro, dual mode

`find_null_columns(model_name=None, column_filter=None)`

- When `model_name` is provided: ad-hoc mode, uses `ref(model_name)`
- When `model_name` is None: post-hook mode, uses `this`
- Guards: returns early if `this is none` (post-hook) or relation doesn't exist (ad-hoc)
- Dispatches via `adapter.dispatch('find_null_columns', 'dbt_eda_tools')` for cross-adapter support

### Execution flow

1. Resolve `full_path`, `table_name`, `db_name` via `fetch_information_metadata` + `fetch_db` (same as `describe`)
2. Query `INFORMATION_SCHEMA.COLUMNS` to get all column names (or subset if `column_filter` provided)
3. Build a single SQL statement — one `COUNTIF(col IS NULL)` expression per column + `COUNT(*)` for total rows — one table scan
4. Wrap in a CTE, filter to `null_count > 0`
5. Ad-hoc: execute via `run_query`, render with `print_pretty_table` and return
6. Post-hook: execute, print result or "no nulls" message, no return value

### Cross-adapter SQL

`COUNTIF` (BigQuery), `COUNT_IF` (Snowflake/DuckDB), `SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END)` fallback — consistent with the branching in `fetch_column_metadata.sql:72`.

---

## Output

### Console (post-hook)

```
| column_name  | null_count | pct_null |
| order_id     | 1240       | 12.40%   |
| shipped_at   | 430        | 4.30%    |
```

### Result set (ad-hoc)

| column_name | null_count | pct_null | total_rows |
|-------------|------------|----------|------------|
| order_id    | 1240       | 12.40    | 10000      |
| shipped_at  | 430        | 4.30     | 10000      |

Only rows where `null_count > 0` are included in both modes.

---

## Schema doc (`macros/schema.yml`)

New entry added:

```yaml
- name: find_null_columns
  description: >
    Finds columns containing null values in a dbt model or view.
    Can be called as a run-operation (ad-hoc) or used as a post-hook logger.
    Only columns with at least one null are reported.
    Post-hook mode requires dbt_eda_tools_log_enable: true.
  arguments:
    - name: model_name
      type: string
      description: The dbt model to inspect. Required for ad-hoc mode; omit for post-hook mode.
    - name: column_filter
      type: list
      description: Optional list of column names to restrict the analysis to.
```

---

## What is NOT in scope

- No changes to `dbt_project.yml` post-hook list (opt-in via `dbt_eda_tools_log_enable` var only)
- No new tests beyond what exists in the package today
- No support for non-dbt tables (raw BigQuery table refs outside of `ref()`)
