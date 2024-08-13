<img align="center" src="./images/dbt_eda_tools_black.png" alt="dbt_eda_tools_logo" style='display:block; margin-left: auto;margin-right: auto;' height="auto">

<p align="center">
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-ff69b4?style=plastic"/>
    <img alt="Static Badge" src="https://img.shields.io/badge/dbt-package-orange">
    <img alt="GitHub Release" src="https://img.shields.io/github/v/release/shankararul/dbt_eda_tools">
    <img alt="GitHub (Pre-)Release Date" src="https://img.shields.io/github/release-date-pre/shankararul/dbt_eda_tools">
</p>

<p align="center">
    <img src="https://img.shields.io/circleci/project/github/badges/shields/master" alt="build status">
    <img alt="GitHub issues" src="https://img.shields.io/github/issues/shankararul/dbt_eda_tools">
    <img alt="GitHub pull requests" src="https://img.shields.io/github/issues-pr/shankararul/dbt_eda_tools">
    <img src="https://img.shields.io/github/contributors/shankararul/dbt_eda_tools" />
</p>

# dbt_eda_tools
## A medley of macros that could be handy for your Exploratory Data Analysis in DBT.

✅ Get Missing Dates
`Finds all the missing dates in a model for the specified dimensions and filters according to the time granularity expected`

✅ Show as Percentage
`Shows the value as percentage of the total value for the specified aggregations`

✅ Exploratory data analysis

> ✅ describe()
`Returns metadata on the model, including the number of rows, the number of columns, and the number of columns by data type (numeric, text, date...).`

> ✅ Categoric column exploration
`Get summary statistics such as Count, Unique values, Null values for categoric columns`

> ✅ Numeric column exploration
`Get summary statistics such as Min, Max, Median, Null values, Percentiles etc. for numeric columns`


> 🚧 Timeseries column exploration (Coming soon)
`Get summary statistics such as Start date, End date, granularity of the timeseries (day,month,year), null values, missing dates for timeseries columns`

🚧 Fill Missing Dates (Coming soon)
`Fills the missing dates in a model for the specified dimensions and filters according to the time granularity expected`


| DB | Status |
| ------ | ------ |
| Snowflake (default) | ✅ |
| Bigquery | ✅ |
| Duckdb | 🔜 |

# 💾 Install

`dbt_eda_tools` currently supports `dbt 1.6.x` or higher.

Include in `packages.yml`

```yaml
packages:
  - package: shankararul/dbt_eda_tools
    version: ">=0.6.0"
```
[Read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

[Latest Release](https://github.com/shankararul/dbt_eda_tools/releases) of dbt_eda_tools


# 🔨 Examples

### Get Missing Dates

```sh
get_missing_date(model_name, date_col, dimensions, filters, expected_frequency)
```

### [Example 1](examples/public/get_missing_dates/get_missing_dates_ex1.sql)
> ➡️ Input
```sh
{{dbt_eda_tools.get_missing_date('missing_day','date_day', [], {}, 'DAY')}}
```

> ⬅️ Output
<img align="center" src="./images/get_missing_date_ex1.png" alt="describe structure" style='display:block; margin-left: auto;margin-right: auto;' height="auto">

> 👓 Explanation
 ```
 Finds all the missing dates For the date_day column in the missing_day model with the expected_frequency set to DAY across all dimensions without any filters

 Returns 1 row with the missing dates
 ```

### [Example 2](examples/public/get_missing_dates/get_missing_dates_ex2.sql)
> ➡️ Input
```sh
{{dbt_eda_tools.get_missing_date('missing_month','date_month', ['country'], {}, 'MONTH')}}
```

> ⬅️ Output
<img align="center" src="./images/get_missing_date_ex2.png" alt="describe structure" style='display:block; margin-left: auto;margin-right: auto;' height="auto">
> 👓 Explanation
 ```
 Finds all the missing dates For the `date_month` column in the `missing_month` model with the `expected_frequency` set to `MONTH` for each of the countries without any filters.

 Returns 6 rows with the missing dates
 ```

### [Example 3](examples/public/get_missing_dates/get_missing_dates_ex3.sql)
> ➡️ Input
```sh

{{
    dbt_eda_tools.get_missing_date(
        'missing_day'
        ,'date_day'
        , ['country','company_name']
        , {
            'country': ('DE','US')
            , 'company_name': 'MSFT'
            , 'str_length': '>2'
        }
        , 'DAY'
    )
}}
```

> ⬅️ Output
<img align="center" src="./images/get_missing_date_ex3.png" alt="describe structure" style='display:block; margin-left: auto;margin-right: auto;' height="auto">
> 👓 Explanation
 ```
 Finds all the missing dates For the `daye_day` column in the `missing_day` model with the `expected_frequency` set to `DAY` for each of the countries and companies with the country as `DE` or `US` and the company name as `MSFT` and the string length greater than 2.

 Returns 3 rows with the missing dates
 ```

💁 Note: You can send in numeric comparison operators as filters as well within quotes ['=3'](examples/public/get_missing_dates_ex4.sql) or '!=3'


### Show as Percent of total

```sh
percent_of_total(column_to_aggregate, aggregation,precision,level)
```

### [Example 1](examples/public/percent_of_total/percent_of_total_ex1.sql)
> ➡️ Input
```sh
SELECT
    country
    -- example: sum
    , SUM(str_length) AS sum_2_str_length
    , {{dbt_eda_tools.percent_of_total('str_length','sum',3)}} AS sum_percent

    -- example: count
    , COUNT(company_name) AS count_company_name
    -- defaults to count if no aggregation function is specified and 2 decimals if no precision is specified
    , {{dbt_eda_tools.percent_of_total('company_name', precision=3)}} AS count_percent

FROM ref('data_aggregated')
GROUP BY 1
```

> ⬅️ Output
<img align="center" src="./images/percent_of_total_ex1.png" alt="describe structure" style='display:block; margin-left: auto;margin-right: auto;' height="auto">

> 👓 Explanation
 ```
 The `sum_percent` column is the percentage of the total sum of the str_length column for each country. The `count_percent` column is the percentage of the total count of the company_name column for each country.
 ```

### [Example 2](examples/public/percent_of_total/percent_of_total_ex2.sql)
> ➡️ Input
```sh
SELECT
    company_name
    , country
    , count(str_length) AS count_str_length
    -- the percentages are caclulated at the aggregation of company_name and not entire column
    , {{dbt_eda_tools.percent_of_total('str_length','count',3, ['company_name'])}} AS count_percent

FROM {{ ref('data_aggregated') }}
GROUP BY 1,2
```

> ⬅️ Output
<img align="center" src="./images/percent_of_total_ex2.png" alt="describe structure" style='display:block; margin-left: auto;margin-right: auto;' height="auto">

> 👓 Explanation
 ```
 The percentages are calculated at the level of company_name and not the entire column. Hence the percentages of MSFT sum to 1 and GOG sum to 1.
 ```

### Describe

```sh
describe(model_name)
```

### [Example 1](examples/public/describe/describe_ex1.sql)
> ➡️ Input
```sh
{{describe('data_generator_enriched_describe')}}
```

> ⬅️ Output
<img align="center" src="./images/describe_ex1.png" alt="describe structure" style='display:block; margin-left: auto;margin-right: auto;' height="auto">

> 👓 Explanation
 ```
 This macro returns a table with the number of rows, columns, date columns, text columns, numeric columns, boolean columns and time columns in the input model. The output loosely and closely intends to replicate the behavior of pd.describe() in pandas.
 ```


# 🔧 Contribution
If you'd like to contribute, please do open a Pull Request or an Issue. Feel free to [reach out to me](https://linkedin.com/in/shankararul) should you have any questions.
