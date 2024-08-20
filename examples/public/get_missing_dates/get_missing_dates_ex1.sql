/**
 * Retrieves the missing dates in the date_day column of the missing_day model at the Day level granularity.
 *
 * @param {string} missing_day - The name of the model.
 * @param {string} date_day - The name of the column containing the dates.
 * @param {array} [dimensions] - An optional array of dimensions across which the missing dates are computed.
 * @param {object} [filters] - An optional filter object.
 * @param {string} [expected_frequency] - The interval to use when generating the missing dates (e.g. 'DAY', 'MONTH', 'YEAR').
 * @returns {rows} Rows of missing dates.
 */

{{
    config(
        materialized = 'view' if var('dbt_eda_tools_developer',false) else 'ephemeral'
    )
}}

{{dbt_eda_tools.get_missing_date('missing_day','date_day', [], {}, 'DAY')}}
