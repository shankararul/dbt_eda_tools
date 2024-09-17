def model(dbt,session):

    dbt.config(materialized="table")
    return dbt.ref("print_table1").describe()
