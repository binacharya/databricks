#!/usr/bin/env python3
"""
Check null/None percentage per column for tables listed in a config table
(seeded from seeds/dq_tables_config.csv into <catalog>.config.dq_tables_config).

Runs two ways:
- On Databricks compute (as a job task): uses the native Spark session, no
  credentials needed.
- Standalone (locally or in CI): uses databricks-sql-connector with
  DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN env vars, same
  convention as the rest of this project (see profiles.yml.example).

Results are printed and written to <catalog>.config.dq_null_check_results. Each
run truncates the table first, so it always holds only the latest run's rows
— it is not a history table.

Emailing the report is handled separately by send_report_email.py --report-type
null (a different job task), which reads the results table this script writes.
"""

from datetime import datetime, timezone

from dq_common import (
    CONFIG_TABLE,
    RESULTS_TABLE,
    format_report,
    get_connector_connection,
    get_spark,
    row_getter,
    run_query,
)


def get_enabled_tables(spark, connection):
    sql = f"SELECT catalog, schema_name, table_name FROM {CONFIG_TABLE} WHERE enabled = true"
    rows, columns = run_query(spark, connection, sql)
    tables = []
    for row in rows:
        get = row_getter(spark, row, columns)
        tables.append((get("catalog"), get("schema_name"), get("table_name")))
    return tables


def get_columns(spark, connection, catalog, schema, table):
    sql = f"DESCRIBE TABLE {catalog}.{schema}.{table}"
    rows, columns = run_query(spark, connection, sql)
    col_names = []
    for row in rows:
        name = row["col_name"] if spark is not None else row[0]
        if not name or name.startswith("#"):
            break
        col_names.append(name)
    return col_names


def check_table_nulls(spark, connection, catalog, schema, table):
    columns = get_columns(spark, connection, catalog, schema, table)
    if not columns:
        return []

    null_exprs = ",\n    ".join(
        f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `{c}__nulls`" for c in columns
    )
    sql = f"""
    SELECT COUNT(*) AS total_rows,
    {null_exprs}
    FROM {catalog}.{schema}.{table}
    """
    rows, result_columns = run_query(spark, connection, sql)
    get = row_getter(spark, rows[0], result_columns)
    total_rows = get("total_rows")

    results = []
    for c in columns:
        null_count = get(f"{c}__nulls") or 0
        null_pct = round((null_count / total_rows) * 100, 2) if total_rows else 0.0
        results.append(
            {
                "catalog": catalog,
                "schema_name": schema,
                "table_name": table,
                "column_name": c,
                "total_rows": total_rows,
                "null_count": null_count,
                "null_pct": null_pct,
            }
        )
    return results


def ensure_results_table(spark, connection):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
        run_ts TIMESTAMP,
        catalog STRING,
        schema_name STRING,
        table_name STRING,
        column_name STRING,
        total_rows BIGINT,
        null_count BIGINT,
        null_pct DOUBLE
    ) USING DELTA
    """
    run_query(spark, connection, sql)


def write_results(spark, connection, results, run_ts):
    if not results:
        return

    if spark is not None:
        from pyspark.sql import Row

        rows = [Row(run_ts=run_ts, **r) for r in results]
        spark.createDataFrame(rows).write.format("delta").mode("overwrite").saveAsTable(RESULTS_TABLE)
        return

    # TRUNCATE first so each run replaces the previous one's rows rather than
    # accumulating history.
    run_query(spark, connection, f"TRUNCATE TABLE {RESULTS_TABLE}")

    values = ",\n".join(
        "('{run_ts}', '{catalog}', '{schema_name}', '{table_name}', '{column_name}', "
        "{total_rows}, {null_count}, {null_pct})".format(run_ts=run_ts.isoformat(), **r)
        for r in results
    )
    sql = f"""
    INSERT INTO {RESULTS_TABLE}
    (run_ts, catalog, schema_name, table_name, column_name, total_rows, null_count, null_pct)
    VALUES {values}
    """
    run_query(spark, connection, sql)


def main():
    spark = get_spark()
    connection = None if spark is not None else get_connector_connection()

    try:
        ensure_results_table(spark, connection)
        tables = get_enabled_tables(spark, connection)
        if not tables:
            print(f"No enabled tables found in {CONFIG_TABLE}.")
            return

        run_ts = datetime.now(timezone.utc)
        all_results = []
        for catalog, schema, table in tables:
            print(f"Checking {catalog}.{schema}.{table} ...")
            all_results.extend(check_table_nulls(spark, connection, catalog, schema, table))

        print(format_report(all_results))
        write_results(spark, connection, all_results, run_ts)
        print(f"\nWrote {len(all_results)} rows to {RESULTS_TABLE}.")
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
