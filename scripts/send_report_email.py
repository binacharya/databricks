#!/usr/bin/env python3
"""
Email a single combined data-quality report via Gmail SMTP, covering both:
- the null percentage check (<catalog>.config.dq_null_check_results)
- the test execution report (<catalog>.config.dq_test_results)

One email, a short "reports attached" note (no data in the body), with all
four files attached (null CSV/PDF, test CSV/PDF).

Runs two ways:
- On Databricks compute (as a job task): uses the native Spark session.
- Standalone (locally or in CI): uses databricks-sql-connector with
  DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN env vars.

  python scripts/send_report_email.py

Sends via Gmail SMTP. Credentials come from SMTP_USERNAME/SMTP_PASSWORD env
vars, falling back to a Databricks secret scope when running on Databricks
compute (see dq_common.get_smtp_credentials / README). If neither is set,
prints both reports and skips emailing instead of failing.
"""

import os
from datetime import datetime, timezone

from dq_common import (
    RESULTS_TABLE,
    TEST_RESULTS_TABLE,
    build_csv_bytes,
    build_pdf_bytes,
    build_test_csv_bytes,
    build_test_pdf_bytes,
    format_report,
    format_test_report,
    get_connector_connection,
    get_spark,
    row_getter,
    run_query,
    send_email,
)


def get_latest_null_results(spark, connection):
    sql = f"""
    SELECT run_ts, catalog, schema_name, table_name, column_name,
           total_rows, null_count, null_pct
    FROM {RESULTS_TABLE}
    ORDER BY table_name, column_name
    """
    rows, columns = run_query(spark, connection, sql)
    results = []
    run_ts = None
    for row in rows:
        get = row_getter(spark, row, columns)
        run_ts = get("run_ts")
        results.append(
            {
                "catalog": get("catalog"),
                "schema_name": get("schema_name"),
                "table_name": get("table_name"),
                "column_name": get("column_name"),
                "total_rows": get("total_rows"),
                "null_count": get("null_count"),
                "null_pct": get("null_pct"),
            }
        )
    return results, run_ts


def get_latest_test_results(spark, connection):
    sql = f"""
    SELECT run_ts, test_name, test_type, file_path, model_name,
           status, execution_time, message
    FROM {TEST_RESULTS_TABLE}
    ORDER BY status, test_name
    """
    rows, columns = run_query(spark, connection, sql)
    results = []
    run_ts = None
    for row in rows:
        get = row_getter(spark, row, columns)
        run_ts = get("run_ts")
        results.append(
            {
                "test_name": get("test_name"),
                "test_type": get("test_type"),
                "file_path": get("file_path"),
                "model_name": get("model_name"),
                "status": get("status"),
                "execution_time": get("execution_time") or 0.0,
                "message": get("message"),
            }
        )
    return results, run_ts


def send_combined_report(null_results, test_results, run_label, spark):
    recipient = os.environ.get("DQ_REPORT_RECIPIENT", "acharyabina01@gmail.com")
    title = f"Data Quality Report — {run_label}"

    html_body = (
        "<div style='font-family:Arial,Helvetica,sans-serif;'>"
        f"<h2 style='color:#2c3e50;margin-bottom:4px;'>{title}</h2>"
        f"<p style='color:#555;'>Find the null percentage and test execution reports attached, run on {run_label}.</p>"
        "</div>"
    )

    attachments = [
        ("null_percentage_report.csv", build_csv_bytes(null_results)),
        ("null_percentage_report.pdf", build_pdf_bytes(null_results, f"Null Percentage Report — {run_label}")),
        ("test_execution_report.csv", build_test_csv_bytes(test_results)),
        ("test_execution_report.pdf", build_test_pdf_bytes(test_results, f"Test Execution Report — {run_label}")),
    ]

    send_email(title, html_body, attachments, recipient, spark)


def main():
    spark = get_spark()
    connection = None if spark is not None else get_connector_connection()

    try:
        null_results, null_run_ts = get_latest_null_results(spark, connection)
        test_results, test_run_ts = get_latest_test_results(spark, connection)

        if not null_results and not test_results:
            print(f"No results found in {RESULTS_TABLE} or {TEST_RESULTS_TABLE} — nothing to email.")
            return

        run_ts = null_run_ts or test_run_ts
        run_label = run_ts.isoformat() if hasattr(run_ts, "isoformat") else str(run_ts or datetime.now(timezone.utc))

        print(format_report(null_results))
        print()
        print(format_test_report(test_results))

        send_combined_report(null_results, test_results, run_label, spark)
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
