#!/usr/bin/env python3
"""
Email a data-quality report via Gmail SMTP — either the null percentage
report (<catalog>.config.dq_null_check_results) or the test execution report
(<catalog>.config.dq_test_results). Which one is chosen via --report-type
(or DQ_REPORT_TYPE env var), so this single script backs two separate job
tasks (send_null_report, send_test_report) with different `parameters` in
resources/DBT_automation_job _from_code.yml.

Runs two ways:
- On Databricks compute (as a job task): uses the native Spark session.
- Standalone (locally or in CI): uses databricks-sql-connector with
  DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN env vars.

  python scripts/send_report_email.py --report-type null
  python scripts/send_report_email.py --report-type test

Sends via Gmail SMTP. Credentials come from SMTP_USERNAME/SMTP_PASSWORD env
vars, falling back to a Databricks secret scope when running on Databricks
compute (see dq_common.get_smtp_credentials / README). If neither is set,
prints the report and skips emailing instead of failing.
"""

import argparse
import os
from datetime import datetime, timezone

from dq_common import (
    RESULTS_TABLE,
    TEST_RESULTS_TABLE,
    build_csv_bytes,
    build_html_report,
    build_pdf_bytes,
    build_test_csv_bytes,
    build_test_html_report,
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


def send_null_report(results, run_label, spark):
    recipient = os.environ.get("DQ_REPORT_RECIPIENT", "acharyabina01@gmail.com")
    title = f"Null Percentage Report — {run_label}"

    html_body = (
        "<div style='font-family:Arial,Helvetica,sans-serif;'>"
        f"<h2 style='color:#2c3e50;margin-bottom:4px;'>{title}</h2>"
        "<p style='color:#555;margin-top:0;'>"
        "Null percentage per column, color-coded (green = 0%, orange = &lt;5%, red = &ge;5%). "
        "The same data is attached as CSV and PDF."
        "</p>"
        f"{build_html_report(results)}"
        "</div>"
    )
    attachments = [
        ("null_percentage_report.csv", build_csv_bytes(results)),
        ("null_percentage_report.pdf", build_pdf_bytes(results, title)),
    ]
    send_email(title, html_body, attachments, recipient, spark)


def send_test_report(results, run_label, spark):
    recipient = os.environ.get("DQ_REPORT_RECIPIENT", "acharyabina01@gmail.com")
    title = f"Test Execution Report — {run_label}"

    failed = sum(1 for r in results if r["status"] in ("fail", "error"))
    passed = sum(1 for r in results if r["status"] == "pass")

    html_body = (
        "<div style='font-family:Arial,Helvetica,sans-serif;'>"
        f"<h2 style='color:#2c3e50;margin-bottom:4px;'>{title}</h2>"
        "<p style='color:#555;margin-top:0;'>"
        f"{passed} passed, {failed} failed/errored out of {len(results)} tests. "
        "Status color-coded (green = pass, red = fail/error, orange = warn, gray = other). "
        "The same data is attached as CSV and PDF."
        "</p>"
        f"{build_test_html_report(results)}"
        "</div>"
    )
    attachments = [
        ("test_execution_report.csv", build_test_csv_bytes(results)),
        ("test_execution_report.pdf", build_test_pdf_bytes(results, title)),
    ]
    send_email(title, html_body, attachments, recipient, spark)


REPORTS = {
    "null": {
        "fetch": get_latest_null_results,
        "send": send_null_report,
        "format": format_report,
        "table": RESULTS_TABLE,
    },
    "test": {
        "fetch": get_latest_test_results,
        "send": send_test_report,
        "format": format_test_report,
        "table": TEST_RESULTS_TABLE,
    },
}


def main():
    parser = argparse.ArgumentParser(description="Email a data-quality report.")
    parser.add_argument(
        "--report-type",
        choices=sorted(REPORTS),
        default=os.environ.get("DQ_REPORT_TYPE", "null"),
        help="Which report to send. Default: DQ_REPORT_TYPE env var, or 'null'.",
    )
    args = parser.parse_args()
    report = REPORTS[args.report_type]

    spark = get_spark()
    connection = None if spark is not None else get_connector_connection()

    try:
        results, run_ts = report["fetch"](spark, connection)
        if not results:
            print(f"No results found in {report['table']} — nothing to email.")
            return

        run_label = run_ts.isoformat() if hasattr(run_ts, "isoformat") else str(run_ts or datetime.now(timezone.utc))
        print(report["format"](results))
        report["send"](results, run_label, spark)
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
