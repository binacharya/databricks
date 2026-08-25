#!/usr/bin/env python3
"""
Email the null percentage report written by check_null_percentage.py to
<catalog>.config.dq_null_check_results. Intended to run as the job task right
after null_percentage_check, but works standalone too.

Runs two ways, same as check_null_percentage.py:
- On Databricks compute (as a job task): uses the native Spark session.
- Standalone (locally or in CI): uses databricks-sql-connector with
  DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN env vars.

Sends via Gmail SMTP. Credentials are looked up two ways:
- SMTP_USERNAME / SMTP_PASSWORD env vars (for standalone/local/CI runs).
- If those aren't set and this is running on Databricks compute, falls back
  to a Databricks secret scope (default "dq-report", override via
  DQ_SECRET_SCOPE) with keys "smtp-username" / "smtp-password". See README
  for the `databricks secrets` CLI commands to set this up once.

If neither source has credentials, prints the report and skips emailing
instead of failing. The email body is an HTML table (color-coded by null %),
with the same data attached as both a CSV and a PDF.
"""

import os
from datetime import datetime, timezone

from dq_common import (
    RESULTS_TABLE,
    build_csv_bytes,
    build_html_report,
    build_pdf_bytes,
    format_report,
    get_connector_connection,
    get_spark,
    row_getter,
    run_query,
)


def get_latest_results(spark, connection):
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


def _get_smtp_credentials(spark):
    smtp_username = os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    if smtp_username and smtp_password:
        return smtp_username, smtp_password

    if spark is None:
        return None, None

    scope = os.environ.get("DQ_SECRET_SCOPE", "dq-report")
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        smtp_username = dbutils.secrets.get(scope=scope, key="smtp-username")
        smtp_password = dbutils.secrets.get(scope=scope, key="smtp-password")
        return smtp_username, smtp_password
    except Exception as e:
        print(f"Could not read SMTP credentials from secret scope '{scope}': {e}")
        return None, None


def send_report_email(results, run_label, spark):
    smtp_username, smtp_password = _get_smtp_credentials(spark)
    if not smtp_username or not smtp_password:
        print(
            "No SMTP credentials found (checked SMTP_USERNAME/SMTP_PASSWORD env vars, "
            "then the Databricks secret scope) — skipping email."
        )
        return

    recipient = os.environ.get("DQ_REPORT_RECIPIENT", "acharyabina01@gmail.com")

    import smtplib
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    title = f"Null Percentage Report — {run_label}"

    msg = MIMEMultipart("mixed")
    msg["Subject"] = title
    msg["From"] = smtp_username
    msg["To"] = recipient

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
    msg.attach(MIMEText(html_body, "html"))

    csv_part = MIMEApplication(build_csv_bytes(results), Name="null_percentage_report.csv")
    csv_part["Content-Disposition"] = 'attachment; filename="null_percentage_report.csv"'
    msg.attach(csv_part)

    pdf_part = MIMEApplication(build_pdf_bytes(results, title), Name="null_percentage_report.pdf")
    pdf_part["Content-Disposition"] = 'attachment; filename="null_percentage_report.pdf"'
    msg.attach(pdf_part)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, [recipient], msg.as_string())

    print(f"Emailed report (HTML body + CSV + PDF attachments) to {recipient}.")


def main():
    spark = get_spark()
    connection = None if spark is not None else get_connector_connection()

    try:
        results, run_ts = get_latest_results(spark, connection)
        if not results:
            print(f"No results found in {RESULTS_TABLE} — nothing to email.")
            return

        run_label = run_ts.isoformat() if hasattr(run_ts, "isoformat") else str(run_ts or datetime.now(timezone.utc))
        print(format_report(results))
        send_report_email(results, run_label, spark)
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
