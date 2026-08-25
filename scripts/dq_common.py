"""
Shared helpers for the data-quality scripts (check_null_percentage.py,
send_report_email.py). Not a standalone entry point.
"""

import os

DEFAULT_CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
CONFIG_SCHEMA = os.environ.get("DQ_CONFIG_SCHEMA", "config")
CONFIG_TABLE = f"{DEFAULT_CATALOG}.{CONFIG_SCHEMA}.dq_tables_config"
RESULTS_TABLE = f"{DEFAULT_CATALOG}.{CONFIG_SCHEMA}.dq_null_check_results"
TEST_RESULTS_TABLE = f"{DEFAULT_CATALOG}.{CONFIG_SCHEMA}.dq_test_results"


def get_spark():
    """Return a live SparkSession if running on Databricks compute, else None."""
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return None
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def get_connector_connection():
    from databricks import sql as databricks_sql

    return databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def run_query(spark, connection, sql):
    """Run a query via Spark (list of Row) or the SQL connector (rows + column names)."""
    if spark is not None:
        return spark.sql(sql).collect(), None
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [d[0] for d in cursor.description] if cursor.description else []
    cursor.close()
    return rows, columns


def row_getter(spark, row, columns):
    if spark is not None:
        return lambda key: row[key]
    idx = {c: i for i, c in enumerate(columns)}
    return lambda key: row[idx[key]]


def get_smtp_credentials(spark):
    """SMTP_USERNAME/SMTP_PASSWORD env vars first (standalone/local/CI runs);
    if unset and running on Databricks compute, falls back to a secret scope
    (default "dq-report", override via DQ_SECRET_SCOPE)."""
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


def send_email(subject, html_body, attachments, recipient, spark):
    """attachments: list of (filename, bytes) tuples. Returns True if sent."""
    smtp_username, smtp_password = get_smtp_credentials(spark)
    if not smtp_username or not smtp_password:
        print(
            "No SMTP credentials found (checked SMTP_USERNAME/SMTP_PASSWORD env vars, "
            "then the Databricks secret scope) — skipping email."
        )
        return False

    import smtplib
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = smtp_username
    msg["To"] = recipient
    msg.attach(MIMEText(html_body, "html"))

    for filename, content in attachments:
        part = MIMEApplication(content, Name=filename)
        part["Content-Disposition"] = f'attachment; filename="{filename}"'
        msg.attach(part)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, [recipient], msg.as_string())

    print(f"Emailed report (HTML body + {len(attachments)} attachment(s)) to {recipient}.")
    return True


def format_report(results):
    if not results:
        return "No results."
    header = f"{'table':<45} {'column':<30} {'total_rows':>12} {'null_count':>12} {'null_pct':>10}"
    lines = [header, "-" * len(header)]
    for r in results:
        full_table = f"{r['catalog']}.{r['schema_name']}.{r['table_name']}"
        lines.append(
            f"{full_table:<45} {r['column_name']:<30} {r['total_rows']:>12} "
            f"{r['null_count']:>12} {r['null_pct']:>9.2f}%"
        )
    return "\n".join(lines)


def _severity_color(null_pct):
    """Red/orange/green thresholds shared by the HTML and PDF report builders."""
    if null_pct >= 5:
        return (192, 57, 43)  # red
    if null_pct > 0:
        return (214, 137, 16)  # orange
    return (30, 132, 73)  # green


def build_csv_bytes(results):
    import csv
    import io

    output = io.StringIO()
    fieldnames = ["catalog", "schema_name", "table_name", "column_name", "total_rows", "null_count", "null_pct"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in results:
        writer.writerow(r)
    return output.getvalue().encode("utf-8")


def build_html_report(results):
    if not results:
        return "<p>No results.</p>"

    rows_html = []
    for r in results:
        color = "rgb{}".format(_severity_color(r["null_pct"]))
        rows_html.append(
            "<tr>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;'>{r['catalog']}.{r['schema_name']}.{r['table_name']}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;'>{r['column_name']}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;text-align:right;'>{r['total_rows']}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;text-align:right;'>{r['null_count']}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;text-align:right;color:{color};font-weight:bold;'>"
            f"{r['null_pct']:.2f}%</td>"
            "</tr>"
        )

    return (
        "<table style='border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:13px;width:100%;'>"
        "<thead><tr style='background:#2c3e50;color:#ffffff;'>"
        "<th style='padding:8px 10px;text-align:left;'>Table</th>"
        "<th style='padding:8px 10px;text-align:left;'>Column</th>"
        "<th style='padding:8px 10px;text-align:right;'>Total Rows</th>"
        "<th style='padding:8px 10px;text-align:right;'>Null Count</th>"
        "<th style='padding:8px 10px;text-align:right;'>Null %</th>"
        "</tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
    )


def _pdf_safe(text):
    """fpdf2's core fonts (Helvetica etc.) only support Latin-1, not full
    Unicode — swap common punctuation for ASCII equivalents, then drop
    anything else that still doesn't fit rather than crashing the run."""
    text = str(text)
    replacements = {
        "—": "-",  # em dash
        "–": "-",  # en dash
        "‘": "'",
        "’": "'",
        "“": '"',
        "”": '"',
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    return text.encode("latin-1", "replace").decode("latin-1")


def build_pdf_bytes(results, title):
    from fpdf import FPDF

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, _pdf_safe(title))
    pdf.ln(12)

    col_widths = [70, 55, 45, 30, 30, 25]
    headers = ["Table", "Column", "Catalog.Schema", "Total Rows", "Null Count", "Null %"]

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(44, 62, 80)
    pdf.set_text_color(255, 255, 255)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 8, _pdf_safe(h), border=1, fill=True)
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    if not results:
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 8, "No results.", border=1)
    for r in results:
        row_cells = [
            _pdf_safe(r["table_name"]),
            _pdf_safe(r["column_name"]),
            _pdf_safe(f"{r['catalog']}.{r['schema_name']}"),
            str(r["total_rows"]),
            str(r["null_count"]),
            f"{r['null_pct']:.2f}%",
        ]
        color = _severity_color(r["null_pct"])
        for i, (w, cell_text) in enumerate(zip(col_widths, row_cells)):
            if i == len(row_cells) - 1:
                pdf.set_text_color(*color)
            else:
                pdf.set_text_color(0, 0, 0)
            pdf.cell(w, 7, cell_text, border=1)
        pdf.ln()

    return bytes(pdf.output())


def _test_status_color(status):
    """Red/orange/gray/green thresholds shared by the test-report HTML/PDF builders."""
    if status == "pass":
        return (30, 132, 73)  # green
    if status in ("fail", "error"):
        return (192, 57, 43)  # red
    if status == "warn":
        return (214, 137, 16)  # orange
    return (127, 140, 141)  # gray (skipped or other)


def format_test_report(results):
    if not results:
        return "No results."
    header = f"{'test':<45} {'type':<10} {'model':<25} {'status':<10} {'time(s)':>8}"
    lines = [header, "-" * len(header)]
    for r in results:
        lines.append(
            f"{r['test_name']:<45} {r['test_type']:<10} {(r['model_name'] or ''):<25} "
            f"{r['status']:<10} {r['execution_time']:>8.2f}"
        )
    return "\n".join(lines)


def build_test_csv_bytes(results):
    import csv
    import io

    output = io.StringIO()
    fieldnames = ["test_name", "test_type", "file_path", "model_name", "status", "execution_time", "message"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in results:
        writer.writerow(r)
    return output.getvalue().encode("utf-8")


def build_test_html_report(results):
    if not results:
        return "<p>No results.</p>"

    rows_html = []
    for r in results:
        color = "rgb{}".format(_test_status_color(r["status"]))
        rows_html.append(
            "<tr>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;'>{r['test_name']}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;'>{r['test_type']}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;'>{r['model_name'] or ''}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;color:{color};font-weight:bold;'>{r['status']}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;text-align:right;'>{r['execution_time']:.2f}s</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;'>{r['message'] or ''}</td>"
            "</tr>"
        )

    return (
        "<table style='border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:13px;width:100%;'>"
        "<thead><tr style='background:#2c3e50;color:#ffffff;'>"
        "<th style='padding:8px 10px;text-align:left;'>Test</th>"
        "<th style='padding:8px 10px;text-align:left;'>Type</th>"
        "<th style='padding:8px 10px;text-align:left;'>Model</th>"
        "<th style='padding:8px 10px;text-align:left;'>Status</th>"
        "<th style='padding:8px 10px;text-align:right;'>Time</th>"
        "<th style='padding:8px 10px;text-align:left;'>Message</th>"
        "</tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
    )


def build_test_pdf_bytes(results, title):
    from fpdf import FPDF

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, _pdf_safe(title))
    pdf.ln(12)

    col_widths = [65, 22, 45, 20, 18, 100]
    headers = ["Test", "Type", "Model", "Status", "Time(s)", "Message"]

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(44, 62, 80)
    pdf.set_text_color(255, 255, 255)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 8, _pdf_safe(h), border=1, fill=True)
    pdf.ln()

    pdf.set_font("Helvetica", "", 8)
    if not results:
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 8, "No results.", border=1)
    for r in results:
        row_cells = [
            _pdf_safe(r["test_name"]),
            _pdf_safe(r["test_type"]),
            _pdf_safe(r["model_name"] or ""),
            _pdf_safe(r["status"]),
            f"{r['execution_time']:.2f}",
            _pdf_safe((r["message"] or "")[:90]),
        ]
        color = _test_status_color(r["status"])
        for i, (w, cell_text) in enumerate(zip(col_widths, row_cells)):
            if i == 3:
                pdf.set_text_color(*color)
            else:
                pdf.set_text_color(0, 0, 0)
            pdf.cell(w, 7, cell_text, border=1)
        pdf.ln()

    return bytes(pdf.output())
