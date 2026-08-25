"""
Shared helpers for the data-quality scripts (check_null_percentage.py,
send_null_report_email.py). Not a standalone entry point.
"""

import os

DEFAULT_CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
CONFIG_SCHEMA = os.environ.get("DQ_CONFIG_SCHEMA", "config")
CONFIG_TABLE = f"{DEFAULT_CATALOG}.{CONFIG_SCHEMA}.dq_tables_config"
RESULTS_TABLE = f"{DEFAULT_CATALOG}.{CONFIG_SCHEMA}.dq_null_check_results"


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


def build_pdf_bytes(results, title):
    from fpdf import FPDF

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, title)
    pdf.ln(12)

    col_widths = [70, 55, 45, 30, 30, 25]
    headers = ["Table", "Column", "Catalog.Schema", "Total Rows", "Null Count", "Null %"]

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(44, 62, 80)
    pdf.set_text_color(255, 255, 255)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 8, h, border=1, fill=True)
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    if not results:
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 8, "No results.", border=1)
    for r in results:
        row_cells = [
            r["table_name"],
            r["column_name"],
            f"{r['catalog']}.{r['schema_name']}",
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
