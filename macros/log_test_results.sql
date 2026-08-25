{#
  Writes a pass/fail/error summary of every test run in this invocation
  (both generic tests from *.yml files and singular tests from tests/*.sql)
  to <catalog>.config.dq_test_results. Called from on-run-end in
  dbt_project.yml, with dbt's built-in `results` variable.

  Each run truncates and replaces the table's contents — same convention as
  <catalog>.config.dq_null_check_results — so it always reflects only the
  latest invocation, not accumulated history.
#}
{% macro log_test_results(results) %}
  {% if execute %}
    {% set test_rows = [] %}

    {% for result in results %}
      {% if result.node.resource_type == 'test' %}
        {% set node = result.node %}
        {% set is_generic = node.original_file_path.endswith('.yml') or node.original_file_path.endswith('.yaml') %}
        {% set related_unique_id = node.attached_node or (node.depends_on.nodes[0] if node.depends_on.nodes else none) %}
        {% set model_name = related_unique_id.split('.')[-1] if related_unique_id else none %}
        {% set message = (result.message or '') | replace('\\', '\\\\') | replace("'", "''") %}
        {% set message = message[:2000] %}

        {% set row = {
            'test_name': node.name,
            'test_type': 'generic' if is_generic else 'singular',
            'file_path': node.original_file_path,
            'model_name': model_name,
            'status': result.status,
            'execution_time': result.execution_time,
            'message': message
        } %}
        {% do test_rows.append(row) %}
      {% endif %}
    {% endfor %}

    {% if test_rows | length > 0 %}
      {% set target_relation = api.Relation.create(database=target.database, schema='config', identifier='dq_test_results') %}

      {% set create_sql %}
        CREATE TABLE IF NOT EXISTS {{ target_relation }} (
          run_ts TIMESTAMP,
          test_name STRING,
          test_type STRING,
          file_path STRING,
          model_name STRING,
          status STRING,
          execution_time DOUBLE,
          message STRING
        ) USING DELTA
      {% endset %}
      {% do run_query(create_sql) %}
      {% do run_query('TRUNCATE TABLE ' ~ target_relation) %}

      {% set values = [] %}
      {% for row in test_rows %}
        {% set model_literal = "'" ~ row.model_name ~ "'" if row.model_name else 'NULL' %}
        {% set value_str = "(current_timestamp(), '" ~ row.test_name ~ "', '" ~ row.test_type ~ "', '" ~ row.file_path
          ~ "', " ~ model_literal ~ ", '" ~ row.status ~ "', " ~ (row.execution_time or 0) ~ ", '" ~ row.message ~ "')" %}
        {% do values.append(value_str) %}
      {% endfor %}

      {% set insert_sql %}
        INSERT INTO {{ target_relation }}
        (run_ts, test_name, test_type, file_path, model_name, status, execution_time, message)
        VALUES {{ values | join(',\n') }}
      {% endset %}
      {% do run_query(insert_sql) %}

      {{ log("Logged " ~ (test_rows | length) ~ " test result(s) to " ~ target_relation, info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
