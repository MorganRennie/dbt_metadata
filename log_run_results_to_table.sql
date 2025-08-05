{% macro log_run_results_to_table(results) %}
    {% set first_model = results | selectattr("node.resource_type", "equalto", "model") | list | first %}
    {% set log_table = first_model.node.database ~ '.' ~ first_model.node.schema ~ '.dbt_run_logs' %}

  {% for result in results %}
    {% set node = result.node %}
    {% if node.resource_type == 'model' %}

      {% set timing_info = result.timing | selectattr("name", "equalto", "execute") | list %}
      {% set started_at = timing_info[0].started_at if timing_info and timing_info[0].started_at else none %}
      {% set completed_at = timing_info[0].completed_at if timing_info and timing_info[0].completed_at else none %}

      {% set refs_json = node.refs | tojson %}
      {% set sources_json = node.sources | tojson %}
      {% set tags_json = node.tags | tojson %}
      {% set meta_json = node.meta | tojson %}
      {% set adapter_response_json = result.adapter_response | tojson %}

      {% set raw_code_clean = node.raw_code | replace("'", "''") %}
      {% set compiled_code_clean = node.compiled_code | replace("'", "''") %}

      {% set sql %}
       insert into {{ log_table }} (
          model_name,
          unique_id,
          run_id,
          run_status,
          execution_time,
          started_at,
          completed_at,
          materialization,
          relation_name,
          checksum,
          compiled_code,
          raw_code,
          refs,
          sources,
          adapter_response,
          tags,
          meta
        )
        values (
          '{{ node.name }}',
          '{{ node.unique_id }}',
          '{{ invocation_id }}',
          '{{ result.status }}',
          {{ result.execution_time }},
          {{ "'" ~ started_at ~ "'" if started_at else "null" }},
          {{ "'" ~ completed_at ~ "'" if completed_at else "null" }},
          '{{ node.config.materialized }}',
          '{{ node.relation_name }}',
          '{{ node.checksum.checksum }}',
          '{{ compiled_code_clean }}',
          '{{ raw_code_clean }}',
          '{{ refs_json }}',
          '{{ sources_json }}',
          '{{ adapter_response_json }}',
          '{{ tags_json }}',
          '{{ meta_json }}'
        );
      {% endset %}

      {% do run_query(sql) %}
    {% endif %}
  {% endfor %}
{% endmacro %}
