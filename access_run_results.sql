-- optimized for big query. creates table in line, may be better to create individual models and run them once. takes path name from the first run model.
{% macro access_run_results(results) %}

    {% set model_results = results | selectattr("node.resource_type", "equalto", "model") | list %}

    {# Set the target destination to the first database/scehma found in the results - may be best to refactor this?#}
    {% set first_model = model_results[0] %}
    {% set log_table = first_model.node.database ~ '.' ~ first_model.node.schema ~ '.dbt_run_logsv3' %}

    {# Build a table for metadata_logging if it doesnt already exist within the system #}
    {% set build %}
        create table if not exists {{ log_table }} (
            model_name STRING,
            unique_id STRING,
            run_id STRING,
            run_status STRING,
            execution_time FLOAT64,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            materialization STRING,
            checksum STRING,
            compiled_code STRING,
            raw_code STRING,
            tags STRING
        );
    {% endset %}
    {% do run_query(build) %}

    {% for result in model_results %}
        {% set node = result.node %}
        {% set timing_info = result.timing | selectattr("name", "equalto", "execute") | list %}
        {% set started_at = timing_info[0].started_at if timing_info and timing_info[0].started_at is not none else none %}
        {% set completed_at = timing_info[0].completed_at if timing_info and timing_info[0].completed_at is not none else none %}

        {# error handle tags to return as json#}
        {% if node.tags is defined and node.tags is not none and node.tags is not undefined %}
            {% set tags_json = node.tags | tojson %}
        {% else %}
            {% set tags_json = '[]' %}
        {% endif %}

        {# Safeguard raw_code #}
        {% if node.raw_code is defined and node.raw_code is not none and node.raw_code is not undefined %}
            {% set raw_code_json = node.raw_code | tojson %}
        {% else %}
            {% set raw_code_json = '""' %}
        {% endif %}
        {% do log(raw_code_json) %}

        {# Safeguard compiled_code #}
        {% if node.compiled_code is defined and node.compiled_code is not none and node.compiled_code is not undefined %}
            {% set compiled_code_json = node.compiled_code | tojson %}
        {% else %}
            {% set compiled_code_json = '""' %}
        {% endif %}

        {# insert the values into the table #}
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
                checksum,
                compiled_code,
                raw_code,
                tags
            )
            values (
                '{{ node.name | replace("'", "''") }}',
                '{{ node.unique_id | replace("'", "''") }}',
                '{{ invocation_id | replace("'", "''") }}',
                '{{ result.status | replace("'", "''") }}',
                {{ result.execution_time }},
                {{ "'" ~ started_at ~ "'" if started_at else "null" }},
                {{ "'" ~ completed_at ~ "'" if completed_at else "null" }},
                '{{ node.config.materialized | replace("'", "''") }}',
                '{{ node.checksum.checksum }}',
                '{{ compiled_code_json | replace("'", "''") }}',
                '{{ raw_code_json | replace("'", "''") }}',
                '{{ tags_json | replace("'", "''") }}'
            );
        {% endset %}

        {% do run_query(sql) %}
    {% endfor %}

    {% do log("✅ log_run_results_to_table macro completed successfully.", info=True) %}    
        
{% endmacro %}
