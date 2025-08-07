-- models/logs/dbt_run_logs.sql
{{ config(materialized='table') }}

select
    cast(null as string) as model_name,
    cast(null as string) as unique_id,
    cast(null as string) as run_id,
    cast(null as string) as run_status,
    cast(null as float) as execution_time,
    cast(null as timestamp) as started_at,
    cast(null as timestamp) as completed_at,
    cast(null as string) as materialization,
    cast(null as string) as relation_name,
    cast(null as string) as checksum,
    cast(null as string) as compiled_code,
    cast(null as string) as raw_code,
    cast(null as string) as refs,
    cast(null as string) as sources,
    cast(null as string) as adapter_response,
    cast(null as string) as tags,
    cast(null as string) as meta
where false

-- on-run-end:
--  - "{{ log_run_results_to_table(results) }}"
