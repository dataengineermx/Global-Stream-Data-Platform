{% set since = "'" ~ data_interval_start ~ "'::timestamptz - interval '1 hour'" %}

INSERT INTO ops.pipeline_runs
    (run_id, dag_id, window_start, rows_clean, rows_rejected, rows_fact)
SELECT
    '{{ run_id }}', '{{ dag.dag_id }}', '{{ data_interval_start }}'::timestamptz,
    (SELECT count(*) FROM staging.earthquakes_clean    WHERE updated_at >= {{ since }}),
    (SELECT count(*) FROM staging.earthquakes_rejected WHERE updated_at >= {{ since }}),
    (SELECT count(*) FROM analytics.fct_earthquakes    WHERE updated_at >= {{ since }})
ON CONFLICT (run_id) DO UPDATE SET
    rows_clean    = EXCLUDED.rows_clean,
    rows_rejected = EXCLUDED.rows_rejected,
    rows_fact     = EXCLUDED.rows_fact,
    finished_at   = now();
