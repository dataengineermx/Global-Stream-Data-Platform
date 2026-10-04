{% set since = "'" ~ data_interval_start ~ "'::timestamptz - interval '1 hour'" %}

-- Recalcula solo los días tocados por eventos nuevos/actualizados
WITH touched AS (
    SELECT DISTINCT date_trunc('day', event_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS day
    FROM analytics.fct_earthquakes
    WHERE updated_at >= {{ since }}
)
INSERT INTO analytics.agg_earthquakes_daily
    (day, n_events, n_m4_plus, max_mag, avg_mag, avg_depth_km, n_significant, n_tsunami)
SELECT
    t.day,
    count(*),
    count(*) FILTER (WHERE f.magnitude >= 4),
    max(f.magnitude),
    round(avg(f.magnitude), 2),
    round(avg(f.depth_km)::numeric, 2),
    count(*) FILTER (WHERE f.is_significant),
    count(*) FILTER (WHERE f.tsunami)
FROM touched t
JOIN analytics.fct_earthquakes f
  ON date_trunc('day', f.event_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' = t.day
GROUP BY t.day
ON CONFLICT (day) DO UPDATE SET
    n_events      = EXCLUDED.n_events,
    n_m4_plus     = EXCLUDED.n_m4_plus,
    max_mag       = EXCLUDED.max_mag,
    avg_mag       = EXCLUDED.avg_mag,
    avg_depth_km  = EXCLUDED.avg_depth_km,
    n_significant = EXCLUDED.n_significant,
    n_tsunami     = EXCLUDED.n_tsunami;
