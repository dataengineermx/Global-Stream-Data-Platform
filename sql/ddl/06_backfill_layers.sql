-- Backfill ÚNICO de capas: procesa TODO raw.earthquakes -> staging -> fct -> agg diario.
-- Ejecutar una vez (sin -1 / sin transacción única es suficiente). Luego: scripts/backfill_geo.sh


-- ===== staging/earthquakes_clean.sql =====
-- 1) Ventana de trabajo (solapada 1h para tolerar actualizaciones tardías)
DROP TABLE IF EXISTS tmp_src;
CREATE TEMP TABLE tmp_src AS
SELECT
    e.*,
    CASE
        WHEN e.mag IS NULL                                  THEN 'mag_null'
        WHEN e.mag NOT BETWEEN -2 AND 10                    THEN 'mag_out_of_range'
        WHEN e.latitude  NOT BETWEEN -90  AND 90            THEN 'lat_out_of_range'
        WHEN e.longitude NOT BETWEEN -180 AND 180           THEN 'lon_out_of_range'
        WHEN e.depth_km IS NOT NULL
             AND e.depth_km NOT BETWEEN -5 AND 800          THEN 'depth_out_of_range'
        WHEN e."time" > now() + interval '5 minutes'        THEN 'future_event'
        WHEN lower(trim(e.event_type)) <> 'earthquake'      THEN 'not_earthquake'
    END AS reject_reason
FROM raw.earthquakes e
WHERE e.updated >= '-infinity'::timestamptz;

-- 2) Quita de la tabla contraria los ids que cambiaron de estado
DELETE FROM staging.earthquakes_clean    c USING tmp_src s WHERE c.id = s.id AND s.reject_reason IS NOT NULL;
DELETE FROM staging.earthquakes_rejected r USING tmp_src s WHERE r.id = s.id AND s.reject_reason IS NULL;

-- 3) Cuarentena
INSERT INTO staging.earthquakes_rejected (id, updated_at, reason, payload)
SELECT id, updated, reject_reason, to_jsonb(s) - 'reject_reason'
FROM tmp_src s
WHERE reject_reason IS NOT NULL
ON CONFLICT (id) DO UPDATE SET
    updated_at  = EXCLUDED.updated_at,
    reason      = EXCLUDED.reason,
    payload     = EXCLUDED.payload,
    rejected_at = now();

-- 4) Registros limpios, tipados y normalizados
INSERT INTO staging.earthquakes_clean
    (id, event_time, updated_at, latitude, longitude, depth_km, mag,
     mag_type, place, status, alert, felt, sig, tsunami)
SELECT
    id, "time", updated, latitude, longitude, depth_km,
    round(mag::numeric, 1),
    NULLIF(lower(trim(mag_type)), ''),
    NULLIF(trim(place), ''),
    NULLIF(lower(trim(status)), ''),
    NULLIF(lower(trim(alert)), ''),
    felt, sig, tsunami
FROM tmp_src
WHERE reject_reason IS NULL
ON CONFLICT (id) DO UPDATE SET
    event_time = EXCLUDED.event_time,
    updated_at = EXCLUDED.updated_at,
    latitude   = EXCLUDED.latitude,
    longitude  = EXCLUDED.longitude,
    depth_km   = EXCLUDED.depth_km,
    mag        = EXCLUDED.mag,
    mag_type   = EXCLUDED.mag_type,
    place      = EXCLUDED.place,
    status     = EXCLUDED.status,
    alert      = EXCLUDED.alert,
    felt       = EXCLUDED.felt,
    sig        = EXCLUDED.sig,
    tsunami    = EXCLUDED.tsunami;

DROP TABLE IF EXISTS tmp_src;


-- ===== analytics/fct_earthquakes.sql =====
-- Si un evento pasó a cuarentena, sácalo de la tabla final
DELETE FROM analytics.fct_earthquakes f
USING staging.earthquakes_rejected r
WHERE f.event_id = r.id;

INSERT INTO analytics.fct_earthquakes
    (event_id, event_time, updated_at, latitude, longitude, depth_km, magnitude,
     mag_type, place, region, mag_class, depth_class, is_significant,
     tsunami, felt, alert, sig)
SELECT
    id, event_time, updated_at, latitude, longitude, depth_km, mag,
    mag_type, place,
    -- "10 km SW of Ometepec, Mexico" -> "Mexico"; sin coma -> place completo
    NULLIF(trim(substring(place FROM '([^,]+)$')), ''),
    CASE WHEN mag < 3 THEN 'micro'
         WHEN mag < 4 THEN 'menor'
         WHEN mag < 5 THEN 'ligero'
         WHEN mag < 6 THEN 'moderado'
         WHEN mag < 7 THEN 'fuerte'
         ELSE 'mayor' END,
    CASE WHEN depth_km IS NULL THEN NULL
         WHEN depth_km < 70    THEN 'superficial'
         WHEN depth_km < 300   THEN 'intermedio'
         ELSE 'profundo' END,
    (COALESCE(sig, 0) >= 600 OR mag >= 6),
    tsunami, felt, alert, sig
FROM staging.earthquakes_clean
WHERE updated_at >= '-infinity'::timestamptz
ON CONFLICT (event_id) DO UPDATE SET
    event_time     = EXCLUDED.event_time,
    updated_at     = EXCLUDED.updated_at,
    latitude       = EXCLUDED.latitude,
    longitude      = EXCLUDED.longitude,
    depth_km       = EXCLUDED.depth_km,
    magnitude      = EXCLUDED.magnitude,
    mag_type       = EXCLUDED.mag_type,
    place          = EXCLUDED.place,
    region         = EXCLUDED.region,
    mag_class      = EXCLUDED.mag_class,
    depth_class    = EXCLUDED.depth_class,
    is_significant = EXCLUDED.is_significant,
    tsunami        = EXCLUDED.tsunami,
    felt           = EXCLUDED.felt,
    alert          = EXCLUDED.alert,
    sig            = EXCLUDED.sig;


-- ===== analytics/agg_earthquakes_daily.sql =====
-- Recalcula solo los días tocados por eventos nuevos/actualizados
WITH touched AS (
    SELECT DISTINCT date_trunc('day', event_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS day
    FROM analytics.fct_earthquakes
    WHERE updated_at >= '-infinity'::timestamptz
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
