{% set since = "'" ~ data_interval_start ~ "'::timestamptz - interval '1 hour'" %}

-- 1) Ventana de trabajo (solapada 1h para tolerar actualizaciones tardías)
DROP TABLE IF EXISTS tmp_src;
CREATE TEMP TABLE tmp_src ON COMMIT DROP AS
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
WHERE e.updated >= {{ since }};

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
