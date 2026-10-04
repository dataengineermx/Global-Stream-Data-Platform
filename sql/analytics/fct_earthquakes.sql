{% set since = "'" ~ data_interval_start ~ "'::timestamptz - interval '1 hour'" %}

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
WHERE updated_at >= {{ since }}
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
