{% set since = "'" ~ data_interval_start ~ "'::timestamptz - interval '1 hour'" %}

-- 1) Fuera de la caja de México: sin contexto (evita calcular distancias inútiles)
UPDATE analytics.fct_earthquakes
SET cve_ent = NULL, estado = NULL, cve_mun = NULL, municipio = NULL,
    dist_km_to_land = NULL, in_mexico_land = false
WHERE updated_at >= {{ since }}
  AND NOT (longitude BETWEEN -120 AND -84 AND latitude BETWEEN 10 AND 34);

-- 2) Dentro de la caja: municipio que contiene al sismo o, si cae en el mar,
--    el más cercano (estado/municipio solo si está a <= 150 km)
WITH ev AS (
    SELECT event_id, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS pt
    FROM analytics.fct_earthquakes
    WHERE updated_at >= {{ since }}
      AND longitude BETWEEN -120 AND -84 AND latitude BETWEEN 10 AND 34
),
hit AS (
    SELECT ev.event_id, n.*
    FROM ev
    CROSS JOIN LATERAL (
        SELECT m.cve_ent, m.cve_mun, m.municipio, m.estado,
               ST_Distance(m.geom::geography, ev.pt::geography) / 1000.0 AS dist_km
        FROM analytics.dim_municipio m
        ORDER BY m.geom <-> ev.pt
        LIMIT 1
    ) n
)
UPDATE analytics.fct_earthquakes f
SET cve_ent         = CASE WHEN h.dist_km <= 150 THEN h.cve_ent   END,
    estado          = CASE WHEN h.dist_km <= 150 THEN h.estado    END,
    cve_mun         = CASE WHEN h.dist_km <= 150 THEN h.cve_mun   END,
    municipio       = CASE WHEN h.dist_km <= 150 THEN h.municipio END,
    dist_km_to_land = round(h.dist_km::numeric, 1),
    in_mexico_land  = (h.dist_km < 0.001)
FROM hit h
WHERE f.event_id = h.event_id;
