-- Un LOTE del backfill geográfico: enriquece hasta :batch_size sismos de la caja de México
-- que aún no tienen distancia calculada (dist_km_to_land IS NULL). Reanudable e idempotente.
-- Imprime cuántas filas actualizó; scripts/backfill_geo.sh lo repite hasta llegar a 0.
WITH ev AS (
    SELECT event_id, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS pt
    FROM analytics.fct_earthquakes
    WHERE dist_km_to_land IS NULL
      AND longitude BETWEEN -120 AND -84 AND latitude BETWEEN 10 AND 34
    LIMIT :batch_size
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
),
upd AS (
    UPDATE analytics.fct_earthquakes f
    SET cve_ent         = CASE WHEN h.dist_km <= 150 THEN h.cve_ent   END,
        estado          = CASE WHEN h.dist_km <= 150 THEN h.estado    END,
        cve_mun         = CASE WHEN h.dist_km <= 150 THEN h.cve_mun   END,
        municipio       = CASE WHEN h.dist_km <= 150 THEN h.municipio END,
        dist_km_to_land = round(h.dist_km::numeric, 1),
        in_mexico_land  = (h.dist_km < 0.001)
    FROM hit h
    WHERE f.event_id = h.event_id
    RETURNING 1
)
SELECT count(*) FROM upd;
