-- Ejecutar después de scripts/load_mgn.sh (que carga raw.mgn_estados y raw.mgn_municipios).
BEGIN;
TRUNCATE analytics.dim_municipio;

INSERT INTO analytics.dim_municipio (cvegeo, cve_ent, cve_mun, municipio, estado, geom)
SELECT
    m.cvegeo, m.cve_ent, m.cve_mun, m.nomgeo, e.nomgeo,
    ST_Multi(ST_CollectionExtract(ST_MakeValid(m.geom), 3))::geometry(MultiPolygon, 4326)
FROM raw.mgn_municipios m
JOIN raw.mgn_estados    e USING (cve_ent);

COMMIT;
ANALYZE analytics.dim_municipio;
