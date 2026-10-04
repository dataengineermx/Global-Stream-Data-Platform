-- Requiere imagen con PostGIS (postgis/postgis:16-3.5) y rol con permiso para crear extensiones.
CREATE EXTENSION IF NOT EXISTS postgis;

-- Dimensión de municipios (se reconstruye con 04_build_dim_geo.sql tras cargar el MGN)
CREATE TABLE IF NOT EXISTS analytics.dim_municipio (
    cvegeo    text PRIMARY KEY,
    cve_ent   text NOT NULL,
    cve_mun   text NOT NULL,
    municipio text NOT NULL,
    estado    text NOT NULL,
    geom      geometry(MultiPolygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS dim_municipio_geom_gix ON analytics.dim_municipio USING gist (geom);

-- Contexto geográfico en la tabla final
ALTER TABLE analytics.fct_earthquakes
    ADD COLUMN IF NOT EXISTS cve_ent         text,
    ADD COLUMN IF NOT EXISTS estado          text,
    ADD COLUMN IF NOT EXISTS cve_mun         text,
    ADD COLUMN IF NOT EXISTS municipio       text,
    ADD COLUMN IF NOT EXISTS dist_km_to_land numeric(7,1),
    ADD COLUMN IF NOT EXISTS in_mexico_land  boolean NOT NULL DEFAULT false;
