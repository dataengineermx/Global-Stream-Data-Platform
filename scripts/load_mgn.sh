#!/usr/bin/env bash
# Carga el Marco Geoestadístico (INEGI) a raw.mgn_estados y raw.mgn_municipios.
# Uso: PGHOST=localhost PGPORT=5433 PGDATABASE=... PGUSER=... PGPASSWORD=... \
#      MGN_DIR=~/mgn_work/conjunto_de_datos ./scripts/load_mgn.sh
set -euo pipefail

: "${PGHOST:?}" "${PGPORT:?}" "${PGDATABASE:?}" "${PGUSER:?}" "${PGPASSWORD:?}"
MGN_DIR="${MGN_DIR:-$HOME/mgn_work/conjunto_de_datos}"
PG="PG:host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER"

load() {  # $1 = shapefile, $2 = tabla destino
  # Origen: EPSG:6372 (Lambert ITRF2008) -> EPSG:4326. Los .cpg dicen ISO-8859-1.
  ogr2ogr -f PostgreSQL "$PG" "$MGN_DIR/$1" \
    --config SHAPE_ENCODING ISO-8859-1 \
    -nln "$2" -lco SCHEMA=raw -lco GEOMETRY_NAME=geom -lco FID=ogc_fid \
    -lco SPATIAL_INDEX=GIST -t_srs EPSG:4326 -nlt PROMOTE_TO_MULTI -overwrite
}

load 00ent.shp mgn_estados
load 00mun.shp mgn_municipios

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 \
  -f "$(dirname "$0")/../sql/ddl/04_build_dim_geo.sql"

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -At -c \
  "SELECT 'municipios=' || count(*) || ' estados=' || count(DISTINCT estado) FROM analytics.dim_municipio"
