#!/usr/bin/env bash
# Enriquece el histórico de analytics.fct_earthquakes por lotes (cada lote hace commit).
# Se puede interrumpir con Ctrl+C y volver a ejecutar: continúa donde se quedó.
# Uso: PGHOST=... PGPORT=... PGDATABASE=... PGUSER=... PGPASSWORD=... ./scripts/backfill_geo.sh [tamaño_lote]
set -euo pipefail
: "${PGHOST:?}" "${PGPORT:?}" "${PGDATABASE:?}" "${PGUSER:?}" "${PGPASSWORD:?}"
BATCH="${1:-20000}"
SQL="$(dirname "$0")/../sql/ddl/05_backfill_geo.sql"

total=0
while true; do
  start=$(date +%s)
  n=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 \
        -v batch_size="$BATCH" -At -q -f "$SQL")
  total=$((total + n))
  echo "lote: $n filas en $(( $(date +%s) - start ))s | acumulado: $total"
  [ "$n" -eq 0 ] && break
done
echo "Listo. Pendientes en la caja de México: 0"
