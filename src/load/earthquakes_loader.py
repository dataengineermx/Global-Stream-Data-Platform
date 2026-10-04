"""Carga eventos del GeoJSON de USGS en Postgres: COPY a tabla temporal + upsert por id."""
import logging
import os
from datetime import datetime, timezone

import psycopg
import requests
import psycopg.conninfo


logger = logging.getLogger(__name__)

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Orden de columnas = orden de los valores que devuelve feature_to_row()
COLUMNS = [
    "time", "updated", "longitude", "latitude",
    "mag", "depth_km", "felt", "cdi", "mmi", "dmin", "rms", "gap",
    "sig", "nst", "tsunami",
    "status", "alert", "mag_type", "event_type", "place", "id",
]


def fetch_features(starttime=None, endtime=None, updatedafter=None) -> list[dict]:
    """Descarga eventos. El API limita a 20,000 eventos por consulta: para históricos, pide por tramos."""
    params = {"format": "geojson"}
    if starttime:
        params["starttime"] = starttime
    if endtime:
        params["endtime"] = endtime
    if updatedafter:  # carga incremental: solo eventos creados o revisados desde esa fecha
        params["updatedafter"] = updatedafter
    resp = requests.get(API_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()["features"]


def _ts(ms):
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _norm(value):
    """Minúsculas y None si viene vacío (la API mezcla 'reviewed' y 'REVIEWED', 'ml' y 'Md')."""
    return value.lower() if value else None


def feature_to_row(f: dict) -> tuple:
    p = f["properties"]
    lon, lat, depth = f["geometry"]["coordinates"][:3]
    return (
        _ts(p["time"]), _ts(p["updated"]), lon, lat,
        p.get("mag"), depth, p.get("felt"), p.get("cdi"), p.get("mmi"),
        p.get("dmin"), p.get("rms"), p.get("gap"),
        p.get("sig"), p.get("nst"), bool(p.get("tsunami")),
        _norm(p.get("status")), p.get("alert"), _norm(p.get("magType")),
        p.get("type"), p.get("place"), f["id"],
    )


def get_conninfo() -> str:
    return psycopg.conninfo.make_conninfo(
        host=os.environ["APP_DB_HOST"],
        port=os.environ["APP_DB_PORT"],
        dbname=os.environ["APP_DB_NAME"],
        user=os.environ["APP_DB_USER"],
        password=os.environ["APP_DB_PASSWORD"],
    )


def load_features(features: list[dict], conninfo: str | None = None) -> int:
    """Inserta eventos nuevos y actualiza los revisados. Devuelve filas insertadas + actualizadas."""
    rows = [feature_to_row(f) for f in features]
    if not rows:
        logger.info("Sin eventos que cargar")
        return 0

    cols = ", ".join(COLUMNS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "id")

    with psycopg.connect(conninfo or get_conninfo()) as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE stg (LIKE raw.earthquakes INCLUDING DEFAULTS) ON COMMIT DROP")
        with cur.copy(f"COPY stg ({cols}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
        # Solo sobrescribe si la versión nueva es más reciente que la guardada
        cur.execute(
            f"""
            INSERT INTO raw.earthquakes AS e ({cols})
            SELECT {cols} FROM stg
            ON CONFLICT (id) DO UPDATE SET {updates}
            WHERE e.updated < EXCLUDED.updated
            """
        )
        affected = cur.rowcount
    logger.info("Eventos recibidos: %s | insertados/actualizados: %s", len(rows), affected)
    return affected
