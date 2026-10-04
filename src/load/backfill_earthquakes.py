"""Carga histórica de sismos de USGS por tramos de tiempo.

Uso (dentro del contenedor de Airflow, con las variables APP_DB_* del .env):
    python -m src.load.backfill_earthquakes --start 2014-01-01 --end 2015-01-01 --dry-run
    python -m src.load.backfill_earthquakes --start 2014-01-01 --end 2015-01-01

Es idempotente: se puede repetir o reanudar sin duplicar datos (upsert por id).
"""
import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

from src.load import earthquakes_loader as el

logger = logging.getLogger("backfill")

API_URL = os.getenv("USGS_API_URL", el.API_URL)
COUNT_URL = API_URL.rsplit("/", 1)[0] + "/count"
TIME_FMT = "%Y-%m-%dT%H:%M:%S"  # USGS interpreta las fechas sin zona horaria como UTC
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 2


def _get(url: str, params: dict) -> dict:
    """GET con reintentos ante errores de red, 429 y 5xx. Un 400 u otro error de cliente se propaga."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = status is None or status == 429 or status >= 500
            if not retryable or attempt == MAX_ATTEMPTS:
                raise
            wait = BACKOFF_SECONDS * 2 ** (attempt - 1)
            logger.warning("Intento %s/%s falló (%s); reintento en %ss", attempt, MAX_ATTEMPTS, exc, wait)
            time.sleep(wait)


def _params(a: datetime, b: datetime, args) -> dict:
    params = {"format": "geojson", "starttime": a.strftime(TIME_FMT), "endtime": b.strftime(TIME_FMT)}
    if args.minmag is not None:
        params["minmagnitude"] = args.minmag
    if args.eventtype:
        params["eventtype"] = args.eventtype
    return params


def process_window(a: datetime, b: datetime, args, stats: dict) -> None:
    """Cuenta los eventos del tramo; si superan el límite del API lo divide a la mitad, si no, lo carga."""
    params = _params(a, b, args)
    info = _get(COUNT_URL, params)
    n, limit = info["count"], info["maxAllowed"]
    if n == 0:
        return

    if n > limit:
        if b - a <= timedelta(seconds=1):
            raise RuntimeError(f"{n} eventos en un tramo de 1 s ({a}); no se puede dividir más")
        mid = (a + (b - a) / 2).replace(microsecond=0)
        logger.info("%s eventos en %s -> %s superan el límite (%s): divido el tramo", n, a, b, limit)
        process_window(a, mid, args, stats)
        process_window(mid, b, args, stats)
        return

    if args.dry_run:
        stats["events"] += n
        logger.info("[dry-run] %s -> %s: %s eventos", a.date(), b.date(), n)
        return

    features = _get(API_URL, params)["features"]
    written = el.load_features(features)
    stats["events"] += len(features)
    stats["written"] += written
    logger.info("%s -> %s: recibidos %s, insertados/actualizados %s", a.date(), b.date(), len(features), written)
    time.sleep(args.sleep)


def _parse_day(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Carga histórica de sismos de USGS en Postgres")
    parser.add_argument("--start", required=True, type=_parse_day, help="Fecha inicial UTC (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_day, help="Fecha final UTC (YYYY-MM-DD). Por defecto: hoy")
    parser.add_argument("--step-days", type=int, default=30, help="Tamaño inicial de cada tramo (días)")
    parser.add_argument("--minmag", type=float, help="Magnitud mínima (reduce mucho el volumen)")
    parser.add_argument("--eventtype", help="Ej. earthquake, para excluir explosiones, canteras, etc.")
    parser.add_argument("--sleep", type=float, default=0.5, help="Pausa entre peticiones (segundos)")
    parser.add_argument("--dry-run", action="store_true", help="Solo cuenta eventos, no descarga ni carga")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    end = args.end or datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    stats = {"events": 0, "written": 0}

    a = args.start
    while a < end:
        b = min(a + timedelta(days=args.step_days), end)
        process_window(a, b, args, stats)
        a = b

    if not args.dry_run:
        with psycopg.connect(el.get_conninfo(), autocommit=True) as conn:
            conn.execute("ANALYZE earthquakes")
    logger.info("Fin. Eventos %s: %s | filas insertadas/actualizadas: %s",
                "contados" if args.dry_run else "recibidos", stats["events"], stats["written"])


if __name__ == "__main__":
    main()
