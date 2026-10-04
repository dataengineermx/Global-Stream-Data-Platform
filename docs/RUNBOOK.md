# Manual de operación – Global Data Platform / DataStreams-API

Pipeline horario de sismos (USGS) con capas `raw → staging → analytics`, enriquecimiento geográfico (INEGI MGN + PostGIS) y dashboard en Grafana.

**Cómo usar este manual:** si algo falla, ve primero a la sección 4 (chequeo de 5 minutos) y luego a la 5 (diagnóstico por síntoma). La sección 7 es un catálogo de errores que ya ocurrieron en este proyecto, con su solución.

---

## 0. Preparación (una vez por sesión de terminal)

Todos los comandos se ejecutan desde la raíz del proyecto (`DataStreams-API/`).

```bash
# Atajo para consultar la base de datos de datos (no la de metadatos de Airflow)
alias pgd='docker compose exec -T postgres-data psql -U datauser -d datadb'
```

Los alias no funcionan dentro de scripts; en ellos usa el comando completo.

> **Seguridad:** no pegues en chats ni tickets la salida de `docker compose config` ni de `env`; expanden el `.env` y muestran contraseñas en texto plano.

---

## 1. Arquitectura y flujo de datos

```
USGS API ──► extract_and_load ──► raw.earthquakes
                                      │
                         stage_clean_and_quarantine
                          ├─► staging.earthquakes_clean
                          └─► staging.earthquakes_rejected  (cuarentena, con motivo)
                                      │
                           build_fct_earthquakes ──► analytics.fct_earthquakes
                                      │
                                 enrich_geo  (usa analytics.dim_municipio, PostGIS)
                                      │
            ┌─────────────────────────┼─────────────────────────┐
     build_agg_daily          dq_no_future_events        dq_magnitude_in_range
  (analytics.agg_earthquakes_daily)   (SQLCheck)              (SQLCheck)
            └─────────────────────────┼─────────────────────────┘
                              log_run_metrics ──► ops.pipeline_runs
                                      │
                        Grafana (rol de solo lectura grafana_ro)
```

Principios de diseño que conviene recordar al depurar:

- **Idempotencia:** todas las cargas son `UPSERT`. Repetir una tarea, un run o un backfill no duplica datos.
- **Ventana incremental:** cada run procesa `updated >= data_interval_start - 1 hora` (solape intencional).
- **Cuarentena:** los registros que incumplen reglas no se borran; van a `staging.earthquakes_rejected` con su `reason`.
- **`log_run_metrics` solo corre si todo lo anterior pasó** (incluidos los chequeos de calidad). Un run sin fila en `ops.pipeline_runs` es un run que falló o no terminó.

---

## 2. Inventario de componentes

### 2.1 Servicios Docker Compose

| Servicio | Imagen | Puerto host → contenedor | Función |
|---|---|---|---|
| `postgres` | `postgres:16` | — (5432 interno) | Base de **metadatos de Airflow** (no contiene sismos) |
| `redis` | `redis:7.2-bookworm` | — (6379 interno) | Broker de Celery |
| `postgres-data` | `postgis/postgis:16-3.5` | `5433 → 5432` | Base de **datos** `datadb` (usuario `datauser`) |
| `airflow-init` | `apache/airflow:3.3.1` | — | Inicialización; **termina con `Exited`: es normal** |
| `airflow-apiserver` | `apache/airflow:3.3.1` | `8080 → 8080` | UI y API de Airflow |
| `airflow-scheduler` | `apache/airflow:3.3.1` | — | Programa los runs |
| `airflow-dag-processor` | `apache/airflow:3.3.1` | — | Lee y valida los archivos de `dags/` |
| `airflow-worker` | `apache/airflow:3.3.1` | — | Ejecuta las tareas |
| `grafana` | `grafana/grafana` | `3001 → 3000` | Dashboards |

Redes: `airflow-net` y `datastreams-api_default`. **Dos contenedores solo se ven por nombre si comparten al menos una red** (Grafana debe estar en la misma red que `postgres-data`).

### 2.2 Esquemas de la base `datadb`

| Esquema | Objetos clave | Quién escribe | Quién lee |
|---|---|---|---|
| `raw` | `earthquakes` (PK `id`) | `extract_and_load` | `stage_clean_and_quarantine` |
| `raw` | `mgn_estados`, `mgn_municipios` | `scripts/load_mgn.sh` | `04_build_dim_geo.sql` |
| `staging` | `earthquakes_clean`, `earthquakes_rejected` | `stage_clean_and_quarantine` | `build_fct_earthquakes`, vistas de `ops` |
| `analytics` | `fct_earthquakes`, `agg_earthquakes_daily`, `dim_municipio` | tareas del DAG / scripts | Grafana |
| `ops` | `pipeline_runs`, vista `v_rejected_by_reason` | `log_run_metrics` / DDL | Grafana |

### 2.3 Mapa de archivos

| Ruta | Contenido |
|---|---|
| `dags/earthquake_pipeline.py` | Definición del DAG |
| `src/load/earthquakes_loader.py` | `fetch_features` (API USGS) y `load_features` (COPY + upsert) |
| `sql/staging/earthquakes_clean.sql` | Validación, normalización y cuarentena |
| `sql/analytics/fct_earthquakes.sql` | Tabla de hechos y campos derivados |
| `sql/analytics/enrich_fct_geo.sql` | Estado/municipio/distancia a costa |
| `sql/analytics/agg_earthquakes_daily.sql` | Agregado diario |
| `sql/ops/log_run.sql` | Métricas por run |
| `sql/ddl/00…07_*.sql` | DDL y scripts de uso único (ver 8.7) |
| `scripts/load_mgn.sh` | Carga del Marco Geoestadístico |
| `scripts/backfill_geo.sh` | Enriquecimiento geográfico por lotes (reanudable) |
| `grafana/provisioning/…`, `grafana/dashboards/earthquakes.json` | Datasource y dashboard versionados |

### 2.4 Variables y credenciales (`.env`, nunca versionado)

| Variable | Uso | Valor correcto dentro de los contenedores |
|---|---|---|
| `APP_DB_HOST` / `APP_DB_PORT` | Loader de Python | `postgres-data` / `5432` (puerto **interno**, no el 5433) |
| `APP_DB_NAME` / `APP_DB_USER` / `APP_DB_PASSWORD` | Loader de Python | `datadb` / `datauser` / secreto |
| `AIRFLOW_CONN_POSTGRES_EARTHQUAKES` | Conexión `postgres_earthquakes` de las tareas SQL | URI de `postgres-data:5432` |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | Login de Grafana (solo se aplican al **crear** el volumen) | — |
| `GRAFANA_DB_PASSWORD` | Contraseña del rol `grafana_ro` | Debe coincidir con la del `ALTER ROLE` |

Desde el **host** (scripts `load_mgn.sh` y `backfill_geo.sh`) se usa `PGHOST=localhost PGPORT=5433`.

---

## 3. Dónde mirar (mapa de logs y estados)

| Qué quieres saber | Dónde mirar |
|---|---|
| Estado de los contenedores | `docker compose ps` |
| Log de un servicio | `docker compose logs --tail 100 <servicio>` |
| Estado de runs y tareas | UI de Airflow → DAG `earthquake_pipeline` → **Grid** |
| Log de una tarea (UI) | Clic en el cuadro de la tarea → pestaña **Logs** (uno por intento) |
| Log de una tarea (disco) | `logs/dag_id=earthquake_pipeline/run_id=<run>/task_id=<tarea>/attempt=<N>.log` |
| SQL ya renderizado de una tarea | UI → tarea → **Rendered Templates** |
| Valor devuelto por `extract_and_load` | UI → tarea → **XCom** |
| Errores al leer el DAG | `docker compose logs airflow-dag-processor` y `logs/dag_processor/<fecha>/` |
| Salud del pipeline en datos | Tabla `ops.pipeline_runs` (también panel en Grafana) |
| Calidad de datos | `ops.v_rejected_by_reason` y `staging.earthquakes_rejected` |

Los logs de tareas son líneas JSON. Para ver solo los errores:

```bash
# último run y último intento de una tarea
ls -t logs/dag_id=earthquake_pipeline | head -3
f="logs/dag_id=earthquake_pipeline/run_id=<RUN>/task_id=<TAREA>/attempt=<N>.log"
grep '"level":"error"' "$f" | head -5
```

Dentro de `error_detail` el campo `exc_type` / `exc_value` es la causa real; lo demás es traza.

---

## 4. Chequeo de salud en 5 minutos

```bash
# 1) Contenedores (solo airflow-init debe estar Exited)
docker compose ps

# 2) Airflow y Grafana responden
curl -s http://localhost:8080/api/v2/monitor/health
curl -s http://localhost:3001/api/health          # debe incluir "database": "ok"

# 3) Recursos del servidor
df -h . ; free -h ; docker system df

# 4) Frescura (¿el pipeline está corriendo?)
pgd -c "SELECT now() - max(finished_at) AS desde_ultimo_run FROM ops.pipeline_runs;" \
    -c "SELECT now() - max(updated)     AS retraso_en_raw   FROM raw.earthquakes;" \
    -c "SELECT now() - max(event_time)  AS ultimo_sismo     FROM analytics.fct_earthquakes;"
```

Referencia: con schedule `@hourly`, `desde_ultimo_run` debería ser **menor de ~2 horas**. Si pasa de 3 horas, hay un problema.

```bash
# 5) Consistencia entre capas
pgd -c "SELECT (SELECT count(*) FROM raw.earthquakes)               AS raw,
               (SELECT count(*) FROM staging.earthquakes_clean)      AS clean,
               (SELECT count(*) FROM staging.earthquakes_rejected)   AS rejected,
               (SELECT count(*) FROM analytics.fct_earthquakes)      AS fct;"
```

Invariantes (si no se cumplen, ver sección 9):

- `clean + rejected = raw`
- `fct = clean`

---

## 5. Diagnóstico por síntoma

### 5.1 El dashboard está vacío o desactualizado

1. **¿Hay datos recientes?** Ejecuta las consultas de frescura de la sección 4.
   - `retraso_en_raw` alto → el problema es la **extracción** (ve a 5.2, tarea `extract_and_load`).
   - `raw` fresco pero `fct` viejo → falla `stage_clean_and_quarantine` o `build_fct_earthquakes`.
2. **¿Los filtros del dashboard esconden todo?** Por defecto: **magnitud mínima 4** y **ámbito "cerca de México"**. Con un rango corto es normal ver pocos eventos. Prueba magnitud 0 y ámbito "todo el mundo".
3. **¿Grafana conecta?** *Connections → Data sources → Earthquakes → Save & test*. Si falla, ve a 5.4.
4. **¿Rango de tiempo?** El dashboard abre en los últimos 30 días.

### 5.2 Una tarea del DAG está en rojo

1. En el **Grid**, identifica la **primera** tarea en rojo (las siguientes en `upstream_failed` solo son consecuencia).
2. Abre su log (sección 3) y localiza `exc_type` / `exc_value`.
3. Busca el mensaje en la sección 7 o en la tabla de la sección 6.
4. Corrige la causa y reintenta: **Clear task** con *Downstream* marcado (así se reejecuta también lo posterior). Es seguro por la idempotencia.

Las tareas reintentan solas 3 veces con 2 minutos de espera; el estado definitivo en rojo significa que ya agotó los reintentos.

### 5.3 El DAG no aparece o aparece con error de importación

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail 50 airflow-dag-processor
docker compose exec airflow-scheduler python -c "import src.load.earthquakes_loader; print('import ok')"
```

Causas típicas: error de sintaxis en `dags/earthquake_pipeline.py`, `src/` sin montar o fuera del `PYTHONPATH`, DAG en pausa (toggle en la UI).

### 5.4 Grafana no conecta a la base de datos

Mira el mensaje exacto del *Save & test*; indica la capa del problema:

| Mensaje | Causa | Acción |
|---|---|---|
| `lookup postgres-data … server misbehaving` | Grafana y `postgres-data` **no comparten red** | Añadir `networks: [airflow-net]` al servicio `grafana` y `docker compose up -d grafana` |
| `password authentication failed for user "grafana_ro"` | Contraseña distinta entre `.env` y la base | Ver 8.6 |
| `permission denied for schema/table` | Faltan permisos de `grafana_ro` | Ver 7 (permisos) |
| `connection refused` | `postgres-data` caído o no saludable | `docker compose ps` / `logs postgres-data` |

Prueba de resolución de nombres: `docker compose exec grafana sh -c 'getent hosts postgres-data || echo NO resuelve'`.

### 5.5 No puedo entrar a Grafana

Las variables `GRAFANA_ADMIN_*` solo se aplican **la primera vez** que se crea el volumen `grafana-data`. Si cambiaste el `.env` después, no surte efecto. Ver 8.6 (reset de contraseña).

### 5.6 El mapa o un panel muestra "No data" pero los datos existen

- Revisa filtros y rango de tiempo (5.1).
- Abre el panel → **Inspect → Query** y ejecuta esa consulta con `pgd`.
- Si falla con `could not resize shared memory segment`, falta `shm_size` en `postgres-data` (7).
- Panel de calidad o de salud vacío → ¿existe la vista `ops.v_rejected_by_reason` (`sql/ddl/07_ops_views.sql`)? ¿Hay filas en `ops.pipeline_runs`?

---

## 6. Guía por tarea del DAG

| Tarea | Lee | Escribe | Fallas típicas | Cómo validar |
|---|---|---|---|---|
| `extract_and_load` | API USGS | `raw.earthquakes` | HTTP 400 por más de 20,000 eventos en una consulta; timeouts o 5xx de USGS (reintenta sola); `KeyError APP_DB_*` o `OperationalError` (variables o host); `ModuleNotFoundError: src`; `UndefinedTable raw.earthquakes` | XCom con nº de filas; `SELECT max(updated) FROM raw.earthquakes` |
| `stage_clean_and_quarantine` | `raw.earthquakes` (ventana) | `staging.earthquakes_clean`, `staging.earthquakes_rejected` | `PermissionError`/`TemplateNotFound` sobre `/opt/airflow/sql/...`; `relation does not exist` (DDL `01` sin ejecutar) | Conteos de la sección 4; `SELECT reason, count(*) FROM staging.earthquakes_rejected GROUP BY 1` |
| `build_fct_earthquakes` | `staging.earthquakes_clean` | `analytics.fct_earthquakes` | Tabla inexistente; columnas faltantes si no se aplicó el DDL | `fct = clean` |
| `enrich_geo` | `fct_earthquakes`, `dim_municipio` | columnas geográficas de `fct` | `function st_makepoint does not exist` (falta PostGIS); `column "cve_ent" does not exist` (falta `03_geo.sql`); **`dim_municipio` vacía: no da error, simplemente no actualiza nada** | `SELECT count(*) FROM analytics.dim_municipio` debe dar **2478**; consulta de pendientes (sección 9) |
| `build_agg_daily` | `fct_earthquakes` | `agg_earthquakes_daily` | Rara vez falla | Que existan filas para los días recientes |
| `dq_no_future_events` | `fct_earthquakes` | — | Falla si hay eventos con `event_time` futuro (> now + 5 min). Posible desfase de reloj del contenedor | `SELECT event_id, event_time FROM analytics.fct_earthquakes WHERE event_time > now() + interval '5 minutes';` |
| `dq_magnitude_in_range` | `fct_earthquakes` | — | Falla si hay magnitud fuera de -2 a 10 | `SELECT event_id, magnitude FROM analytics.fct_earthquakes WHERE magnitude NOT BETWEEN -2 AND 10;` |
| `log_run_metrics` | capas | `ops.pipeline_runs` | Solo corre si todo lo anterior pasó | `SELECT * FROM ops.pipeline_runs ORDER BY finished_at DESC LIMIT 3;` |

> **Nota de diseño:** los chequeos `dq_*` corren **después** de construir `fct_earthquakes`. Detectan, pero no impiden que un dato malo ya esté en la tabla (y por tanto en Grafana) hasta que se corrija.

---

## 7. Catálogo de errores conocidos

| Mensaje / síntoma | Causa | Solución |
|---|---|---|
| `PermissionError: [Errno 13] … /opt/airflow/sql/…` | El usuario del contenedor no puede leer los archivos montados | `chmod -R a+rX sql` (en el host). No requiere reiniciar |
| `TemplateNotFound: staging/earthquakes_clean.sql` | Archivo en carpeta equivocada o `./sql` sin montar | Revisar estructura (`sql/staging`, `sql/analytics`, `sql/ops`) y el volumen `./sql:/opt/airflow/sql`; `docker compose up -d` |
| `database "datadb" has a collation version mismatch` (2.41 vs 2.31) | Cambio de imagen entre bases Debian con distinta glibc | `REINDEX DATABASE datadb;` y `ALTER DATABASE datadb REFRESH COLLATION VERSION;` (ver 8.8). Evitar mezclar imágenes Debian/Alpine |
| `extension "postgis" is not available` / `postgis.control: No such file` | La imagen de Postgres no trae PostGIS | Usar `postgis/postgis:16-3.5` |
| `function postgis_version() does not exist` | Imagen correcta, extensión sin crear | Ejecutar `sql/ddl/03_geo.sql` |
| `could not resize shared memory segment … No space left on device` | `/dev/shm` de Docker (64 MB) insuficiente para consultas o `VACUUM` paralelos | `shm_size: 1gb` en `postgres-data` y `docker compose up -d postgres-data` |
| `failed to bind host port 0.0.0.0:3000 … address already in use` | Puerto ocupado en el servidor | Mapear otro puerto (`3001:3000`); ver quién lo usa con `sudo ss -ltnp \| grep :3000` |
| Grafana: `no such file or directory …/provisioning/dashboards` | Falta `grafana/provisioning/dashboards/dashboards.yaml` | Estructura exacta en 2.3; `docker compose restart grafana` |
| Grafana: `lookup postgres-data … server misbehaving` | Sin red en común con `postgres-data` | Ver 5.4 |
| No puedo hacer login en Grafana con el `.env` | Credenciales solo se aplican al crear el volumen | Ver 8.6 |
| `bash: pass: No such file or directory` al lanzar un script | Se escribió el marcador `<pass>` literal (bash lo lee como redirección) | Usar `read -s -p "Password: " PGPASSWORD; export PGPASSWORD` |
| `ogr2ogr`: "Several coordinate operations have been used…" | Advertencia de PROJ ITRF2008→WGS84 | Inofensiva (diferencia de 1-2 m). Se silencia con `-ct_opt WARN_ABOUT_DIFFERENT_COORD_OP=NO` |
| Acentos rotos (`MichoacÃ¡n`) tras cargar el MGN | Codificación del DBF (`.cpg` = ISO-8859-1) | `load_mgn.sh` fuerza `SHAPE_ENCODING=ISO-8859-1`; recargar |
| `permission denied for schema/table` desde Grafana | `grafana_ro` sin `GRANT` | `GRANT USAGE ON SCHEMA analytics, ops TO grafana_ro; GRANT SELECT ON ALL TABLES IN SCHEMA analytics, ops TO grafana_ro;` |
| `airflow-init` aparece `Exited` | Es lo esperado: solo inicializa | Ignorar |

---

## 8. Procedimientos operativos

### 8.1 Levantar, parar y reiniciar

```bash
docker compose up -d                  # levantar todo (recrea lo que cambió en la configuración)
docker compose up -d grafana          # aplicar cambios de un servicio (redes, volúmenes, variables)
docker compose restart <servicio>     # solo reinicia; NO aplica cambios de compose
docker compose down                   # parar y quitar contenedores (los volúmenes se conservan)
```

`down -v` **borra los volúmenes (incluidos los datos)**. No usarlo sin respaldo.

### 8.2 Reintentar una tarea o un run

UI de Airflow → clic en la tarea → **Clear task** con *Downstream* marcado. Es seguro: todo es idempotente.

Desde la línea de comandos (verifica la sintaxis con `--help` en tu versión, 3.3.1):

```bash
docker compose exec airflow-scheduler airflow dags trigger earthquake_pipeline
docker compose exec airflow-scheduler airflow dags test earthquake_pipeline   # ejecución local de prueba
```

### 8.3 Recuperar un hueco (Airflow o el servidor estuvieron caídos)

El DAG tiene `catchup=False`: no ejecuta las horas perdidas por sí solo.

- **Hueco corto (pocas horas):** lanza un backfill del intervalo perdido desde la UI de Airflow (Trigger → Backfill) o con `airflow backfill create --help` para ver los parámetros. Un solo run desde el inicio del hueco basta: pide a USGS todo lo actualizado desde entonces.
- **Hueco largo:** USGS limita cada consulta a **20,000 eventos**; si se excede, la extracción falla con HTTP 400. Usa `src/load/backfill_earthquakes.py` por tramos de fechas (es idempotente) y después reprocesa las capas con `sql/ddl/06_backfill_layers.sql` (ver 8.4).

### 8.4 Reprocesar todas las capas (después de un backfill de `raw`)

El pipeline horario solo procesa la ventana reciente. Tras cargar datos históricos en `raw`:

```bash
# 1) raw -> staging -> fct -> agg (tarda minutos con millones de filas; revisa df -h antes)
pgd < sql/ddl/06_backfill_layers.sql

# 2) Geografía por lotes, reanudable (Ctrl+C y volver a lanzar continúa donde se quedó)
read -s -p "Password de datauser: " PGPASSWORD; export PGPASSWORD; echo
PGHOST=localhost PGPORT=5433 PGDATABASE=datadb PGUSER=datauser ./scripts/backfill_geo.sh 5000

# 3) Mantenimiento
pgd -c "VACUUM ANALYZE;"
```

Referencia de rendimiento medida en este proyecto: unos 700 sismos/segundo en el enriquecimiento geográfico (lotes de 5,000 en 5-10 s).

### 8.5 Recargar el Marco Geoestadístico (MGN)

```bash
read -s -p "Password de datauser: " PGPASSWORD; export PGPASSWORD; echo
PGHOST=localhost PGPORT=5433 PGDATABASE=datadb PGUSER=datauser \
MGN_DIR=~/mgn_work/conjunto_de_datos ./scripts/load_mgn.sh
```

Debe terminar con `municipios=2478 estados=32`. Comprobación con un punto conocido (Zócalo, CDMX):

```bash
pgd -c "SELECT municipio, estado FROM analytics.dim_municipio WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(-99.1332, 19.4326), 4326));"
```

Debe devolver `Cuauhtémoc | Ciudad de México`. Después, `scripts/backfill_geo.sh` completa los sismos que hayan quedado sin geografía.

### 8.6 Credenciales de Grafana

**Cambiar la contraseña del rol de base de datos (`grafana_ro`):**

```bash
pgd -c "ALTER ROLE grafana_ro PASSWORD '<nueva>'"
# actualizar GRAFANA_DB_PASSWORD en .env con el mismo valor y recrear el contenedor:
docker compose up -d grafana
```

**Restablecer la contraseña de administrador de Grafana** (sin perder nada):

```bash
read -s -p "Nueva contraseña de admin: " NP; echo
docker compose exec grafana grafana cli admin reset-admin-password "$NP"; unset NP
```

Alternativa: borrar el volumen `datastreams-api_grafana-data`. No se pierde nada, porque datasource y dashboard se aprovisionan desde archivos:

```bash
docker compose rm -sf grafana && docker volume rm datastreams-api_grafana-data && docker compose up -d grafana
```

### 8.7 Scripts de `sql/ddl/` (uso único, no los ejecuta Airflow)

| Archivo | Cuándo |
|---|---|
| `00_schemas.sql`, `01_tables.sql` | Instalación inicial |
| `02_grafana_role.sql` | Crear el rol de solo lectura (cambiar antes la contraseña `CAMBIAME`) |
| `03_geo.sql` | Crear extensión PostGIS, `dim_municipio` y columnas geográficas |
| `04_build_dim_geo.sql` | Lo ejecuta `load_mgn.sh` (reconstruye `dim_municipio`) |
| `05_backfill_geo.sql` | Un lote del enriquecimiento; lo repite `backfill_geo.sh` |
| `06_backfill_layers.sql` | Reprocesar todo `raw` hacia las demás capas |
| `07_ops_views.sql` | Vista de calidad que Grafana consume |

### 8.8 Cambiar de imagen de Postgres

1. Respaldo previo (8.9).
2. Mantener la misma versión mayor y la misma familia (Debian con Debian).
3. Tras el cambio, si aparece el aviso de collation: `REINDEX DATABASE datadb;` y `ALTER DATABASE datadb REFRESH COLLATION VERSION;`. Si otras bases dan el mismo aviso (p. ej. la de Airflow), repetir con su nombre (`\l` las lista).

### 8.9 Respaldo y restauración

```bash
# Respaldo de datos (formato comprimido)
docker compose exec -T postgres-data pg_dump -U datauser -Fc datadb > backup_$(date +%F).dump

# Restauración: preferible sobre una base vacía (con la imagen PostGIS ya en uso)
docker compose exec -T postgres-data pg_restore -U datauser -d datadb --no-owner < backup_<fecha>.dump
```

La base de metadatos de Airflow (`postgres`) es independiente: perderla borra el historial de runs, no los sismos. Respáldala aparte si importa conservar el historial.

### 8.10 Mantenimiento periódico

| Frecuencia | Tarea |
|---|---|
| Tras cargas masivas | `VACUUM ANALYZE;` |
| Semanal (recomendado) | Revisar `df -h` y el tamaño de `logs/`; los logs de tareas crecen continuamente |
| Mensual | Revisar `staging.earthquakes_rejected` por motivo (cambios bruscos indican cambios en la fuente) |
| Antes de cualquier cambio de imagen | Respaldo (8.9) |

---

## 9. Calidad de datos: verificaciones

```bash
# Rechazos por motivo (esperado: mag_null y not_earthquake concentran casi todo)
pgd -c "SELECT reason, count(*) FROM staging.earthquakes_rejected GROUP BY 1 ORDER BY 2 DESC;"

# Filas de raw que no están ni en clean ni en rejected (deben ser 0 si staging está al día)
pgd -c "SELECT count(*) AS sin_procesar FROM raw.earthquakes e
        WHERE NOT EXISTS (SELECT 1 FROM staging.earthquakes_clean c    WHERE c.id = e.id)
          AND NOT EXISTS (SELECT 1 FROM staging.earthquakes_rejected r WHERE r.id = e.id);"

# Sismos de la caja de México sin geografía calculada (debe ser 0)
pgd -c "SELECT count(*) AS sin_geo FROM analytics.fct_earthquakes
        WHERE dist_km_to_land IS NULL AND longitude BETWEEN -120 AND -84 AND latitude BETWEEN 10 AND 34;"
```

Cómo actuar:

| Resultado | Acción |
|---|---|
| `sin_procesar > 0` | Reprocesar capas (8.4, paso 1) |
| `sin_geo > 0` | Verificar `dim_municipio` (2478 filas) y ejecutar `scripts/backfill_geo.sh` |
| Cambio brusco en los motivos de rechazo | Revisar si USGS cambió el formato; inspeccionar `payload` en `staging.earthquakes_rejected` |

Notas de interpretación (evitan falsas alarmas):

- **Sesgo de cobertura:** Baja California y Sonora concentran muchos sismos de magnitud 2 por la red de sensores vecina (California/Arizona), no por mayor sismicidad. Compara estados con magnitud mínima ≥ 4.
- **`in_mexico_land`** indica que el sismo cae dentro de un municipio. En el mar, `estado`/`municipio` significan el **más cercano** y solo se asignan a ≤ 150 km.
- Los umbrales de validación (magnitud -2 a 10, profundidad -5 a 800 km) viven en `sql/staging/earthquakes_clean.sql`.

---

## 10. Límites y riesgos conocidos

- **Detección, no prevención:** los chequeos `dq_*` corren después de poblar `fct_earthquakes`.
- **Fallo silencioso de `enrich_geo`:** con `dim_municipio` vacía no hay error, solo no se actualizan filas (detectable con la consulta `sin_geo`).
- **Sin catch-up automático:** una caída larga requiere recuperación manual (8.3).
- **Límite de la API de USGS:** 20,000 eventos por consulta.
- **Grafana con `latest`:** conviene fijar una versión concreta.
- **Ventana incremental de 1 hora de solape:** cada run solo ve los cambios recientes. Si un run falla y no se reintenta a tiempo, o hay un hueco, hay que recuperarlo manualmente (8.3).

---

## 11. Antes de pedir ayuda, reúne esto

```bash
docker compose ps
docker compose logs --tail 60 <servicio_con_problema>
# log de la tarea fallida (ruta de la sección 3) y su exc_type / exc_value
pgd -c "SELECT * FROM ops.pipeline_runs ORDER BY finished_at DESC LIMIT 3;"
```

Incluye: qué comando ejecutaste, el mensaje completo, y qué cambió justo antes (imagen, `.env`, archivos movidos). **Oculta contraseñas** antes de compartir.
