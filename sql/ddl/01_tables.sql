-- ============ STAGING ============
CREATE TABLE IF NOT EXISTS staging.earthquakes_clean (
    id          text PRIMARY KEY,
    event_time  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    latitude    double precision NOT NULL,
    longitude   double precision NOT NULL,
    depth_km    real,
    mag         numeric(3,1) NOT NULL,
    mag_type    text,
    place       text,
    status      text,
    alert       text,
    felt        integer,
    sig         smallint,
    tsunami     boolean NOT NULL
);
CREATE INDEX IF NOT EXISTS earthquakes_clean_updated_idx
    ON staging.earthquakes_clean (updated_at DESC);

CREATE TABLE IF NOT EXISTS staging.earthquakes_rejected (
    id          text PRIMARY KEY,
    updated_at  timestamptz,
    reason      text NOT NULL,
    payload     jsonb,
    rejected_at timestamptz NOT NULL DEFAULT now()
);

-- ============ ANALYTICS (lo que consume Grafana) ============
CREATE TABLE IF NOT EXISTS analytics.fct_earthquakes (
    event_id       text PRIMARY KEY,
    event_time     timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL,
    latitude       double precision NOT NULL,
    longitude      double precision NOT NULL,
    depth_km       real,
    magnitude      numeric(3,1) NOT NULL,
    mag_type       text,
    place          text,
    region         text,
    mag_class      text NOT NULL,
    depth_class    text,
    is_significant boolean NOT NULL,
    tsunami        boolean NOT NULL,
    felt           integer,
    alert          text,
    sig            smallint
);
CREATE INDEX IF NOT EXISTS fct_earthquakes_time_idx
    ON analytics.fct_earthquakes (event_time DESC);
CREATE INDEX IF NOT EXISTS fct_earthquakes_mag_idx
    ON analytics.fct_earthquakes (magnitude DESC);

CREATE TABLE IF NOT EXISTS analytics.agg_earthquakes_daily (
    day            timestamptz PRIMARY KEY,   -- medianoche UTC
    n_events       integer NOT NULL,
    n_m4_plus      integer NOT NULL,
    max_mag        numeric(3,1),
    avg_mag        numeric(4,2),
    avg_depth_km   numeric(7,2),
    n_significant  integer NOT NULL,
    n_tsunami      integer NOT NULL
);

-- ============ OPS (observabilidad del pipeline) ============
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id        text PRIMARY KEY,
    dag_id        text NOT NULL,
    window_start  timestamptz NOT NULL,
    rows_clean    integer,
    rows_rejected integer,
    rows_fact     integer,
    finished_at   timestamptz NOT NULL DEFAULT now()
);
