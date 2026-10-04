-- Cambia la contraseña (o créala desde una variable de entorno).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro') THEN
    CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana';
  END IF;
END $$;

GRANT USAGE ON SCHEMA analytics, ops TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics, ops TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops       GRANT SELECT ON TABLES TO grafana_ro;
