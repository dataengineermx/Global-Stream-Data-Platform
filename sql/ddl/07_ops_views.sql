-- Vista para el panel de calidad: grafana_ro NO tiene acceso a staging, pero sí a esta vista
-- (las vistas se ejecutan con los permisos de su dueño).
CREATE OR REPLACE VIEW ops.v_rejected_by_reason AS
SELECT reason, count(*)::bigint AS n
FROM staging.earthquakes_rejected
GROUP BY reason;

GRANT SELECT ON ops.v_rejected_by_reason TO grafana_ro;
