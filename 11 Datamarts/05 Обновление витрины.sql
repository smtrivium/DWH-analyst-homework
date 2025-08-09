-- Процедура для обновления витрины
CREATE OR REPLACE PROCEDURE refresh_analytics()
LANGUAGE SQL
AS $$
    REFRESH MATERIALIZED VIEW flight_analytics;
    REFRESH MATERIALIZED VIEW airport_stats;
    REFRESH MATERIALIZED VIEW salary_analytics;
    REFRESH MATERIALIZED VIEW time_analytics;
$$;