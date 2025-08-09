-- Трансформ для временного анализа рейсов
CREATE MATERIALIZED VIEW time_analytics AS
SELECT 
    DATE_TRUNC('day', scheduled_departure) AS flight_date,
    EXTRACT(HOUR FROM scheduled_departure) AS departure_hour,
    COUNT(DISTINCT flight_no) AS flight_count,
    COUNT(DISTINCT employee_id) AS crew_members,
    AVG(EXTRACT(EPOCH FROM (scheduled_arrival - scheduled_departure))/60) AS avg_duration_minutes,
    status,
    departure_airport
FROM flights
GROUP BY 
    DATE_TRUNC('day', scheduled_departure),
    EXTRACT(HOUR FROM scheduled_departure),
    status,
    departure_airport;