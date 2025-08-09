-- Трансформ для агрегации данных по аэропортам
CREATE MATERIALIZED VIEW airport_stats AS
SELECT 
    a.airport_code,
    a.airport_name,
    a.country,
    COUNT(DISTINCT f.flight_no) AS total_flights,
    COUNT(DISTINCT CASE WHEN f.status = 'Scheduled' THEN f.flight_no END) AS scheduled_flights,
    COUNT(DISTINCT CASE WHEN f.status = 'Completed' THEN f.flight_no END) AS completed_flights,
    COUNT(DISTINCT f.employee_id) AS unique_crew_members,
    AVG(EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure))/3600) AS avg_flight_duration_hours
FROM airports a
LEFT JOIN flights f ON a.airport_code IN (f.departure_airport, f.arrival_airport)
GROUP BY a.airport_code, a.airport_name, a.country;