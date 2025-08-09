SELECT 
    flight_no,
    scheduled_departure,
    estimated_crew_cost,
    flight_duration_hours,
    crew_count
FROM flight_analytics
ORDER BY estimated_crew_cost DESC
LIMIT 10;