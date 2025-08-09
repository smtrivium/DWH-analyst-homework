SELECT 
    airport_code,
    airport_name,
    total_flights,
    RANK() OVER (ORDER BY total_flights DESC) AS rank
FROM airport_stats;