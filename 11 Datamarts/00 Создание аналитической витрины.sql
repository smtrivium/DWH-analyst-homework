CREATE MATERIALIZED VIEW flight_analytics AS
WITH current_employees AS (
    -- Актуальные данные сотрудников
    SELECT 
        employee_id,
        employee_name,
        birth_dt,
        position,
        salary
    FROM employee
    WHERE valid_to_dt = '5999-01-01'
),
flight_details AS (
    -- Агрегированные данные по рейсам
    SELECT 
        f.flight_no,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.departure_airport AS departure_code,
        dep.airport_name AS departure_airport,
        dep.country AS departure_country,
        f.arrival_airport AS arrival_code,
        arr.airport_name AS arrival_airport,
        arr.country AS arrival_country,
        f.status,
        COUNT(DISTINCT f.employee_id) AS crew_count,
        SUM(CASE WHEN e.position = 'Pilot' THEN 1 ELSE 0 END) AS pilot_count,
        SUM(CASE WHEN e.position = 'SFA' THEN 1 ELSE 0 END) AS sfa_count,
        SUM(CASE WHEN e.position = 'Steward' THEN 1 ELSE 0 END) AS steward_count,
        AVG(e.salary) AS avg_crew_salary,
        EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure))/3600 AS flight_duration_hours
    FROM flights f
    JOIN airports dep ON f.departure_airport = dep.airport_code
    JOIN airports arr ON f.arrival_airport = arr.airport_code
    JOIN current_employees e ON f.employee_id = e.employee_id
    GROUP BY 
        f.flight_no,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.departure_airport,
        dep.airport_name,
        dep.country,
        f.arrival_airport,
        arr.airport_name,
        arr.country,
        f.status
)
SELECT 
    fd.flight_no,
    fd.scheduled_departure,
    fd.scheduled_arrival,
    fd.departure_code,
    fd.departure_airport,
    fd.departure_country,
    fd.arrival_code,
    fd.arrival_airport,
    fd.arrival_country,
    fd.status,
    fd.crew_count,
    fd.pilot_count,
    fd.sfa_count,
    fd.steward_count,
    fd.avg_crew_salary,
    fd.flight_duration_hours,
    -- Расчет стоимости экипажа (160 рабочих часов в месяц)
    ROUND((fd.avg_crew_salary * fd.crew_count * fd.flight_duration_hours / 160)::numeric, 2) AS estimated_crew_cost,
    -- Категории на русском языке
    CASE 
        WHEN fd.flight_duration_hours < 2 THEN 'Короткие рейсы'
        WHEN fd.flight_duration_hours BETWEEN 2 AND 6 THEN 'Средние рейсы'
        ELSE 'Длинные рейсы'
    END AS flight_category,
    CASE 
        WHEN fd.crew_count < 3 THEN 'Маленький экипаж'
        WHEN fd.crew_count BETWEEN 3 AND 5 THEN 'Средний экипаж'
        ELSE 'Большой экипаж'
    END AS crew_category
FROM flight_details fd;