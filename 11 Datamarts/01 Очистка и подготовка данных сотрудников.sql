-- Трансформ для очистки данных сотрудников
WITH employee_clean AS (
    SELECT 
        employee_id,
        TRIM(employee_name) AS employee_name,
        birth_dt,
        CASE 
            WHEN position NOT IN ('Pilot', 'SFA', 'Steward') THEN 'Other'
            ELSE position
        END AS position,
        CASE 
            WHEN salary < 0 THEN 0
            WHEN salary > 10000 THEN 10000
            ELSE salary
        END AS salary,
        valid_from_dt,
        valid_to_dt
    FROM employee
)
INSERT INTO employee_clean
SELECT * FROM employee_clean;