-- Трансформ для расчета зарплатной статистики
CREATE MATERIALIZED VIEW salary_analytics AS
WITH current_salaries AS (
    SELECT 
        position,
        salary,
        employee_id
    FROM employee
    WHERE valid_to_dt = '5999-01-01'
)
SELECT 
    position,
    COUNT(employee_id) AS employee_count,
    AVG(salary) AS avg_salary,
    MIN(salary) AS min_salary,
    MAX(salary) AS max_salary,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary,
    SUM(salary) AS total_salary_expenses
FROM current_salaries
GROUP BY position;