WITH first_orders AS (
    SELECT 
        user_id,
        DATE(MIN(order_date)) AS first_order_date
    FROM orders
    GROUP BY user_id
)

SELECT 
    DATE_TRUNC('month', first_order_date) AS cohort_month,
    COUNT(DISTINCT CASE WHEN DATE(order_date) = first_order_date THEN user_id END) AS new_users,
    COUNT(DISTINCT CASE WHEN DATE(order_date) = first_order_date + INTERVAL '1 month' THEN user_id END) AS retained_1m,
    ROUND(COUNT(DISTINCT CASE WHEN DATE(order_date) = first_order_date + INTERVAL '1 month' THEN user_id END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN DATE(order_date) = first_order_date THEN user_id END), 0), 2) AS retention_1m
FROM first_orders
JOIN orders USING (user_id)
GROUP BY DATE_TRUNC('month', first_order_date)
ORDER BY cohort_month;