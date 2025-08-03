SELECT 
    DATE(visit_date) AS visit_day,
    COUNT(DISTINCT v.user_id) AS visitors,
    COUNT(DISTINCT o.user_id) AS buyers,
    ROUND(COUNT(DISTINCT o.user_id) * 100.0 / NULLIF(COUNT(DISTINCT v.user_id), 0), 2) AS conversion_rate
FROM visits v
LEFT JOIN orders o ON v.user_id = o.user_id AND DATE(o.order_date) = DATE(v.visit_date)
GROUP BY DATE(visit_date)
ORDER BY visit_day;