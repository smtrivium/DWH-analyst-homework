SELECT 
    pc.category_name,
    AVG(o.total_amount) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN product_categories pc ON p.category_id = pc.category_id
GROUP BY pc.category_name
ORDER BY avg_order_value DESC;