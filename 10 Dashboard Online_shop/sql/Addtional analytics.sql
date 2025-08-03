-- Создаем таблицу возвратов
CREATE TABLE returns (
    return_id SERIAL PRIMARY KEY,
    order_item_id INT REFERENCES order_items(order_item_id),
    return_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    return_reason VARCHAR(100) NOT NULL,
    return_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    refund_amount DECIMAL(10, 2),
    processed_date TIMESTAMP
);

-- Добавляем тестовые данные возвратов
INSERT INTO returns (order_item_id, return_date, return_reason, return_status, refund_amount, processed_date) VALUES
(2, '2025-07-05 12:00:00', 'Не подошел размер', 'completed', 1999.99, '2025-07-08 14:30:00'),
(4, '2025-07-10 09:15:00', 'Не соответствует описанию', 'processing', NULL, NULL);