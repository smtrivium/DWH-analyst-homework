-- Заполняем таблицу категорий
INSERT INTO product_categories (category_name) VALUES
('Электроника'), ('Одежда'), ('Книги'), ('Дом и сад');

-- Добавляем подкатегории
INSERT INTO product_categories (category_name, parent_category_id) VALUES
('Смартфоны', 1), ('Ноутбуки', 1), ('Телевизоры', 1),
('Мужская одежда', 2), ('Женская одежда', 2),
('Художественная литература', 3), ('Научная литература', 3),
('Мебель', 4), ('Бытовая техника', 4);

-- Заполняем таблицу товаров
INSERT INTO products (product_name, category_id, price, stock_quantity) VALUES
('Смартфон X', 5, 59999.99, 100),
('Ноутбук Pro', 6, 89999.99, 50),
('Футболка мужская', 8, 1999.99, 200),
('Роман "Путешествие"', 10, 499.99, 150),
('Диван угловой', 13, 34999.99, 20),
('Холодильник', 14, 45999.99, 30);

-- Добавляем пользователей
INSERT INTO users (username, email, registration_date, region, age, gender) VALUES
('ivanov', 'ivanov@example.com', '2025-01-15', 'Москва', 30, 'male'),
('petrova', 'petrova@example.com', '2025-02-20', 'Санкт-Петербург', 25, 'female'),
('sidorov', 'sidorov@example.com', '2025-03-10', 'Новосибирск', 40, 'male'),
('smirnova', 'smirnova@example.com', '2025-04-05', 'Екатеринбург', 35, 'female');

-- Добавляем посещения
INSERT INTO visits (user_id, visit_date, session_duration, traffic_source, device_type) VALUES
(1, '2025-07-01 10:00:00', 300, 'organic', 'desktop'),
(2, '2025-07-01 11:30:00', 450, 'social', 'mobile'),
(3, '2025-07-02 09:15:00', 600, 'email', 'tablet'),
(1, '2025-07-02 14:00:00', 180, 'direct', 'mobile'),
(4, '2025-07-03 16:45:00', 720, 'organic', 'desktop');

-- Добавляем заказы
INSERT INTO orders (user_id, order_date, total_amount, status, payment_method) VALUES
(1, '2025-07-01 10:05:00', 59999.99, 'completed', 'credit_card'),
(2, '2025-07-01 11:35:00', 1999.99, 'completed', 'paypal'),
(3, '2025-07-02 09:20:00', 34999.99, 'processing', 'credit_card'),
(1, '2025-07-02 14:05:00', 499.99, 'shipped', 'paypal');

-- Добавляем позиции заказов
INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase) VALUES
(1, 1, 1, 59999.99),
(2, 3, 1, 1999.99),
(3, 5, 1, 34999.99),
(4, 4, 1, 499.99);