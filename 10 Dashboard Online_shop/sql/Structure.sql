-- Создаем базу данных для интернет-магазина
CREATE DATABASE online_store_bi;

-- Подключаемся к базе данных
\c online_store_bi

-- Создаем таблицу пользователей
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    registration_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    region VARCHAR(50),
    age INT,
    gender VARCHAR(10)
);

-- Создаем таблицу категорий товаров
CREATE TABLE product_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    parent_category_id INT REFERENCES product_categories(category_id)
);

-- Создаем таблицу товаров
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category_id INT REFERENCES product_categories(category_id),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0
);

-- Создаем таблицу заказов
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'processing',
    payment_method VARCHAR(30)
);

-- Создаем таблицу позиций заказов
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    quantity INT NOT NULL,
    price_at_purchase DECIMAL(10, 2) NOT NULL
);

-- Создаем таблицу посещений сайта
CREATE TABLE visits (
    visit_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    visit_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    session_duration INT, -- в секундах
    traffic_source VARCHAR(50),
    device_type VARCHAR(20)
);