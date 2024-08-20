-- create a commerce schema
CREATE SCHEMA commerce;

-- Use commerce schema
SET
    search_path TO commerce;

-- create a table named products
CREATE TABLE products (
    id int PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price REAL NOT NULL
);

-- create a users table
CREATE TABLE users (
    id int PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    PASSWORD VARCHAR(255) NOT NULL
);

ALTER TABLE
    products REPLICA IDENTITY FULL;

ALTER TABLE
    users REPLICA IDENTITY FULL;

CREATE TABLE attributed_checkouts (
    checkout_id VARCHAR PRIMARY KEY,
    user_id INTEGER,
    user_name VARCHAR,
    click_id VARCHAR,
    product_id VARCHAR,
    payment_method VARCHAR,
    total_amount DECIMAL(5, 2),
    shipping_address VARCHAR,
    billing_address VARCHAR,
    user_agent VARCHAR,
    ip_address VARCHAR,
    checkout_time TIMESTAMP,
    click_time TIMESTAMP
);

CREATE TABLE commerce.clicks (
    click_id VARCHAR PRIMARY KEY,
    user_id INTEGER,
    product_id VARCHAR,
    product VARCHAR(255),
    price DECIMAL(10, 2),
    url TEXT,
    user_agent VARCHAR(255),
    ip_address VARCHAR(45),
    datetime_occured TIMESTAMP(3) NOT NULL
);
CREATE TABLE commerce.checkouts (
    checkout_id VARCHAR PRIMARY KEY,
    user_id INTEGER,
    product_id VARCHAR,
    payment_method VARCHAR(255),
    total_amount DECIMAL(10, 2),
    shipping_address TEXT,
    billing_address TEXT,
    user_agent VARCHAR(255),
    ip_address VARCHAR(45),
    datetime_occured TIMESTAMP(3) NOT NULL
);
