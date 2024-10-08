CREATE TABLE checkouts_sink (
    checkout_id STRING,
    user_id INTEGER,
    product_id INTEGER,
    payment_method STRING,
    total_amount FLOAT,
    shipping_address STRING,
    billing_address STRING,
    user_agent STRING,
    ip_address STRING,
    datetime_occured TIMESTAMP(3),
    processing_time TIMESTAMP,
    PRIMARY KEY (checkout_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'commerce.checkouts_sink',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
);
