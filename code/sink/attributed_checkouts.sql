CREATE TABLE attributed_checkouts (
    checkout_id STRING,
    user_id INTEGER,
    user_name STRING,
    click_id STRING,
    product_id INTEGER,
    payment_method STRING,
    total_amount FLOAT,
    shipping_address STRING,
    billing_address STRING,
    user_agent STRING,
    ip_address STRING,
    checkout_time TIMESTAMP(3),
    click_time TIMESTAMP,
    click_time_source TIMESTAMP,
    checkout_time_source TIMESTAMP,
    processing_time TIMESTAMP,
    PRIMARY KEY (checkout_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'commerce.attributed_checkouts',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)