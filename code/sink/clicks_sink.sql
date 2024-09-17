CREATE TABLE clicks_sink (
    click_id STRING,
    user_id INT,
    product_id INTEGER,
    product STRING,
    price DOUBLE,
    url STRING,
    user_agent STRING,
    ip_address STRING,
    datetime_occured TIMESTAMP(3),
    processing_time TIMESTAMP,
    PRIMARY KEY (click_id) NOT ENFORCED
    ) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'commerce.clicks_sink',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)