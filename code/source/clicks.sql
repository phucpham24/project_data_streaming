CREATE TABLE clicks (
    click_id STRING,
    user_id INT,
    product_id INTEGER,
    product STRING,
    price DOUBLE,
    url STRING,
    user_agent STRING,
    ip_address STRING,
    datetime_occured TIMESTAMP(3),
    thread_id bigint,
    processing_time AS PROCTIME(),
    WATERMARK FOR datetime_occured AS datetime_occured - INTERVAL '10' SECOND
) WITH (
    'connector' = '{{ connector }}',
    'topic' = '{{ topic }}',
    'properties.bootstrap.servers' = '{{ bootstrap_servers }}',
    'properties.group.id' = '{{ consumer_group_id }}',
    'scan.startup.mode' = '{{ scan_stratup_mode }}',
    'format' = '{{ format }}'
);