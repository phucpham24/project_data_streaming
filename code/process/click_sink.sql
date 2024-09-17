INSERT INTO 
    clicks_sink 
SELECT 
    click_id,
    user_id,
    product_id,
    product,
    price,
    url,
    user_agent,
    ip_address,
    datetime_occured,
    PROCTIME() AS processing_time
FROM (
    SELECT 
    click_id,
    user_id,
    product_id,
    product,
    price,
    url,
    user_agent,
    ip_address,
    datetime_occured,
    ROW_NUMBER() OVER (
    PARTITION BY  click_id, product_id, user_id
    ORDER BY
        datetime_occured
    ) AS rn
    FROM clicks
)
where rn = 1
;