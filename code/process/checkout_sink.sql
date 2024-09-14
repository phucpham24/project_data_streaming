INSERT INTO 
    checkouts_sink 
SELECT 
    checkout_id,
    user_id,
    product_id,
    payment_method,
    total_amount,
    shipping_address,
    billing_address,
    user_agent,
    ip_address,
    datetime_occured
FROM (
    SELECT 
    checkout_id,
    user_id,
    product_id,
    payment_method,
    total_amount,
    shipping_address,
    billing_address,
    user_agent,
    ip_address,
    datetime_occured,
    ROW_NUMBER() OVER (
    PARTITION BY  checkout_id,product_id, user_id
    ORDER BY
        datetime_occured
    ) AS rn
    FROM checkouts
)
where rn = 1
;