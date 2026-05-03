-- EDA query 2: category performance analysis
-- Business insight: identify top and underperforming categories for inventory management and promotional strategies

USE team18_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS eda_2;
CREATE TABLE eda_2 AS
WITH category_events AS (
    SELECT 
        category_code,
        category_id,
        COUNT(*) as total_events,
        COUNT(DISTINCT product_id) as unique_products,
        COUNT(DISTINCT user_id) as unique_users,
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
        SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as cart_adds,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
        SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) as cart_removals,
        AVG(CASE WHEN event_type = 'purchase' THEN price ELSE NULL END) as avg_purchase_price,
        MIN(CASE WHEN event_type = 'purchase' THEN price ELSE NULL END) as min_price,
        MAX(CASE WHEN event_type = 'purchase' THEN price ELSE NULL END) as max_price
    FROM events_partitioned
    WHERE category_code IS NOT NULL AND category_code != ''
    GROUP BY category_code, category_id
)
SELECT 
    category_code,
    category_id,
    total_events,
    unique_products,
    unique_users,
    views,
    cart_adds,
    purchases,
    cart_removals,
    ROUND(purchases * 100.0 / NULLIF(views, 0), 2) as view_to_purchase_rate,
    ROUND(cart_adds * 100.0 / NULLIF(views, 0), 2) as view_to_cart_rate,
    ROUND(purchases * 100.0 / NULLIF(cart_adds, 0), 2) as cart_to_purchase_rate,
    ROUND(cart_removals * 100.0 / NULLIF(cart_adds, 0), 2) as cart_abandonment_rate,
    ROUND(avg_purchase_price, 2) as avg_purchase_price,
    ROUND(min_price, 2) as min_price,
    ROUND(max_price, 2) as max_price
FROM category_events
ORDER BY purchases DESC
LIMIT 50;

INSERT OVERWRITE DIRECTORY 'project/output/eda_2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM eda_2;