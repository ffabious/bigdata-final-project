-- EDA query 4: brand affinity and performance analysis
-- Business insight: understand brand loyalty and performance for partnership and pricing strategies

USE team18_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS eda_4;
CREATE TABLE eda_4 AS
WITH brand_interactions AS (
    SELECT 
        brand,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT product_id) as unique_products,
        COUNT(*) as total_interactions,
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
        SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as cart_adds,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
        AVG(CASE WHEN event_type = 'purchase' THEN price ELSE NULL END) as avg_price,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) as unique_buyers
    FROM events_partitioned
    WHERE brand IS NOT NULL AND brand != ''
    GROUP BY brand
),
brand_loyalty AS (
    SELECT 
        brand,
        COUNT(DISTINCT user_id) as repeat_buyers
    FROM (
        SELECT 
            brand,
            user_id,
            COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_session END) as purchase_sessions
        FROM events_partitioned
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand, user_id
        HAVING COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_session END) > 1
    ) t
    GROUP BY brand
)
SELECT 
    bi.brand,
    bi.unique_users,
    bi.unique_products,
    bi.total_interactions,
    bi.views,
    bi.cart_adds,
    bi.purchases,
    ROUND(bi.avg_price, 2) as avg_purchase_price,
    bi.unique_buyers,
    ROUND(bi.purchases * 100.0 / NULLIF(bi.views, 0), 2) as conversion_rate,
    COALESCE(bl.repeat_buyers, 0) as repeat_buyers,
    ROUND(COALESCE(bl.repeat_buyers, 0) * 100.0 / NULLIF(bi.unique_buyers, 0), 2) as loyalty_rate
FROM brand_interactions bi
LEFT JOIN brand_loyalty bl ON bi.brand = bl.brand
ORDER BY bi.purchases DESC
LIMIT 50;

INSERT OVERWRITE DIRECTORY 'project/output/eda_4'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM eda_4;