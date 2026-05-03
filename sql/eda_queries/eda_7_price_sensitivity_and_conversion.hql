-- EDA query 7: price sensitivity and conversion analysis
-- Business insight: understand price elasticity to optimize pricing strategy and promotions

USE team18_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS eda_7_price_analysis;
CREATE TABLE eda_7_price_analysis AS
WITH price_buckets AS (
    SELECT 
        CASE 
            WHEN price < 10 THEN 'Under $10'
            WHEN price >= 10 AND price < 25 THEN '$10-$24.99'
            WHEN price >= 25 AND price < 50 THEN '$25-$49.99'
            WHEN price >= 50 AND price < 100 THEN '$50-$99.99'
            WHEN price >= 100 AND price < 250 THEN '$100-$249.99'
            WHEN price >= 250 AND price < 500 THEN '$250-$499.99'
            WHEN price >= 500 AND price < 1000 THEN '$500-$999.99'
            ELSE '$1000+'
        END as price_range,
        event_type,
        user_id,
        user_session,
        product_id,
        price
    FROM events_partitioned
    WHERE price IS NOT NULL AND price > 0
)
SELECT 
    price_range,
    COUNT(*) as total_events,
    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as cart_adds,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT product_id) as unique_products,
    ROUND(SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as view_to_purchase_rate,
    ROUND(SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END), 0), 2) as cart_to_purchase_rate,
    ROUND(SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END), 0), 2) as cart_abandonment_rate,
    ROUND(AVG(price), 2) as avg_price_in_range
FROM price_buckets
GROUP BY price_range
ORDER BY 
    CASE price_range
        WHEN 'Under $10' THEN 1
        WHEN '$10-$24.99' THEN 2
        WHEN '$25-$49.99' THEN 3
        WHEN '$50-$99.99' THEN 4
        WHEN '$100-$249.99' THEN 5
        WHEN '$250-$499.99' THEN 6
        WHEN '$500-$999.99' THEN 7
        ELSE 8
    END;

INSERT OVERWRITE DIRECTORY 'project/output/eda_7_price_analysis'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM eda_7_price_analysis;



-- Additional analysis: category-wise price sensitivity
DROP TABLE IF EXISTS eda_7_category_price_sensitivity;
CREATE TABLE eda_7_category_price_sensitivity AS
SELECT 
    category_code,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(STDDEV(price), 2) as price_stddev,
    ROUND(MIN(price), 2) as min_price,
    ROUND(MAX(price), 2) as max_price,
    COUNT(*) as total_events,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    ROUND(SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate,
    -- Price distribution within category
    ROUND(PERCENTILE_APPROX(price, 0.25), 2) as price_25th_percentile,
    ROUND(PERCENTILE_APPROX(price, 0.50), 2) as price_median,
    ROUND(PERCENTILE_APPROX(price, 0.75), 2) as price_75th_percentile
FROM events_partitioned
WHERE category_code IS NOT NULL 
    AND category_code != '' 
    AND price IS NOT NULL 
    AND price > 0
GROUP BY category_code
ORDER BY purchases DESC
LIMIT 30;

INSERT OVERWRITE DIRECTORY 'project/output/eda_7_category_price_sensitivity'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM eda_7_category_price_sensitivity;