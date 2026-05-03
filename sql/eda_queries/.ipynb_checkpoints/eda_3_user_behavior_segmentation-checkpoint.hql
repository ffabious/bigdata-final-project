-- EDA query 3: user behavior segmentation
-- Business insight: segment users by activity levels to personalize marketing and improve retention

USE team18_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS eda_3;
CREATE TABLE eda_3 AS
WITH user_metrics AS (
    SELECT 
        user_id,
        COUNT(DISTINCT user_session) as total_sessions,
        COUNT(*) as total_events,
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as total_views,
        SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as total_carts,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
        COUNT(DISTINCT product_id) as distinct_products_viewed,
        COUNT(DISTINCT category_id) as distinct_categories_explored,
        AVG(price) as avg_price_interest,
        DATEDIFF(MAX(event_time), MIN(event_time)) as active_days_span
    FROM events_partitioned
    GROUP BY user_id
),
user_segments AS (
    SELECT 
        *,
        CASE 
            WHEN total_purchases > 5 THEN 'High Value Customer'
            WHEN total_purchases > 0 THEN 'Regular Customer'
            WHEN total_carts > 0 THEN 'Engaged Browser'
            WHEN total_views > 10 THEN 'Active Browser'
            WHEN total_events > 0 THEN 'Passive Browser'
            ELSE 'Unknown'
        END as user_segment
    FROM user_metrics
)
SELECT 
    user_segment,
    COUNT(*) as user_count,
    ROUND(AVG(total_sessions), 1) as avg_sessions,
    ROUND(AVG(total_views), 1) as avg_views,
    ROUND(AVG(total_carts), 1) as avg_carts,
    ROUND(AVG(total_purchases), 1) as avg_purchases,
    ROUND(AVG(distinct_products_viewed), 1) as avg_products_explored,
    ROUND(AVG(distinct_categories_explored), 1) as avg_categories_explored,
    ROUND(AVG(active_days_span), 1) as avg_active_days,
    ROUND(SUM(total_purchases) * 100.0 / SUM(total_views), 2) as conversion_rate
FROM user_segments
GROUP BY user_segment
ORDER BY 
    CASE user_segment
        WHEN 'High Value Customer' THEN 1
        WHEN 'Regular Customer' THEN 2
        WHEN 'Engaged Browser' THEN 3
        WHEN 'Active Browser' THEN 4
        WHEN 'Passive Browser' THEN 5
        ELSE 6
    END;


INSERT OVERWRITE DIRECTORY 'project/output/eda_3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM eda_3;