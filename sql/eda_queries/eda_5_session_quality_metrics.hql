-- EDA query 5: session quality and purchase intent analysis
-- Business insight: identify key indicators of purchase intent to optimize user experience

USE team18_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS eda_5;
CREATE TABLE eda_5 AS
SELECT 
    CASE 
        WHEN purchase_count > 0 THEN 'Purchase Sessions'
        WHEN carts_count > 0 THEN 'Cart Sessions (No Purchase)'
        WHEN views_count > 5 THEN 'High Engagement Sessions'
        WHEN views_count > 1 THEN 'Low Engagement Sessions'
        ELSE 'Bounce Sessions'
    END as session_type,
    COUNT(*) as session_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage_of_total,
    ROUND(AVG(event_count), 1) as avg_events_per_session,
    ROUND(AVG(views_count), 1) as avg_views,
    ROUND(AVG(carts_count), 1) as avg_cart_adds,
    ROUND(AVG(remove_count), 1) as avg_removes,
    ROUND(AVG(distinct_products), 1) as avg_distinct_products,
    ROUND(AVG(distinct_categories), 1) as avg_distinct_categories,
    ROUND(AVG(total_session_duration_seconds), 1) as avg_session_duration_sec,
    ROUND(AVG(avg_time_between_events_seconds), 1) as avg_time_between_events_sec,
    ROUND(AVG(avg_price), 2) as avg_product_price
FROM session_features
WHERE session_date IS NOT NULL
GROUP BY 
    CASE 
        WHEN purchase_count > 0 THEN 'Purchase Sessions'
        WHEN carts_count > 0 THEN 'Cart Sessions (No Purchase)'
        WHEN views_count > 5 THEN 'High Engagement Sessions'
        WHEN views_count > 1 THEN 'Low Engagement Sessions'
        ELSE 'Bounce Sessions'
    END
ORDER BY 
    CASE session_type
        WHEN 'Purchase Sessions' THEN 1
        WHEN 'Cart Sessions (No Purchase)' THEN 2
        WHEN 'High Engagement Sessions' THEN 3
        WHEN 'Low Engagement Sessions' THEN 4
        WHEN 'Bounce Sessions' THEN 5
    END;


INSERT OVERWRITE DIRECTORY 'project/output/eda_5'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM eda_5;