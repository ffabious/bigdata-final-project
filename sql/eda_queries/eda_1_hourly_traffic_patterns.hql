-- EDA query 1: hourly traffic and purchase patterns
-- Business insight: understanding peak shopping hours helps optimize marketing campaigns and server resources

USE team18_projectdb;

SET hive.execution.engine=tez;

-- Create output table for hourly patterns
DROP TABLE IF EXISTS eda_1;
CREATE TABLE eda_1 AS
SELECT 
    HOUR(event_time) as hour_of_day,
    COUNT(*) as total_events,
    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as carts,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) as removals,
    ROUND(SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / COUNT(event_type), 2) as conversion_rate,
    ROUND(AVG(CASE WHEN event_type = 'purchase' THEN price ELSE NULL END), 2) as avg_purchase_value
FROM events_partitioned
GROUP BY HOUR(event_time)
ORDER BY hour_of_day;

INSERT OVERWRITE DIRECTORY 'project/output/eda_1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM eda_1
ORDER BY hour_of_day;