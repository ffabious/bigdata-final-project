-- EDA query 6: session timing and purchase patterns
-- Business insight: identify optimal days and hours when purchase intent is highest

USE team18_projectdb;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS eda_6;
CREATE TABLE eda_6 AS
SELECT 
    first_event_hour,
    CASE 
        WHEN first_event_day_of_week IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as session_count,
    SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) as purchase_sessions,
    ROUND(SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as purchase_rate,
    ROUND(AVG(event_count), 1) as avg_events,
    ROUND(AVG(total_session_duration_seconds), 1) as avg_duration_seconds
FROM session_features
WHERE session_date IS NOT NULL
GROUP BY first_event_hour, 
    CASE 
        WHEN first_event_day_of_week IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END
ORDER BY first_event_hour, day_type;

INSERT OVERWRITE DIRECTORY 'project/output/eda_6'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM eda_6;