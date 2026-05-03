USE team18_projectdb;

-- Set optimization parameters
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=500; 

-- Table 1: Events partitioned by year, month, day
-- This allows efficient time-based queries
DROP TABLE IF EXISTS events_partitioned;

CREATE EXTERNAL TABLE events_partitioned (
    event_time TIMESTAMP,
    event_type STRING,
    product_id BIGINT,
    category_id BIGINT,
    category_code STRING,
    brand STRING,
    price DECIMAL(10,2),
    user_id BIGINT,
    user_session STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/events_partitioned'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Insert data with dynamic partitioning
INSERT OVERWRITE TABLE events_partitioned
PARTITION (year, month, day)
SELECT 
    FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT)) as event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session,
    YEAR(FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT))) as year,
    MONTH(FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT))) as month,
    DAY(FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT))) as day
FROM events_raw
WHERE event_time IS NOT NULL;

-- Table 2: Session-level aggregated features 
-- Partitioned by date, bucketed by user_id for efficient joins
DROP TABLE IF EXISTS session_features;

CREATE EXTERNAL TABLE session_features (
    user_session STRING,
    user_id BIGINT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    event_count INT,
    views_count INT,
    carts_count INT,
    remove_count INT,
    purchase_count INT,
    distinct_products INT,
    distinct_categories INT,
    distinct_brands INT,
    avg_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    total_session_duration_seconds BIGINT,
    avg_time_between_events_seconds DOUBLE,
    first_event_hour INT,
    first_event_day_of_week INT,
    has_purchase BOOLEAN
)
PARTITIONED BY (session_date STRING)
CLUSTERED BY (user_id) INTO 16 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/session_features'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Insert aggregated session features
INSERT OVERWRITE TABLE session_features
PARTITION (session_date)
SELECT 
    user_session,
    MAX(user_id) as user_id,
    MIN(event_time) as session_start,
    MAX(event_time) as session_end,
    COUNT(*) as event_count,
    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views_count,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as carts_count,
    SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) as remove_count,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
    COUNT(DISTINCT product_id) as distinct_products,
    COUNT(DISTINCT category_id) as distinct_categories,
    COUNT(DISTINCT brand) as distinct_brands,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price,
    UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time)) as total_session_duration_seconds,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / NULLIF(COUNT(*) - 1, 0) as avg_time_between_events_seconds,
    HOUR(MIN(event_time)) as first_event_hour,
    DAYOFWEEK(MIN(event_time)) as first_event_day_of_week,
    MAX(CASE WHEN event_type = 'purchase' THEN TRUE ELSE FALSE END) as has_purchase,
    TO_DATE(MIN(event_time)) as session_date
FROM events_partitioned
GROUP BY user_session;

-- Table 3: Historical user features (30-day rolling window)
-- Partitioned by date for efficient time-based queries
DROP TABLE IF EXISTS user_historical_features;

CREATE EXTERNAL TABLE user_historical_features (
    user_id BIGINT,
    purchase_count_30d INT,
    session_count_30d INT,
    avg_session_duration_30d DOUBLE,
    total_views_30d INT,
    total_carts_30d INT,
    cart_to_view_rate_30d DOUBLE,
    purchase_rate_30d DOUBLE,
    days_since_last_session INT,
    days_since_last_purchase INT,
    preferred_category_id BIGINT,
    preferred_brand STRING,
    avg_purchase_value_30d DECIMAL(10,2)
)
PARTITIONED BY (calculation_date STRING)
CLUSTERED BY (user_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/user_historical_features'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Insert historical user features (example for a specific date)
WITH user_30d_stats AS (
    SELECT 
        user_id,
        COUNT(DISTINCT CASE WHEN has_purchase THEN user_session END) as sessions_with_purchase,
        COUNT(DISTINCT user_session) as total_sessions,
        SUM(views_count) as total_views,
        SUM(carts_count) as total_carts,
        AVG(total_session_duration_seconds) as avg_duration,
        AVG(CASE WHEN has_purchase THEN avg_price END) as avg_purchase_value,
        MAX(session_start) as last_session_time,
        MAX(CASE WHEN has_purchase THEN session_start END) as last_purchase_time
    FROM session_features
    WHERE session_date >= DATE_SUB('2019-12-31', 30)
    GROUP BY user_id
)
INSERT OVERWRITE TABLE user_historical_features
PARTITION (calculation_date = '2019-12-31')
SELECT 
    user_id,
    sessions_with_purchase as purchase_count_30d,
    total_sessions as session_count_30d,
    avg_duration as avg_session_duration_30d,
    total_views as total_views_30d,
    total_carts as total_carts_30d,
    CASE WHEN total_views > 0 THEN total_carts * 1.0 / total_views ELSE 0 END as cart_to_view_rate_30d,
    CASE WHEN total_sessions > 0 THEN sessions_with_purchase * 1.0 / total_sessions ELSE 0 END as purchase_rate_30d,
    DATEDIFF('2019-12-31', last_session_time) as days_since_last_session,
    DATEDIFF('2019-12-31', last_purchase_time) as days_since_last_purchase,
    NULL as preferred_category_id,  -- To be computed separately
    NULL as preferred_brand,  -- To be computed separately
    avg_purchase_value as avg_purchase_value_30d
FROM user_30d_stats;

-- Verify partitioned tables
SHOW PARTITIONS events_partitioned;
SHOW PARTITIONS session_features;
SHOW PARTITIONS user_historical_features;

-- Check table structures
DESCRIBE FORMATTED events_partitioned;
DESCRIBE FORMATTED session_features;
DESCRIBE FORMATTED user_historical_features;