echo "Running EDA Query 5: Session Quality Metrics"

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/eda_queries/eda_5_session_quality_metrics.hql

# Add header to CSV - Single table, single CSV
echo "session_type,session_count,percentage_of_total,avg_events_per_session,avg_views,avg_cart_adds,avg_removes,avg_distinct_products,avg_distinct_categories,avg_session_duration_sec,avg_time_between_events_sec,avg_product_price" > output/eda_5.csv
hdfs dfs -cat project/output/eda_5/* >> output/eda_5.csv

echo "Done eda_5"