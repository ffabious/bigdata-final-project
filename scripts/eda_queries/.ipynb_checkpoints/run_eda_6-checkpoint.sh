echo "Running EDA Query 6: Session Timing Patterns"

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/eda_queries/eda_6_session_timing.hql

# Add header to CSV - Single table, single CSV
echo "first_event_hour,day_type,session_count,purchase_sessions,purchase_rate,avg_events,avg_duration_seconds" > output/eda_6.csv
hdfs dfs -cat project/output/eda_6/* >> output/eda_6.csv

echo "Done eda_6"