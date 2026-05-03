#!/bin/bash
echo "Running EDA query 1: hourly traffic patterns"

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/eda_queries/eda_1_hourly_traffic_patterns.hql

# Add header to CSV
echo "hour_of_day,total_events,views,carts,purchases,removals,conversion_rate,avg_purchase_value" > output/eda_1.csv
hdfs dfs -cat project/output/eda_1/* >> output/eda_1.csv
echo "Done eda_1"