echo "Running EDA Query 3: user behavior segmentation"

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/eda_queries/eda_3_user_behavior_segmentation.hql

# Add header to CSV
echo "user_segment,user_count,avg_sessions,avg_views,avg_carts,avg_purchases,avg_products_explored,avg_categories_explored,avg_active_days,conversion_rate" > output/eda_3.csv
hdfs dfs -cat project/output/eda_3/* >> output/eda_3.csv

echo "Done eda_3"