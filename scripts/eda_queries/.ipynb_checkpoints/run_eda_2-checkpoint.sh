echo "Running EDA query 2: category performance analysis"

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/eda_queries/eda_2_category_performance.hql

# Add header to CSV
echo "category_code,category_id,total_events,unique_products,unique_users,views,cart_adds,purchases,cart_removals,view_to_purchase_rate,view_to_cart_rate,cart_to_purchase_rate,cart_abandonment_rate,avg_purchase_price,min_price,max_price" > output/eda_2.csv
hdfs dfs -cat project/output/eda_2/* >> output/eda_2.csv

echo "Done eda_2"