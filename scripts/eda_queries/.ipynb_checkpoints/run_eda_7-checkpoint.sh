echo "Running EDA Query 7: Price Sensitivity Analysis"

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/eda_queries/eda_7_price_sensitivity_and_conversion.hql

# Add headers to CSVs
echo "price_range,total_events,views,cart_adds,purchases,unique_users,unique_products,view_to_purchase_rate,cart_to_purchase_rate,cart_abandonment_rate,avg_price_in_range" > output/eda_7_price_analysis.csv
hdfs dfs -cat project/output/eda_7_price_analysis/* >> output/eda_7_price_analysis.csv

echo "category_code,avg_price,price_stddev,min_price,max_price,total_events,purchases,conversion_rate,price_25th_percentile,price_median,price_75th_percentile" > output/eda_7_category_price_sensitivity.csv
hdfs dfs -cat project/output/eda_7_category_price_sensitivity/* >> output/eda_7_category_price_sensitivity.csv

echo "Done eda_7"