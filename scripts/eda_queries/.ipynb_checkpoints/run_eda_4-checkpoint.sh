echo "Running EDA Query 4: Brand Affinity Analysis"

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/eda_queries/eda_4_brand_affinity_analysis.hql

# Add header to CSV
echo "brand,unique_users,unique_products,total_interactions,views,cart_adds,purchases,avg_purchase_price,unique_buyers,conversion_rate,repeat_buyers,loyalty_rate" > output/eda_4.csv
hdfs dfs -cat project/output/eda_4/* >> output/eda_4.csv

echo "Done eda_4"