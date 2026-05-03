echo "Stage 2: Data Storage/Preparation & EDA"
echo "Started at: $(date)"

password=$(head -n 1 secrets/.hive.pass)

echo "Step 1: Moving AVRO schemas to HDFS..."
hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put -f output/*.avsc project/warehouse/avsc/

# takes approximately 5 minutes
echo "Step 2: Creating Hive database and external tables..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/db.hql > output/hive_results.txt

# takes approximately 13 minutes
echo "Step 3: Creating partitioned and bucketed tables..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team18 -p $password -f sql/create_partitioned_tables.hql > output/hive_results.txt

echo "Step 4: Running EDA queries..."
bash scripts/run_all_eda.sh

echo "Step 5: Verifying outputs..."
echo "CSV files generated:"
ls -la output/*.csv

echo "Stage 2 completed at: $(date)"