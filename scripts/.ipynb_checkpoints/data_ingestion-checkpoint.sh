password=$(head -n 1 secrets/.psql.pass)

echo "Clearing HDFS warehouse directory..."
hdfs dfs -rm -r -f project/warehouse
hdfs dfs -mkdir -p project/warehouse

echo "Importing data from PostgreSQL to HDFS..."
sqoop import-all-tables \
    --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team18_projectdb \
    --username team18 \
    --password $password \
    --compression-codec=snappy \
    --compress \
    --as-avrodatafile \
    --warehouse-dir=project/warehouse \
    --m 1

echo "Copying AVRO schema files to output directory..."
mkdir -p output
cp *.avsc output/ 2>/dev/null || true
cp *.java output/ 2>/dev/null || true

echo "Data ingestion completed successfully"