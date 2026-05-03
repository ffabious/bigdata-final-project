echo "Stage 1: Data Collection and Ingestion"
echo "Started at: $(date)"

source venv/bin/activate

# takes approximately 12 minutes
echo "Step 1: Collecting data..."
bash scripts/data_collection.sh

# takes approximately 15 minutes
echo "Step 2: Building PostgreSQL database..."
python scripts/build_projectdb.py

# takes approximately 30 minutes
echo "Step 3: Ingesting data to HDFS..."
bash scripts/data_ingestion.sh

echo "Stage 1 completed at: $(date)"