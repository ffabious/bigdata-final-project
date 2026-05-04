echo "Stage 3: Predictive Data Analysis with Spark ML"
echo "Started at: $(date)"

mkdir -p data models output

spark-submit \
  --master yarn \
  --deploy-mode client \
  scripts/model.py

echo "Generated local Stage 3 artifacts:"
ls -la data/train.json data/test.json
ls -la output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv
ls -ld models/model1 models/model2

echo "Stage 3 completed at: $(date)"
