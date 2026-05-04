set -euo pipefail

echo "Stage 3: Predictive Data Analysis with Spark ML"
echo "Started at: $(date)"

PYTHON_BIN="${PYSPARK_PYTHON:-python3}"

if [ -d /usr/lib/spark ] && [ -z "${SPARK_HOME:-}" ]; then
  export SPARK_HOME=/usr/lib/spark
fi

export PYSPARK_DRIVER_PYTHON="$PYTHON_BIN"
export PYSPARK_PYTHON="$PYTHON_BIN"

mkdir -p data models output

SPARK_CONF_TMP=$(mktemp -d)
trap 'rm -rf "$SPARK_CONF_TMP"' EXIT
ORIGINAL_SPARK_CONF_DIR="${SPARK_CONF_DIR:-/etc/spark/conf}"
if [ -d "$ORIGINAL_SPARK_CONF_DIR" ]; then
  cp -R "$ORIGINAL_SPARK_CONF_DIR"/. "$SPARK_CONF_TMP"/
fi
if [ -f "$SPARK_CONF_TMP/spark-defaults.conf" ]; then
  grep -Ev '^(spark\.jars|spark\.yarn\.dist\.jars|spark\.sql\.hive\.metastore\.jars)[[:space:]]' \
    "$SPARK_CONF_TMP/spark-defaults.conf" > "$SPARK_CONF_TMP/spark-defaults.filtered"
  mv "$SPARK_CONF_TMP/spark-defaults.filtered" "$SPARK_CONF_TMP/spark-defaults.conf"
fi
printf '%s\n' 'spark.sql.catalogImplementation in-memory' >> \
  "$SPARK_CONF_TMP/spark-defaults.conf"
export SPARK_CONF_DIR="$SPARK_CONF_TMP"

SPARK_CMD=(
  spark-submit
  --master yarn
  --deploy-mode client
  --conf "spark.jars="
  --conf "spark.yarn.dist.jars="
  scripts/model.py
)

echo "Running: ${SPARK_CMD[*]}"
"${SPARK_CMD[@]}"

echo "Generated local Stage 3 artifacts:"
ls -la data/train.json data/test.json
ls -la output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv
ls -ld models/model1 models/model2

echo "Stage 3 completed at: $(date)"
