# pylint: disable=import-error,no-name-in-module,too-many-locals,too-many-statements
import math
import os
import shutil
import subprocess

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as func


TEAM = 18
WAREHOUSE = "project/hive/warehouse"
EVENTS_PARQUET_PATH = f"{WAREHOUSE}/events_partitioned"
HDFS_DATA_DIR = "project/data"
HDFS_MODEL_DIR = "project/models"
HDFS_OUTPUT_DIR = "project/output"
HDFS_TRAIN_PATH = f"{HDFS_DATA_DIR}/train"
HDFS_TEST_PATH = f"{HDFS_DATA_DIR}/test"
HDFS_MODEL1_PREDICTIONS_PATH = f"{HDFS_OUTPUT_DIR}/model1_predictions"
HDFS_MODEL2_PREDICTIONS_PATH = f"{HDFS_OUTPUT_DIR}/model2_predictions"
HDFS_EVALUATION_PATH = f"{HDFS_OUTPUT_DIR}/evaluation"
LOCAL_DATA_DIR = "data"
LOCAL_MODEL_DIR = "models"
LOCAL_OUTPUT_DIR = "output"
SEED = 18
SECONDS_IN_DAY = 24 * 60 * 60
HISTORY_DAYS = 30
NUMERIC_FEATURES = [
    "views_count",
    "carts_count",
    "remove_from_cart_count",
    "distinct_products",
    "distinct_categories",
    "distinct_brands",
    "avg_price",
    "max_price",
    "min_price",
    "prefix_duration_seconds",
    "avg_time_between_events_seconds",
    "cart_to_view_rate",
    "remove_to_cart_rate",
    "cart_in_first_3_events_flag",
    "user_purchase_count_30d",
    "user_days_since_last_session",
    "user_purchase_rate",
    "first_event_hour_sin",
    "first_event_hour_cos",
    "first_event_day_sin",
    "first_event_day_cos",
    "first_event_month_sin",
    "first_event_month_cos",
]
RAW_OUTPUT_COLUMNS = [
    "user_session",
    "user_id",
    "label",
    "views_count",
    "carts_count",
    "remove_from_cart_count",
    "distinct_products",
    "distinct_categories",
    "distinct_brands",
    "avg_price",
    "max_price",
    "min_price",
    "prefix_duration_seconds",
    "avg_time_between_events_seconds",
    "first_event_hour",
    "first_event_day_of_week",
    "first_event_month",
    "cart_to_view_rate",
    "remove_to_cart_rate",
    "cart_in_first_3_events_flag",
    "last_prefix_event_type",
    "user_purchase_count_30d",
    "user_days_since_last_session",
    "user_purchase_rate",
]


def run_command(command):
    """Run a shell command and fail fast if it returns a non-zero status."""
    subprocess.run(command, check=True)


def hdfs_rm(path):
    """Remove an HDFS path if it exists."""
    run_command(["hdfs", "dfs", "-rm", "-r", "-f", path])


def hdfs_getmerge(hdfs_path, local_path):
    """Download a Spark directory output into one local file."""
    if os.path.exists(local_path):
        os.remove(local_path)
    run_command(["hdfs", "dfs", "-getmerge", hdfs_path, local_path])


def hdfs_get_dir(hdfs_path, local_path):
    """Download an HDFS directory, replacing only this script's artifact path."""
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    run_command(["hdfs", "dfs", "-get", hdfs_path, local_path])


def build_spark_session():
    """Create the Spark session configured for the university YARN cluster."""
    return (
        SparkSession.builder.appName(f"team{TEAM} - stage III spark ML")
        .master("yarn")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.sql.avro.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )


def read_events(spark):
    """Read normalized events from the Hive warehouse Parquet table path."""
    events = spark.read.parquet(EVENTS_PARQUET_PATH)
    return events.select(
        func.col("event_time").cast("timestamp").alias("event_time"),
        func.col("event_type"),
        func.col("product_id").cast("long").alias("product_id"),
        func.col("category_id").cast("long").alias("category_id"),
        func.col("brand"),
        func.col("price").cast("double").alias("price"),
        func.col("user_id").cast("long").alias("user_id"),
        func.col("user_session"),
    ).where(
        func.col("event_time").isNotNull()
        & func.col("event_type").isin(
            "view", "cart", "remove_from_cart", "purchase"
        )
        & func.col("user_id").isNotNull()
        & func.col("user_session").isNotNull()
    )


def safe_rate(numerator, denominator):
    """Build a Spark expression for numerator / denominator with zero fallback."""
    return func.when(denominator > 0, numerator / denominator).otherwise(func.lit(0.0))


def add_cyclical_time_features(dataframe):
    """Encode hour, weekday, and month as cyclical features."""
    dataframe = dataframe.withColumn(
        "first_event_hour_sin",
        func.sin(2 * math.pi * func.col("first_event_hour") / func.lit(24)),
    ).withColumn(
        "first_event_hour_cos",
        func.cos(2 * math.pi * func.col("first_event_hour") / func.lit(24)),
    )
    dataframe = dataframe.withColumn(
        "first_event_day_sin",
        func.sin(2 * math.pi * func.col("first_event_day_of_week") / func.lit(7)),
    ).withColumn(
        "first_event_day_cos",
        func.cos(2 * math.pi * func.col("first_event_day_of_week") / func.lit(7)),
    )
    return dataframe.withColumn(
        "first_event_month_sin",
        func.sin(2 * math.pi * func.col("first_event_month") / func.lit(12)),
    ).withColumn(
        "first_event_month_cos",
        func.cos(2 * math.pi * func.col("first_event_month") / func.lit(12)),
    )


def build_session_dataset(events):
    """Engineer prefix and historical features for purchase prediction."""
    session_order = Window.partitionBy("user_session", "user_id").orderBy(
        "event_time", "event_type", "product_id"
    )
    ordered_events = events.withColumn(
        "event_number", func.row_number().over(session_order)
    ).withColumn("event_epoch", func.unix_timestamp("event_time"))

    full_sessions = ordered_events.groupBy("user_session", "user_id").agg(
        func.min("event_time").alias("session_start"),
        func.max("event_time").alias("session_end"),
        func.count("*").cast("int").alias("session_event_count"),
        func.sum(func.when(func.col("event_type") == "purchase", 1).otherwise(0))
        .cast("int")
        .alias("session_purchase_count"),
    )

    user_history_window = (
        Window.partitionBy("user_id")
        .orderBy(func.col("session_start_epoch"))
        .rangeBetween(-(HISTORY_DAYS * SECONDS_IN_DAY), -1)
    )
    previous_session_window = Window.partitionBy("user_id").orderBy("session_start")
    historical_features = (
        full_sessions.withColumn(
            "session_start_epoch", func.unix_timestamp("session_start")
        )
        .withColumn(
            "sessions_30d", func.count("user_session").over(user_history_window)
        )
        .withColumn(
            "purchase_sessions_30d",
            func.sum(
                func.when(func.col("session_purchase_count") > 0, 1).otherwise(0)
            ).over(user_history_window),
        )
        .withColumn(
            "user_purchase_count_30d",
            func.sum("session_purchase_count").over(user_history_window),
        )
        .withColumn(
            "previous_session_start",
            func.lag("session_start").over(previous_session_window),
        )
        .withColumn(
            "user_days_since_last_session",
            func.coalesce(
                func.datediff("session_start", "previous_session_start"),
                func.lit(9999),
            ),
        )
        .withColumn(
            "user_purchase_rate",
            safe_rate(func.col("purchase_sessions_30d"), func.col("sessions_30d")),
        )
        .select(
            "user_session",
            "user_id",
            func.coalesce(func.col("user_purchase_count_30d"), func.lit(0))
            .cast("double")
            .alias("user_purchase_count_30d"),
            func.col("user_days_since_last_session").cast("double"),
            func.coalesce(func.col("user_purchase_rate"), func.lit(0.0))
            .cast("double")
            .alias("user_purchase_rate"),
        )
    )

    prefix_events = ordered_events.where(func.col("event_number") <= 10)
    future_labels = (
        ordered_events.where(func.col("event_number") > 10)
        .groupBy("user_session", "user_id")
        .agg(
            func.max(
                func.when(func.col("event_type") == "purchase", 1).otherwise(0)
            ).alias("label")
        )
    )

    last_event_order = Window.partitionBy("user_session", "user_id").orderBy(
        func.col("event_number").desc()
    )
    last_prefix_events = (
        prefix_events.withColumn(
            "last_event_rank", func.row_number().over(last_event_order)
        )
        .where(func.col("last_event_rank") == 1)
        .select(
            "user_session",
            "user_id",
            func.col("event_type").alias("last_prefix_event_type"),
        )
    )

    prefix_features = prefix_events.groupBy("user_session", "user_id").agg(
        func.min("event_time").alias("prefix_start"),
        func.max("event_time").alias("prefix_end"),
        func.count("*").cast("int").alias("prefix_event_count"),
        func.sum(func.when(func.col("event_type") == "view", 1).otherwise(0))
        .cast("double")
        .alias("views_count"),
        func.sum(func.when(func.col("event_type") == "cart", 1).otherwise(0))
        .cast("double")
        .alias("carts_count"),
        func.sum(
            func.when(func.col("event_type") == "remove_from_cart", 1).otherwise(0)
        )
        .cast("double")
        .alias("remove_from_cart_count"),
        func.sum(func.when(func.col("event_type") == "purchase", 1).otherwise(0))
        .cast("int")
        .alias("prefix_purchase_count"),
        func.countDistinct("product_id").cast("double").alias("distinct_products"),
        func.countDistinct("category_id").cast("double").alias("distinct_categories"),
        func.countDistinct("brand").cast("double").alias("distinct_brands"),
        func.avg("price").cast("double").alias("avg_price"),
        func.max("price").cast("double").alias("max_price"),
        func.min("price").cast("double").alias("min_price"),
        func.max(
            func.when(
                (func.col("event_number") <= 3) & (func.col("event_type") == "cart"),
                1,
            ).otherwise(0)
        )
        .cast("double")
        .alias("cart_in_first_3_events_flag"),
    )

    dataset = (
        prefix_features.where(
            (func.col("prefix_event_count") == 10)
            & (func.col("prefix_purchase_count") == 0)
        )
        .join(last_prefix_events, ["user_session", "user_id"], "inner")
        .join(future_labels, ["user_session", "user_id"], "left")
        .join(historical_features, ["user_session", "user_id"], "left")
        .withColumn("label", func.coalesce("label", func.lit(0)).cast("double"))
        .withColumn(
            "prefix_duration_seconds",
            (
                func.unix_timestamp("prefix_end")
                - func.unix_timestamp("prefix_start")
            ).cast("double"),
        )
        .withColumn(
            "avg_time_between_events_seconds",
            safe_rate(func.col("prefix_duration_seconds"), func.lit(9.0)),
        )
        .withColumn("first_event_hour", func.hour("prefix_start").cast("double"))
        .withColumn(
            "first_event_day_of_week", func.dayofweek("prefix_start").cast("double")
        )
        .withColumn("first_event_month", func.month("prefix_start").cast("double"))
        .withColumn(
            "cart_to_view_rate",
            safe_rate(func.col("carts_count"), func.col("views_count")),
        )
        .withColumn(
            "remove_to_cart_rate",
            safe_rate(func.col("remove_from_cart_count"), func.col("carts_count")),
        )
        .withColumn(
            "last_prefix_event_type",
            func.coalesce("last_prefix_event_type", func.lit("unknown")),
        )
        .withColumn(
            "user_purchase_count_30d",
            func.coalesce("user_purchase_count_30d", func.lit(0.0)),
        )
        .withColumn(
            "user_days_since_last_session",
            func.coalesce("user_days_since_last_session", func.lit(9999.0)),
        )
        .withColumn(
            "user_purchase_rate",
            func.coalesce(func.col("user_purchase_rate"), func.lit(0.0)),
        )
    )

    dataset = add_cyclical_time_features(dataset)
    return dataset.select(*RAW_OUTPUT_COLUMNS, *NUMERIC_FEATURES[17:]).cache()


def build_preprocessing_pipeline():
    """Create reusable feature-preparation stages for both model pipelines."""
    imputed_features = [f"{feature}_imputed" for feature in NUMERIC_FEATURES]
    return Pipeline(
        stages=[
            Imputer(
                inputCols=NUMERIC_FEATURES,
                outputCols=imputed_features,
                strategy="median",
            ),
            StringIndexer(
                inputCol="last_prefix_event_type",
                outputCol="last_prefix_event_type_index",
                handleInvalid="keep",
            ),
            OneHotEncoder(
                inputCols=["last_prefix_event_type_index"],
                outputCols=["last_prefix_event_type_encoded"],
                handleInvalid="keep",
            ),
            VectorAssembler(
                inputCols=imputed_features + ["last_prefix_event_type_encoded"],
                outputCol="unscaled_features",
            ),
            StandardScaler(
                inputCol="unscaled_features",
                outputCol="features",
                withMean=False,
                withStd=True,
            ),
        ]
    )


def classifier_grid(classifier):
    """Return a compact hyperparameter grid for the selected classifier."""
    if isinstance(classifier, LogisticRegression):
        return (
            ParamGridBuilder()
            .addGrid(classifier.regParam, [0.01, 0.1])
            .addGrid(classifier.elasticNetParam, [0.0, 0.5])
            .addGrid(classifier.threshold, [0.35, 0.5])
            .build()
        )

    return (
        ParamGridBuilder()
        .addGrid(classifier.maxDepth, [5, 10])
        .addGrid(classifier.maxBins, [32, 64])
        .addGrid(classifier.subsamplingRate, [0.7, 1.0])
        .build()
    )


def train_model(train_data, classifier, evaluator):
    """Train a classifier inside a full ML pipeline with cross-validation."""
    preprocessing_stages = build_preprocessing_pipeline().getStages()
    pipeline = Pipeline(stages=preprocessing_stages + [classifier])
    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=classifier_grid(classifier),
        evaluator=evaluator,
        numFolds=3,
        parallelism=2,
        seed=SEED,
    )
    return cross_validator.fit(train_data)


def calculate_metrics(model_name, predictions):
    """Calculate binary-classification and multiclass metrics."""
    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction"
    )
    multiclass_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction"
    )
    return {
        "model": model_name,
        "areaUnderROC": binary_evaluator.setMetricName("areaUnderROC").evaluate(
            predictions
        ),
        "areaUnderPR": binary_evaluator.setMetricName("areaUnderPR").evaluate(
            predictions
        ),
        "accuracy": multiclass_evaluator.setMetricName("accuracy").evaluate(
            predictions
        ),
        "f1": multiclass_evaluator.setMetricName("f1").evaluate(predictions),
    }


def write_hdfs_outputs(spark, train_data, test_data, predictions, evaluation):
    """Write all Stage III HDFS artifacts."""
    for path in (HDFS_DATA_DIR, HDFS_MODEL_DIR, HDFS_OUTPUT_DIR):
        run_command(["hdfs", "dfs", "-mkdir", "-p", path])

    for path in (
        HDFS_TRAIN_PATH,
        HDFS_TEST_PATH,
        HDFS_MODEL1_PREDICTIONS_PATH,
        HDFS_MODEL2_PREDICTIONS_PATH,
        HDFS_EVALUATION_PATH,
    ):
        hdfs_rm(path)

    preprocessing_model = build_preprocessing_pipeline().fit(train_data)
    preprocessing_model.transform(train_data).select("features", "label").coalesce(
        1
    ).write.mode("overwrite").json(HDFS_TRAIN_PATH)
    preprocessing_model.transform(test_data).select("features", "label").coalesce(
        1
    ).write.mode("overwrite").json(HDFS_TEST_PATH)

    predictions["model1"].select("label", "prediction").coalesce(1).write.mode(
        "overwrite"
    ).option("header", "true").csv(HDFS_MODEL1_PREDICTIONS_PATH)
    predictions["model2"].select("label", "prediction").coalesce(1).write.mode(
        "overwrite"
    ).option("header", "true").csv(HDFS_MODEL2_PREDICTIONS_PATH)

    spark.createDataFrame(evaluation).coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(HDFS_EVALUATION_PATH)


def save_local_artifacts():
    """Copy required HDFS artifacts to the repository output directories."""
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    os.makedirs(LOCAL_MODEL_DIR, exist_ok=True)
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

    hdfs_getmerge(HDFS_TRAIN_PATH, f"{LOCAL_DATA_DIR}/train.json")
    hdfs_getmerge(HDFS_TEST_PATH, f"{LOCAL_DATA_DIR}/test.json")
    hdfs_getmerge(
        HDFS_MODEL1_PREDICTIONS_PATH,
        f"{LOCAL_OUTPUT_DIR}/model1_predictions.csv",
    )
    hdfs_getmerge(
        HDFS_MODEL2_PREDICTIONS_PATH,
        f"{LOCAL_OUTPUT_DIR}/model2_predictions.csv",
    )
    hdfs_getmerge(HDFS_EVALUATION_PATH, f"{LOCAL_OUTPUT_DIR}/evaluation.csv")

    hdfs_get_dir(f"{HDFS_MODEL_DIR}/model1", f"{LOCAL_MODEL_DIR}/model1")
    hdfs_get_dir(f"{HDFS_MODEL_DIR}/model2", f"{LOCAL_MODEL_DIR}/model2")


def main():
    """Run feature engineering, model training, and artifact export."""
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    events = read_events(spark)
    session_data = build_session_dataset(events)
    sample_fraction = float(os.environ.get("STAGE3_SAMPLE_FRACTION", "1.0"))
    if 0.0 < sample_fraction < 1.0:
        session_data = session_data.sample(False, sample_fraction, seed=SEED)

    train_data, test_data = session_data.randomSplit([0.7, 0.3], seed=SEED)
    train_data = train_data.cache()
    test_data = test_data.cache()
    print(f"Training rows: {train_data.count()}")
    print(f"Testing rows: {test_data.count()}")

    evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    logistic_regression = LogisticRegression(
        labelCol="label", featuresCol="features", family="binomial", maxIter=50
    )
    random_forest = RandomForestClassifier(
        labelCol="label", featuresCol="features", seed=SEED, numTrees=80
    )

    model1 = train_model(train_data, logistic_regression, evaluator)
    model2 = train_model(train_data, random_forest, evaluator)

    predictions = {
        "model1": model1.transform(test_data).cache(),
        "model2": model2.transform(test_data).cache(),
    }
    evaluation = [
        calculate_metrics("logistic_regression", predictions["model1"]),
        calculate_metrics("random_forest", predictions["model2"]),
    ]

    for path in (f"{HDFS_MODEL_DIR}/model1", f"{HDFS_MODEL_DIR}/model2"):
        hdfs_rm(path)
    model1.bestModel.write().overwrite().save(f"{HDFS_MODEL_DIR}/model1")
    model2.bestModel.write().overwrite().save(f"{HDFS_MODEL_DIR}/model2")

    write_hdfs_outputs(spark, train_data, test_data, predictions, evaluation)
    save_local_artifacts()
    spark.stop()


if __name__ == "__main__":
    main()
