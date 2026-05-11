from __future__ import print_function

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("team18 - print saved model params")
    .master("yarn")
    .getOrCreate()
)

models = [
    ("logistic_regression", "project/models/model1"),
    ("random_forest", "project/models/model2"),
]

for model_name, model_path in models:
    model = PipelineModel.load(model_path)
    classifier = model.stages[-1]

    print("")
    print("=== {} ===".format(model_name))
    for param, value in classifier.extractParamMap().items():
        if param.parent == classifier.uid:
            print("{}: {}".format(param.name, value))

spark.stop()
