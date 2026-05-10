USE team18_projectdb;

DROP TABLE IF EXISTS ml_evaluation;
CREATE EXTERNAL TABLE ml_evaluation (
    accuracy DOUBLE,
    areaUnderPR DOUBLE,
    areaUnderROC DOUBLE,
    f1 DOUBLE,
    model STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/evaluation'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS model1_predictions;
CREATE EXTERNAL TABLE model1_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/model1_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS model2_predictions;
CREATE EXTERNAL TABLE model2_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/model2_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP VIEW IF EXISTS ml_model_metrics_long;
CREATE VIEW ml_model_metrics_long AS
SELECT model, 'accuracy' AS metric, accuracy AS value
FROM ml_evaluation
UNION ALL
SELECT model, 'areaUnderPR' AS metric, areaUnderPR AS value
FROM ml_evaluation
UNION ALL
SELECT model, 'areaUnderROC' AS metric, areaUnderROC AS value
FROM ml_evaluation
UNION ALL
SELECT model, 'f1' AS metric, f1 AS value
FROM ml_evaluation;

DROP VIEW IF EXISTS model1_confusion_matrix;
CREATE VIEW model1_confusion_matrix AS
SELECT
    CAST(label AS INT) AS actual_label,
    CAST(prediction AS INT) AS predicted_label,
    COUNT(*) AS records
FROM model1_predictions
GROUP BY CAST(label AS INT), CAST(prediction AS INT);

DROP VIEW IF EXISTS model2_confusion_matrix;
CREATE VIEW model2_confusion_matrix AS
SELECT
    CAST(label AS INT) AS actual_label,
    CAST(prediction AS INT) AS predicted_label,
    COUNT(*) AS records
FROM model2_predictions
GROUP BY CAST(label AS INT), CAST(prediction AS INT);

DROP VIEW IF EXISTS model_prediction_distribution;
CREATE VIEW model_prediction_distribution AS
SELECT
    'logistic_regression' AS model,
    CAST(prediction AS INT) AS prediction,
    COUNT(*) AS records
FROM model1_predictions
GROUP BY CAST(prediction AS INT)
UNION ALL
SELECT
    'random_forest' AS model,
    CAST(prediction AS INT) AS prediction,
    COUNT(*) AS records
FROM model2_predictions
GROUP BY CAST(prediction AS INT);

DROP VIEW IF EXISTS model_actual_distribution;
CREATE VIEW model_actual_distribution AS
SELECT
    CAST(label AS INT) AS label,
    COUNT(*) AS records
FROM model1_predictions
GROUP BY CAST(label AS INT);

DROP VIEW IF EXISTS ml_best_model;
CREATE VIEW ml_best_model AS
SELECT model, areaUnderROC, areaUnderPR, accuracy, f1
FROM ml_evaluation
ORDER BY areaUnderROC DESC
LIMIT 1;

SELECT * FROM ml_evaluation;
SELECT * FROM ml_model_metrics_long;
SELECT * FROM model1_confusion_matrix;
SELECT * FROM model2_confusion_matrix;
SELECT * FROM model_prediction_distribution;
SELECT * FROM ml_best_model;
