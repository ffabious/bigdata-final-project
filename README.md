# Big Data Final Project: Ecommerce Behavior Analysis and Purchase Prediction

This repository contains an end-to-end big data pipeline for analyzing user behavior in a multi-category online store and predicting whether a user session will end with a purchase.

The project follows the full lifecycle:

`Raw CSV -> PostgreSQL -> Sqoop -> HDFS -> Hive -> EDA (SQL) -> Spark ML -> Superset Dashboard`

It is based on the report in `bigdata_report.tex`.

## Project Goals

- Analyze customer behavior by time, category, brand, session, and user segment.
- Find business opportunities (conversion timing, loyalty, drop-off points, high-value segments).
- Build a binary classifier to predict purchase outcome from early session behavior.
- Present analytical and ML results in dashboard format for decision support.

## Dataset

- **Source:** [Kaggle Ecommerce Behavior Dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- **Time range:** 2019-10-01 to 2019-12-31
- **Scale:** 177,493,585 events
- **Main events:** `view`, `cart`, `remove_from_cart`, `purchase`
- **Main raw features:** event time/type, product/category, brand, price, user, session

## Tech Stack

- PostgreSQL (staging relational store)
- Citus (listed in project architecture)
- Sqoop (PostgreSQL -> HDFS ingestion)
- HDFS (distributed storage)
- Hive + Tez (warehouse and analytics)
- PySpark + Spark ML on YARN (predictive modeling)
- Apache Superset (dashboard)

## Repository Structure

- `scripts/` - automation scripts for all project stages
- `sql/` - SQL and Hive DDL/queries
- `output/` - generated artifacts (EDA CSVs, Hive logs, Avro schema/classes)
- `figures/` - report figures exported from dashboard views
- `bigdata_report.tex` - final project report
- `main.sh` - sequential launcher for stages 1-3

## Pipeline Stages

### Stage 1: Data Collection and Ingestion

Script: `scripts/stage1.sh`

1. Download and unzip Kaggle dataset (`scripts/data_collection.sh`)
2. Create/load PostgreSQL database (`scripts/build_projectdb.py`)
3. Import PostgreSQL tables to HDFS as Avro + Snappy (`scripts/data_ingestion.sh`)

### Stage 2: Hive Preparation and EDA

Script: `scripts/stage2.sh`

1. Move Avro schemas to HDFS
2. Create Hive DB and external tables (`sql/db.hql`)
3. Build partitioned/bucketed analytical tables (`sql/create_partitioned_tables.hql`)
4. Run EDA query suite (`scripts/run_all_eda.sh`)

### Stage 3: Predictive Analytics

Script: `scripts/stage3.sh`

1. Run `scripts/model.py` with `spark-submit` on YARN
2. Train and compare:
   - Logistic Regression
   - Random Forest
3. Export model outputs and evaluation metrics

## Prerequisites

This project is configured for the Innopolis Hadoop environment and expects cluster services/endpoints used in scripts.

- Python 3
- `venv` environment with `requirements.txt`
- Hadoop CLI (`hdfs`)
- Hive Beeline client (`beeline`)
- Sqoop (`sqoop`)
- Spark (`spark-submit`) with YARN access
- Network access to:
  - PostgreSQL host: `hadoop-04.uni.innopolis.ru`
  - Hive host: `hadoop-03.uni.innopolis.ru`

Create secret files used by scripts:

- `secrets/.psql.pass` - PostgreSQL password
- `secrets/.hive.pass` - Hive password

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
mkdir -p data output secrets
```

## Run

Run all stages:

```bash
bash main.sh
```

Or run stage by stage:

```bash
bash scripts/stage1.sh
bash scripts/stage2.sh
bash scripts/stage3.sh
```

## Key Outputs

- **Stage 1**
  - HDFS warehouse data (`project/warehouse`)
  - Avro schema/classes: `output/events.avsc`, `output/events.java`
- **Stage 2**
  - EDA artifacts: `output/eda_*.csv`
  - Hive execution log: `output/hive_results.txt`
- **Stage 3**
  - Predictions: `output/model1_predictions.csv`, `output/model2_predictions.csv`
  - Metrics: `output/evaluation.csv`
  - Train/test exports: `data/train.json`, `data/test.json`
  - Saved models: `models/model1`, `models/model2`

## Main Analytical Insights

The EDA part in the report highlights six business insights:

1. Daily shopping rhythm
2. Product category performance
3. Customer segmentation
4. Brand loyalty and repeat buyers
5. Session funnel behavior
6. Weekday vs weekend purchase patterns

Report figures are available in `figures/`.

## ML Results (from report)

Model comparison on test data:

- Logistic Regression: ROC AUC `0.7662`, PR AUC `0.2627`, Accuracy `0.9249`, F1 `0.9006`
- Random Forest: ROC AUC `0.7845`, PR AUC `0.3085`, Accuracy `0.9283`, F1 `0.9001`

**Best model:** Random Forest (better ROC AUC, PR AUC, and Accuracy).

## Business Recommendations

- Prioritize campaigns during high-intent windows (especially morning and weekend morning periods).
- Protect inventory/marketing for top-performing categories.
- Target cart abandoners with recovery triggers.
- Build retention programs for high-value customers.
- Use purchase probability scores for personalized offers.

## Team

- Kirill Greshnov - ML Specialist
- Denis Troegubov - Data Engineer
- Vladimir Paskal - Data Scientist
- Albert Shammasov - Tester and Tech Writer

