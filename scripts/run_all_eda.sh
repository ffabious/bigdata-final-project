echo "Running all EDA queries"

bash scripts/eda_queries/run_eda_1.sh
bash scripts/eda_queries/run_eda_2.sh
bash scripts/eda_queries/run_eda_3.sh
bash scripts/eda_queries/run_eda_4.sh
bash scripts/eda_queries/run_eda_5.sh
bash scripts/eda_queries/run_eda_6.sh
bash scripts/eda_queries/run_eda_7.sh

echo "All EDA queries completed"

echo "Generated CSV files:"
ls -la output/*.csv