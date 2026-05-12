mkdir -p data

echo "Downloading dataset files..."

# It somehow cannot download from those links so I have to use kaggle link with only Oct and Nov and looaded Dec to kaggle myself
# wget -O data1/2019-Oct.csv.gz "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"
# wget -O data1/2019-Nov.csv.gz "https://data.rees46.com/datasets/marketplace/2019-Nov.csv.gz"
# wget -O data1/2019-Dec.csv.gz "https://data.rees46.com/datasets/marketplace/2019-Dec.csv.gz"

# unzip data1/2019-Oct.csv.zip -d data1/
# unzip data1/2019-Nov.csv.zip -d data1/
# unzip data1/2019-Dec.csv.zip -d data1/

wget -O data/data.zip "https://www.kaggle.com/api/v1/datasets/download/mkechinov/ecommerce-behavior-data-from-multi-category-store"
wget -O data/data2.zip "https://www.kaggle.com/api/v1/datasets/download/boggoro/project-dataset"

unzip data/data.zip -d data/
unzip data/data2.zip -d data/

echo "Data collection completed"