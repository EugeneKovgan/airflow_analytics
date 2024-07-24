# docker-airflow
docker-compose up -d
docker-compose ps

<!-- change dags list -->
docker-compose restart webserver scheduler

<!-- change -->
docker-compose down
docker-compose build
docker-compose up -d

<!-- restart -->
docker-compose down
docker-compose up -d

# wsl windows
wsl --install
wsl --update

<!-- reinstall dependencies -->
pip install -r requirements.txt

<!-- start -->
source airflow_venv/bin/activate
airflow scheduler

source airflow_venv/bin/activate
airflow webserver --port 8081

<!-- quick restart -->
pkill -f airflow
airflow webserver --port 8081 &
airflow scheduler &