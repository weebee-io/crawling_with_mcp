export AIRFLOW_VERSION="2.6.3"
export PYTHON_VERSION="3.11"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Airflow와 Postgres·Celery·S3 연동용 패키지 설치
pip install "apache-airflow[postgres,amazon,celery,redis]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
