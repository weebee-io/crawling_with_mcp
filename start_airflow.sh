#!/bin/bash

# Airflow 환경 설정
export AIRFLOW_HOME=~/airflow

# 디렉토리 생성
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/dags

# Airflow 초기화
$HOME/Desktop/final-project/weebee/crawling_venv/bin/airflow db init

# Airflow 사용자 생성 (처음 실행시 주석 해제)
# $HOME/Desktop/final-project/weebee/crawling_venv/bin/airflow users create \
#   --username admin \
#   --firstname Admin \
#   --lastname Admin \
#   --role Admin \
#   --email admin@example.com \
#   --password admin

# Airflow 웹서버 시작
nohup $HOME/Desktop/final-project/weebee/crawling_ve
nv/bin/airflow webserver --port 8080 &> $AIRFLOW_HOME/logs/webserver.log &
echo "Airflow 웹서버가 시작되었습니다. 로그는 $AIRFLOW_HOME/logs/webserver.log 에서 확인할 수 있습니다."

# Airflow 스케줄러 시작
nohup $HOME/Desktop/final-project/weebee/crawling_venv/bin/airflow scheduler &> $AIRFLOW_HOME/logs/scheduler.log &
echo "Airflow 스케줄러가 시작되었습니다. 로그는 $AIRFLOW_HOME/logs/scheduler.log 에서 확인할 수 있습니다."

echo "Airflow UI에 접속할 수 있습니다."
