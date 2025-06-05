#!/bin/bash

# 기존 Airflow 설정 제거
echo "기존 Airflow 설정을 제거합니다..."
rm -rf ~/airflow

# Airflow 환경 설정
export AIRFLOW_HOME=~/airflow

# 필요한 디렉토리 생성
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/dags

# Airflow 데이터베이스 초기화
echo "Airflow 데이터베이스를 초기화합니다..."
$HOME/Desktop/final-project/weebee/crawling_venv/bin/airflow db init

# Airflow 관리자 사용자 생성
echo "Airflow 관리자 사용자를 생성합니다..."
$HOME/Desktop/final-project/weebee/crawling_venv/bin/airflow users create \
  --username admin \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password admin

echo "Airflow 설정이 완료되었습니다."
echo "이제 './start_airflow.sh' 명령어로 Airflow를 시작할 수 있습니다."
