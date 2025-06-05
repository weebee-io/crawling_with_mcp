import os
import sys
import logging
from datetime import datetime, timedelta

import pandas as pd  # 반드시 추가해야 합니다
from airflow import DAG
from airflow.operators.python import PythonOperator

# news_etl.py 모듈을 로드하기 위해 현재 디렉토리를 파이썬 패스에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 로그 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airflow.news_etl_dag")

# news_etl 모듈에서 필요한 함수들을 임포트
from news_etl import (
    fetch_kor_rss_news,
    fetch_eng_rss_news,
    save_news_to_database,
    generate_and_save_quizzes
)

# ===========================
# DAG 기본 설정
# ===========================
default_args = {
    'owner': 'weebee',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 27)
}

dag = DAG(
    'news_etl_dag',
    default_args=default_args,
    description='금융/경제 뉴스 ETL 파이프라인 (최적화 버전)',
    schedule_interval='0 */12 * * *',  # 12시간마다 실행
    catchup=False,
    tags=['news', 'etl', 'finance'],
)

# ===========================
# 1) collect_and_save_news_task 함수 정의
# ===========================
def collect_and_save_news_task(**kwargs):
    """
    1) 한경/영문 뉴스 동시 수집
    2) DB에 저장
    3) XCom에 새로 저장된 news_id 목록 전달
    """
    logger.info("뉴스 수집 시작")

    # 1. 한경 RSS & CNBC RSS 동시 수집
    kor_df = fetch_kor_rss_news()
    eng_df = fetch_eng_rss_news()

    # DataFrame 결합 및 중복 제거
    if kor_df is None:
        kor_df = pd.DataFrame()
    if eng_df is None:
        eng_df = pd.DataFrame()

    if not kor_df.empty or not eng_df.empty:
        combined = pd.concat([kor_df, eng_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['url'])
    else:
        combined = pd.DataFrame()

    # 새로운 기사 없으면 XCom에 빈 리스트만 전달
    if combined.empty:
        logger.info("신규 뉴스가 없어 DB 저장 생략")
        kwargs['ti'].xcom_push(key='news_ids', value=[])
        return []

    # 2. DB 저장
    news_ids = save_news_to_database(combined)
    logger.info(f"DB에 저장된 뉴스 ID 개수: {len(news_ids)}")
    kwargs['ti'].xcom_push(key='news_ids', value=news_ids)
    return news_ids

# ===========================
# 2) quiz_generation_task 함수 정의
# ===========================
def quiz_generation_task(**kwargs):
    """
    XCom으로부터 news_id 목록을 받아 퀴즈 생성 및 저장 호출
    """
    ti = kwargs['ti']
    news_ids = ti.xcom_pull(task_ids='collect_and_save_news', key='news_ids') or []
    if not news_ids:
        logger.info("퀴즈 생성 대상 뉴스가 없습니다.")
        return

    logger.info(f"{len(news_ids)}개 기사에 대해 퀴즈 생성 시작")
    generate_and_save_quizzes(news_ids)
    logger.info("퀴즈 생성 완료")

# ===========================
# 3) PythonOperator 생성
# ===========================
collect_and_save_news = PythonOperator(
    task_id='collect_and_save_news',
    python_callable=collect_and_save_news_task,
    provide_context=True,  # Airflow 2.x 이하에서 컨텍스트(**kwargs) 전달을 위해 필요
    dag=dag
)

generate_quizzes = PythonOperator(
    task_id='generate_quizzes',
    python_callable=quiz_generation_task,
    provide_context=True,
    dag=dag
)

# ===========================
# 4) 태스크 의존성 설정
# ===========================
collect_and_save_news >> generate_quizzes
