#!/usr/bin/env python3
"""
금융 퀴즈 생성기 DAG

이 DAG는 다음 작업을 수행합니다:
1. Claude API를 사용하여 금융/경제 학습용 퀴즈 생성
2. 2지선다 및 4지선다 유형으로 퀴즈 분류
3. 생성된 퀴즈를 데이터베이스에 저장
"""

import os
import json
import logging
import pymysql
import anthropic
import pandas as pd
from datetime import datetime, timedelta
from functools import wraps
from contextlib import contextmanager
from dotenv import load_dotenv

# 환경 변수 설정
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)
logger = logging.getLogger(__name__)

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# API 키 및 데이터베이스 설정
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
DB_IP = os.getenv('DB_IP')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Claude 모델 설정
MODEL_NAME = "claude-3-5-sonnet-20241022"
PRICE_PROMPT = 0.003 / 1000
PRICE_COMPLETION = 0.015 / 1000

# 토큰 제한
MAX_TOKENS = 8192
MAX_INPUT_TOKENS = 80000

# 퀴즈 주제 정의
QUIZ_SUBJECTS = {
    0: "invest",    # 투자 관련
    1: "finance",   # 금융 관련
    2: "credit"     # 신용소비 관련
}

# 유틸리티 함수

@contextmanager
def get_db_connection():
    """데이터베이스 연결 관리 함수"""
    conn = None
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        yield conn
    except Exception as e:
        logger.error(f"데이터베이스 연결 오류: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def error_handler(func):
    """예외 처리 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"{func.__name__} 실행 중 오류: {e}")
            return None
    return wrapper

def count_tokens(messages, model=MODEL_NAME):
    """메시지 토큰 수 계산 함수"""
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키 미설정")
        return 0
    
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.count_tokens(
            model=model,
            messages=messages
        )
        return response.input_tokens
    except Exception as e:
        logger.error(f"토큰 계산 오류: {e}")
        return 0

def log_claude_usage(input_tokens, output_tokens, task="quiz-generation"):
    """토큰 사용량 및 비용 계산 함수"""
    prompt_cost = input_tokens * PRICE_PROMPT
    completion_cost = output_tokens * PRICE_COMPLETION
    total_cost = prompt_cost + completion_cost
    
    logger.info(f"API 비용: ${total_cost:.6f} (입력: ${prompt_cost:.6f}, 출력: ${completion_cost:.6f})")
    logger.info(f"토큰 사용량: 입력 {input_tokens}, 출력 {output_tokens}")
    
    return total_cost

@error_handler
def generate_finance_quiz(subject_id, quiz_type="4지선다", num_quizzes=5):
    """
    Claude API를 사용한 금융/경제 퀴즈 생성 함수
    
    Args:
        subject_id: 퀴즈 주제 ID (0: 투자, 1: 금융, 2: 신용)
        quiz_type: 퀴즈 유형 (2지선다 또는 4지선다)
        num_quizzes: 생성할 퀴즈 개수
    
    Returns:
        생성된 퀴즈 목록
    """
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키 미설정 - 'ANTHROPIC_API_KEY' 환경변수 필요")
        return None
    
    subject = QUIZ_SUBJECTS.get(subject_id, "경제 기초")
    
    system_prompt = "당신은 금융·경제 교육 콘텐츠를 생성하는 AI입니다. 학습자의 이해를 돕는 명확하고 교육적인 퀴즈를 생성합니다."
    
    if quiz_type == "2지선다":
        choice_format = """
        "choice_a": "첫 번째 선택지",
        "choice_b": "두 번째 선택지",
        "correct_ans": "1 또는 2 (정답 선택지)",
        """
    else:  # 4지선다
        choice_format = """
        "choice_a": "첫 번째 선택지",
        "choice_b": "두 번째 선택지",
        "choice_c": "세 번째 선택지",
        "choice_d": "네 번째 선택지",
        "correct_ans": "1, 2, 3, 4 중 정답 선택지",
        """
    
    user_prompt = f"""
    다음 조건에 맞는 금융/경제 학습을 위한 퀴즈를 생성해주세요:
    
    주제: {subject}
    퀴즈 유형: {quiz_type}
    난이도: 기초 (초보자도 이해할 수 있는 수준)
    퀴즈 수: {num_quizzes}개
    
    각 퀴즈는 다음 조건을 만족해야 합니다:
    1. 명확한 질문과 선택지
    2. 초보자가 이해할 수 있는 난이도
    3. 금융/경제 기초 개념에 대한 이해를 검증하는 내용
    4. 학습자에게 유용한 개념을 포함
    
    다음 JSON 형식으로 응답해주세요:
    {{
        "quizzes": [
            {{
                "content": "퀴즈 질문",
                "quiz_level": 50, // BRONZE 문제는 50으로 고정
                "quiz_rank": "BRONZE", // BRONZE, SILVER, GOLD 중 하나
                "subject": {subject_id},
                {choice_format}
                "explanation": "정답에 대한 간단한 설명"
            }},
            // 추가 퀴즈...
        ]
    }}
    
    중요: 반드시 JSON 형식으로 응답해주세요. 다른 설명이나 주석은 포함하지 마세요.
    """
    
    try:
        # 토큰 수 계산
        messages = [
            {"role": "user", "content": user_prompt}
        ]
        input_tokens = count_tokens(messages)
        
        if input_tokens > MAX_INPUT_TOKENS:
            logger.error(f"입력 토큰 수({input_tokens})가 제한({MAX_INPUT_TOKENS})을 초과합니다.")
            return None
        
        # Claude API 호출
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=MODEL_NAME,
            max_tokens=MAX_TOKENS,
            system=system_prompt,
            messages=messages
        )
        
        # 응답 처리
        response_text = response.content[0].text
        output_tokens = response.usage.output_tokens
        
        # 사용량 로깅
        log_claude_usage(input_tokens, output_tokens)
        
        # JSON 추출
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        
        if json_start == -1 or json_end == 0:
            logger.error("응답에서 JSON을 찾을 수 없습니다.")
            return None
        
        json_str = response_text[json_start:json_end]
        
        try:
            result = json.loads(json_str)
            
            # 퀴즈 데이터 유효성 검사
            if "quizzes" not in result or not result["quizzes"]:
                logger.error("생성된 퀴즈가 없습니다.")
                return None
                
            quizzes = result["quizzes"]
            logger.info(f"{len(quizzes)}개의 퀴즈를 생성했습니다.")
            return quizzes
            
        except json.JSONDecodeError:
            logger.error(f"JSON 파싱 오류: {json_str[:100]}...")
            return None
    
    except Exception as e:
        logger.error(f"퀴즈 생성 중 오류: {e}")
        return None

@error_handler
def save_quizzes_to_db(quizzes):
    """
    퀴즈 데이터베이스 저장 함수
    
    Args:
        quizzes: 저장할 퀴즈 리스트
    
    Returns:
        저장된 퀴즈 개수
    """
    if not quizzes:
        logger.warning("저장할 퀴즈 없음")
        return 0
        
    saved_count = 0
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        for quiz in quizzes:
            try:
                # 1. quiz 테이블에 삽입
                cursor.execute('''
                    INSERT INTO quiz (content, quiz_level, quiz_rank, subject)
                    VALUES (%s, %s, %s, %s)
                ''', (
                    quiz["content"],
                    quiz.get("quiz_level", 50),  # BRONZE 문제는 50으로 고정
                    quiz.get("quiz_rank", "BRONZE"),
                    quiz.get("subject", 1)  # 기본값은 finance(1)
                ))
                
                # 삽입된 퀴즈 ID 가져오기
                quiz_id = cursor.lastrowid
                
                # 2. 퀴즈 유형에 따라 적절한 옵션 테이블에 저장
                if "choice_c" in quiz and "choice_d" in quiz:  # 4지선다
                    cursor.execute('''
                        INSERT INTO quiz_option_4 
                        (quiz_id, choice_a, choice_b, choice_c, choice_d, correct_ans)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (
                        quiz_id,
                        quiz["choice_a"],
                        quiz["choice_b"],
                        quiz["choice_c"],
                        quiz["choice_d"],
                        quiz["correct_ans"]
                    ))
                else:  # 2지선다
                    cursor.execute('''
                        INSERT INTO quiz_option_2
                        (quiz_id, choice_a, choice_b, correct_ans)
                        VALUES (%s, %s, %s, %s)
                    ''', (
                        quiz_id,
                        quiz["choice_a"],
                        quiz["choice_b"],
                        quiz["correct_ans"]
                    ))
                
                saved_count += 1
                logger.debug(f"퀴즈 ID {quiz_id} 저장 완료")
                
            except Exception as e:
                logger.error(f"퀴즈 저장 중 오류: {e}")
                # 개별 퀴즈 저장 실패가 전체 처리를 막지 않도록 함
                continue
                
        conn.commit()
    
    logger.info(f"{saved_count}개 퀴즈 저장 완료")
    return saved_count

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'finance_quiz_generator',
    default_args=default_args,
    description='금융/경제 퀴즈 생성 파이프라인',
    schedule_interval=None,  # 수동 트리거만 가능
    catchup=False
)

def generate_and_save_quizzes(subject_id, quiz_type, num_quizzes):
    """퀴즈 생성 및 저장 함수"""
    logger.info(f"주제 ID {subject_id}에 대한 {quiz_type} 퀴즈 {num_quizzes}개 생성 시작")
    
    # 퀴즈 생성
    quizzes = generate_finance_quiz(
        subject_id=subject_id,
        quiz_type=quiz_type,
        num_quizzes=num_quizzes
    )
    
    if not quizzes:
        logger.error(f"주제 ID {subject_id}에 대한 퀴즈 생성 실패")
        return
    
    # 퀴즈 저장
    saved_count = save_quizzes_to_db(quizzes)
    logger.info(f"주제 ID {subject_id}에 대한 {saved_count}개 퀴즈 저장 완료")
    return saved_count

# 각 주제마다 2지선다 20문제, 4지선다 20문제를 생성

# finance(금융) 주제 - 4지선다 퀴즈 생성
task_generate_finance_4options = PythonOperator(
    task_id='generate_finance_4options',
    python_callable=generate_and_save_quizzes,
    op_kwargs={
        'subject_id': 1,  # finance(금융) 관련 문제
        'quiz_type': '4지선다',
        'num_quizzes': 20  # 한 번에 20개 생성
    },
    dag=dag
)

# finance(금융) 주제 - 2지선다 퀴즈 생성
task_generate_finance_2options = PythonOperator(
    task_id='generate_finance_2options',
    python_callable=generate_and_save_quizzes,
    op_kwargs={
        'subject_id': 1,  # finance(금융) 관련 문제
        'quiz_type': '2지선다',
        'num_quizzes': 20  # 한 번에 20개 생성
    },
    dag=dag
)

# invest(투자) 주제 - 4지선다 퀴즈 생성
task_generate_invest_4options = PythonOperator(
    task_id='generate_invest_4options',
    python_callable=generate_and_save_quizzes,
    op_kwargs={
        'subject_id': 0,  # invest(투자) 관련 문제
        'quiz_type': '4지선다',
        'num_quizzes': 20  # 한 번에 20개 생성
    },
    dag=dag
)

# invest(투자) 주제 - 2지선다 퀴즈 생성
task_generate_invest_2options = PythonOperator(
    task_id='generate_invest_2options',
    python_callable=generate_and_save_quizzes,
    op_kwargs={
        'subject_id': 0,  # invest(투자) 관련 문제
        'quiz_type': '2지선다',
        'num_quizzes': 20  # 한 번에 20개 생성
    },
    dag=dag
)

# credit(신용소비) 주제 - 4지선다 퀴즈 생성
task_generate_credit_4options = PythonOperator(
    task_id='generate_credit_4options',
    python_callable=generate_and_save_quizzes,
    op_kwargs={
        'subject_id': 2,  # credit(신용소비) 관련 문제
        'quiz_type': '4지선다',
        'num_quizzes': 20  # 한 번에 20개 생성
    },
    dag=dag
)

# credit(신용소비) 주제 - 2지선다 퀴즈 생성
task_generate_credit_2options = PythonOperator(
    task_id='generate_credit_2options',
    python_callable=generate_and_save_quizzes,
    op_kwargs={
        'subject_id': 2,  # credit(신용소비) 관련 문제
        'quiz_type': '2지선다',
        'num_quizzes': 20  # 한 번에 20개 생성
    },
    dag=dag
)

# 태스크 순서 지정 - 순차적으로 실행
# finance -> invest -> credit 주제 순서로, 각 주제 내에서는 4지선다 -> 2지선다 순서로 실행
task_generate_finance_4options >> task_generate_finance_2options >> \
    task_generate_invest_4options >> task_generate_invest_2options >> \
    task_generate_credit_4options >> task_generate_credit_2options

if __name__ == "__main__":
    # 로컬 테스트를 위한 코드
    logging.basicConfig(level=logging.INFO)
    
    # 각 주제에 대해 퀴즈 생성 테스트
    for subject_id in [1]:  # 테스트용으로 경제 기초 주제만
        # 4지선다 퀴즈
        quizzes_4option = generate_finance_quiz(subject_id, "4지선다", 2)
        if quizzes_4option:
            print(f"\n생성된 4지선다 퀴즈 ({len(quizzes_4option)}개):")
            for idx, quiz in enumerate(quizzes_4option, 1):
                print(f"퀴즈 {idx}: {quiz['content'][:50]}...")
            
            # 저장 테스트 (실제 저장을 원할 경우 주석 해제)
            save_quizzes_to_db(quizzes_4option)
        
        # 2지선다 퀴즈
        quizzes_2option = generate_finance_quiz(subject_id, "2지선다", 1)
        if quizzes_2option:
            print(f"\n생성된 2지선다 퀴즈 ({len(quizzes_2option)}개):")
            for idx, quiz in enumerate(quizzes_2option, 1):
                print(f"퀴즈 {idx}: {quiz['content'][:50]}...")
            
            # 저장 테스트 (실제 저장을 원할 경우 주석 해제)
            save_quizzes_to_db(quizzes_2option)
