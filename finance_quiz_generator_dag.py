#!/usr/bin/env python3
"""
금융 퀴즈 생성기 DAG

이 DAG는 다음 작업을 수행합니다:
1. Claude API를 사용하여 금융/경제 학습용 퀴즈 생성
2. 2지선다 유형으로 퀴즈 생성
3. 기초(BRONZE), 일반(SILVER), 고급(GOLD) 난이도별로 각각 태스크 생성
4. 생성된 퀴즈를 데이터베이스에 저장
"""

import os
import json
import logging
import pymysql
import anthropic
from datetime import datetime, timedelta
from functools import wraps
from contextlib import contextmanager
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# ───────────────────────────────────────────────────────────────────────────────
# 환경 변수 및 로깅 설정
# ───────────────────────────────────────────────────────────────────────────────
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
DB_IP              = os.getenv('DB_IP')
DB_NAME            = os.getenv('DB_NAME')
DB_USERNAME        = os.getenv('DB_USERNAME')
DB_PASSWORD        = os.getenv('DB_PASSWORD')

# ───────────────────────────────────────────────────────────────────────────────
# Claude 모델 및 토큰/비용 상수
# ───────────────────────────────────────────────────────────────────────────────
MODEL_NAME       = "claude-3-5-sonnet-20241022"
PRICE_PROMPT     = 0.003 / 1000
PRICE_COMPLETION = 0.015 / 1000

MAX_TOKENS       = 8192
MAX_INPUT_TOKENS = 80000

# ───────────────────────────────────────────────────────────────────────────────
# 퀴즈 주제 및 난이도 설정
# ───────────────────────────────────────────────────────────────────────────────
# subject_id → 주제 문자열 매핑
QUIZ_SUBJECTS = {
    0: "invest",   # 투자 관련
    1: "finance",  # 금융 관련
    2: "credit"    # 신용소비 관련
}

# difficulty → 설명, level 매핑
DIFFICULTY_SETTINGS = {
    "BRONZE": {"desc": "기초 (초보자도 이해할 수 있는 수준)", "level": 50},
    "SILVER": {"desc": "중급 (기본 개념을 알고 있는 사람이 풀 수 있는 수준)", "level": 70},
    "GOLD":   {"desc": "고급 (심화 개념이나 응용력을 요구하는 수준)",       "level": 100},
}

# 퀴즈 유형 리스트
QUIZ_TYPES = ["2지선다"]  # 2지선다 유형만 생성

# 태스크 생성 순서를 위한 주제 우선순위 (finance -> invest -> credit)
SUBJECT_ORDER = [1, 0, 2]

# ───────────────────────────────────────────────────────────────────────────────
# 데이터베이스 연결 헬퍼
# ───────────────────────────────────────────────────────────────────────────────
@contextmanager
def get_db_connection():
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

# ───────────────────────────────────────────────────────────────────────────────
# 예외 처리 데코레이터
# ───────────────────────────────────────────────────────────────────────────────
def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"{func.__name__} 실행 중 오류: {e}")
            return None
    return wrapper

# ───────────────────────────────────────────────────────────────────────────────
# 토큰 수 계산 및 비용 로깅 헬퍼
# ───────────────────────────────────────────────────────────────────────────────
def count_tokens(messages, model=MODEL_NAME):
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
    prompt_cost     = input_tokens * PRICE_PROMPT
    completion_cost = output_tokens * PRICE_COMPLETION
    total_cost      = prompt_cost + completion_cost

    logger.info(f"API 비용: ${total_cost:.6f} (입력: ${prompt_cost:.6f}, 출력: ${completion_cost:.6f})")
    logger.info(f"토큰 사용량: 입력 {input_tokens}, 출력 {output_tokens}")
    return total_cost

# ───────────────────────────────────────────────────────────────────────────────
# Claude API 기반 퀴즈 생성 함수
# ───────────────────────────────────────────────────────────────────────────────
@error_handler
def generate_finance_quiz(subject_id, quiz_type="4지선다", num_quizzes=5, difficulty="BRONZE"):
    """
    Claude API를 사용한 금융/경제 퀴즈 생성 함수

    Args:
        subject_id: 퀴즈 주제 ID (0: invest, 1: finance, 2: credit)
        quiz_type:   "2지선다" 또는 "4지선다"
        num_quizzes: 생성할 퀴즈 개수 (예: 100)
        difficulty:  "BRONZE", "SILVER", "GOLD"

    Returns:
        List of quiz dicts 혹은 None
    """
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키 미설정")
        return None

    # 주제 & 난이도 설정 가져오기
    subject       = QUIZ_SUBJECTS.get(subject_id, "economy")
    diff_conf     = DIFFICULTY_SETTINGS.get(difficulty, DIFFICULTY_SETTINGS["SILVER"])
    difficulty_desc = diff_conf["desc"]
    quiz_level      = diff_conf["level"]

    system_prompt = "당신은 금융·경제 교육 콘텐츠를 생성하는 AI입니다. 학습자의 이해를 돕는 명확하고 교육적인 퀴즈를 생성합니다."

    # 선택지 형식 정의
    choice_format = """
    "choice_a": "첫 번째 선택지",
    "choice_b": "두 번째 선택지",
    "correct_ans": "1 또는 2 (정답 선택지)",
    """

    # 프롬프트 구성 (기존 로직 그대로 두되 rank/level만 동적으로 변경)
    user_prompt = f"""
    다음 조건에 맞는 금융/경제 학습을 위한 퀴즈를 생성해주세요:

    주제: {subject}
    퀴즈 유형: {quiz_type}
    난이도: {difficulty_desc}
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
                "quiz_level": {quiz_level},        // BRONZE=50 / SILVER=70 / GOLD=100
                "quiz_rank": "{difficulty}",      // "BRONZE", "SILVER", "GOLD" 중 하나
                "subject": {subject_id},
                {choice_format}
                "explanation": "정답에 대한 간단한 설명"
            }},
            // 추가 퀴즈...
        ]
    }}

    중요: 반드시 JSON 형식으로 응답해주세요. 다른 설명이나 주석은 포함하지 마세요.
    """

    # 1) 토큰 수 확인
    messages = [{"role": "user", "content": user_prompt}]
    input_tokens = count_tokens(messages)

    if input_tokens > MAX_INPUT_TOKENS:
        logger.error(f"입력 토큰 수({input_tokens})가 제한({MAX_INPUT_TOKENS})을 초과합니다.")
        return None

    # 2) Claude API 호출
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=MODEL_NAME,
            max_tokens=MAX_TOKENS,
            system=system_prompt,
            messages=messages
        )
    except Exception as e:
        logger.error(f"Claude API 호출 중 오류: {e}")
        return None

    response_text = response.content[0].text
    output_tokens = response.usage.output_tokens

    # 3) 사용량 로깅
    log_claude_usage(input_tokens, output_tokens)

    # 4) JSON 영역 추출
    json_start = response_text.find('{')
    json_end   = response_text.rfind('}') + 1

    if json_start == -1 or json_end == 0:
        logger.error("응답에서 JSON을 찾을 수 없습니다.")
        return None

    json_str = response_text[json_start:json_end]
    try:
        result = json.loads(json_str)
    except json.JSONDecodeError:
        logger.error(f"JSON 파싱 오류: {json_str[:100]}...")
        return None

    # 5) 결과 유효성 검사
    quizzes = result.get("quizzes", [])
    if not quizzes:
        logger.error("생성된 퀴즈가 없습니다.")
        return None

    logger.info(f"{difficulty} / {quiz_type} → {len(quizzes)}개 퀴즈 생성됨")
    return quizzes

# ───────────────────────────────────────────────────────────────────────────────
# 퀴즈 저장 함수 (quiz, quiz_option_2, quiz_option_4 테이블)
# ───────────────────────────────────────────────────────────────────────────────
@error_handler
def save_quizzes_to_db(quizzes, quiz_type):
    """
    생성된 퀴즈 목록을 DB에 저장

    Args:
        quizzes:    리스트 형태의 퀴즈 dict
        quiz_type:  "2지선다" 또는 "4지선다"
    """
    if not quizzes:
        logger.warning("저장할 퀴즈가 없습니다.")
        return 0

    saved_count = 0
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for quiz in quizzes:
            quiz_id = None
            try:
                # quiz 테이블에 기본 정보 삽입
                cursor.execute(
                    '''
                    INSERT INTO quiz
                    (content, quiz_level, quiz_rank, subject)
                    VALUES (%s, %s, %s, %s)
                    ''',
                    (
                        quiz["content"],
                        quiz.get("quiz_level", 50),
                        quiz.get("quiz_rank", "SILVER"),
                        quiz.get("subject", 1)
                    )
                )
                quiz_id = cursor.lastrowid

                # 선택지 테이블에 삽입 (2지선다만 사용)
                cursor.execute(
                    '''
                    INSERT INTO quiz_option_2
                    (quiz_id, choice_a, choice_b, correct_ans)
                    VALUES (%s, %s, %s, %s)
                    ''',
                    (
                        quiz_id,
                        quiz["choice_a"],
                        quiz["choice_b"],
                        quiz["correct_ans"]
                    )
                )
                saved_count += 1
            except Exception as e:
                logger.error(f"퀴즈 저장 중 오류 {'' if quiz_id is None else f'(quiz_id={quiz_id})'}: {e}")
                continue
        conn.commit()

    logger.info(f"{saved_count}개 퀴즈 저장 완료 (유형: {quiz_type})")
    return saved_count

# ───────────────────────────────────────────────────────────────────────────────
# Airflow에서 호출될 함수: 퀴즈 생성 + 저장
# ───────────────────────────────────────────────────────────────────────────────
def generate_and_save_quizzes(subject_id, quiz_type, num_quizzes, difficulty):
    """
    주어진 주제/타입/난이도로 퀴즈를 생성 → DB에 저장

    Args:
        subject_id:   int (0, 1, 2)
        quiz_type:    str ("2지선다" or "4지선다")
        num_quizzes:  int (예: 50)
        difficulty:   str ("BRONZE", "SILVER", "GOLD")
    """
    logger.info(f"작업 시작 → 주제:{subject_id}, 유형:{quiz_type}, 난이도:{difficulty}, 개수:{num_quizzes}")
    quizzes = generate_finance_quiz(
        subject_id=subject_id,
        quiz_type=quiz_type,
        num_quizzes=num_quizzes,
        difficulty=difficulty
    )
    if not quizzes:
        logger.error("퀴즈 생성 실패, 저장 단계로 넘어가지 않습니다.")
        return

    saved = save_quizzes_to_db(quizzes, quiz_type)
    logger.info(f"저장 완료 → 주제:{subject_id}, 유형:{quiz_type}, 난이도:{difficulty}, 저장 개수:{saved}")

# ───────────────────────────────────────────────────────────────────────────────
# DAG 정의
# ───────────────────────────────────────────────────────────────────────────────
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
    'weebee_quiz_generator2',
    default_args=default_args,
    description='금융/경제 2지선다 퀴즈 생성 파이프라인',
    schedule_interval=None,  # 수동 트리거만 가능
    catchup=False
)

# ───────────────────────────────────────────────────────────────────────────────
# 1) 동적으로 태스크 생성
# ───────────────────────────────────────────────────────────────────────────────
# 각 난이도별 100문제 생성
TOTAL_PER_DIFFICULTY = 100
# 모든 문제를 2지선다로 생성
SPLIT = {
    "2지선다": TOTAL_PER_DIFFICULTY
}

# 본격적으로 태스크를 담아둘 딕셔너리
tasks = {}

for subj in SUBJECT_ORDER:
    for diff in ["BRONZE", "SILVER", "GOLD"]:
        for qtype in QUIZ_TYPES:
            task_id = f"gen_{QUIZ_SUBJECTS[subj]}_{diff.lower()}_2opt"
            tasks[(subj, diff, qtype)] = PythonOperator(
                task_id=task_id,
                python_callable=generate_and_save_quizzes,
                op_kwargs={
                    'subject_id': subj,
                    'quiz_type'  : qtype,
                    'num_quizzes': SPLIT[qtype],
                    'difficulty' : diff
                },
                dag=dag
            )

# ───────────────────────────────────────────────────────────────────────────────
# 2) 생성된 태스크들 간에 의존 관계 설정
#    - 동일 주제 내: BRONZE → SILVER → GOLD 순서
#    - 주제 간: finance → invest → credit 순서
# ───────────────────────────────────────────────────────────────────────────────

for subj in SUBJECT_ORDER:
    # 해당 주제의 난이도/유형별 태스크를 순서대로 연결
    prev_task = None
    for diff in ["BRONZE", "SILVER", "GOLD"]:
        for qtype in QUIZ_TYPES:
            current_task = tasks[(subj, diff, qtype)]
            if prev_task:
                prev_task >> current_task
            prev_task = current_task

# 주제 간 연결: finance → invest → credit
# finance 주제의 마지막 태스크는 (1, "GOLD", "2지선다")
# invest  주제의 첨 태스크는  (0, "BRONZE", "2지선다")
tasks[(1, "GOLD", "2지선다")] >> tasks[(0, "BRONZE", "2지선다")]
# invest 주제의 마지막 태스크는 (0, "GOLD", "2지선다")
# credit 주제의 첨 태스크는   (2, "BRONZE", "2지선다")
tasks[(0, "GOLD", "2지선다")] >> tasks[(2, "BRONZE", "2지선다")]

# ───────────────────────────────────────────────────────────────────────────────
# (선택) __main__ 블록: 로컬에서 직접 실행 시 퀴즈 생성 테스트
# ───────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # 예시: finance / BRONZE / 2지선다 → 2개만 생성 테스트
    sample = generate_finance_quiz(subject_id=1, quiz_type="2지선다", num_quizzes=2, difficulty="BRONZE")
    if sample:
        print(f"\n생성된 기초(BRONZE) / finance 2지선다 퀴즈 ({len(sample)}개):")
        for i, q in enumerate(sample, 1):
            print(f"{i}. {q['content'][:100]}...")
        save_quizzes_to_db(sample, "2지선다")