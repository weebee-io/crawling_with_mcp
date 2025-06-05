#!/usr/bin/env python3
"""News ETL Pipeline

이 스크립트는 다음 작업을 수행합니다:
1. 한경 RSS와 CNBC RSS를 통해 금융/경제 관련 뉴스 데이터 수집 (최신 기사만 추출)
2. newspaper 라이브러리를 사용하여 기사 본문 추출
3. Claude API를 사용하여 영문 뉴스 번역, 뉴스 전처리 및 퀴즈 생성
4. AWS RDS에 데이터 저장
"""
import os
import time
import json
import logging
import hashlib
import pymysql
import feedparser
import anthropic
import email.utils  # RFC2822 datetime 파싱
from datetime import datetime, timedelta
from dotenv import load_dotenv
from newspaper import Article
from contextlib import contextmanager
from functools import wraps
import pandas as pd

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 환경 변수 로드
load_dotenv()
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
DB_IP = os.getenv('DB_IP')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Anthropic 클라이언트 (전역에서 한 번만 생성)
if not ANTHROPIC_API_KEY:
    logger.error("Claude API 키가 설정되지 않았습니다.")
    CLAUDE_CLIENT = None
else:
    CLAUDE_CLIENT = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# 데이터베이스 연결 헬퍼
@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
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
            
            
import re

def clean_description(text: str) -> str:
    if not isinstance(text, str):
        return text

    pattern = re.compile(r'사진\s*=[^\n]*')
    match = pattern.search(text)
    if match:
        return text[: match.start()].strip()
    else:
        return text

# 데코레이터: 예외 발생 시 로깅
def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"{func.__name__} 실행 중 오류 발생: {e}")
            return None
    return wrapper

# 헬퍼: DB에 저장된 최신 기사 발행일 가져오기
def get_latest_saved_published_date():
    """
    DB의 news 테이블에서 가장 최신 published_date를 가져옵니다.
    MySQL이 "0000-00-00 00:00:00" 같은 문자열을 반환할 경우를 대비해
    항상 datetime 객체를 돌려주도록 보정합니다.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(published_date) AS latest_date FROM news")
            row = cursor.fetchone()
            if not row or row.get('latest_date') is None:
                # 테이블에 레코드가 하나도 없거나 NULL이라면
                return datetime(1900, 1, 1)

            latest = row['latest_date']
            # 만약 문자열로 넘어왔다면(예: "0000-00-00 00:00:00"), 강제로 아주 이른 datetime 반환
            if isinstance(latest, str):
                return datetime(1900, 1, 1)
            # 정상적인 datetime 객체라면 그대로 반환
            return latest

# Claude API 호출 헬퍼
def call_claude_api(prompt, max_tokens=1000, temperature=0.0):
    """
    Claude(Anthropic) API 호출. 이미 전역에서 생성된 CLAUDE_CLIENT를 재사용.
    """
    if CLAUDE_CLIENT is None:
        logger.error("Claude 클라이언트가 초기화되지 않았습니다.")
        return None

    try:
        message = CLAUDE_CLIENT.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=max_tokens,
            temperature=temperature,
            messages=[{"role": "user", "content": prompt}]
        )
        response_text = message.content[0].text

        # 응답에서 JSON 파싱
        json_start = response_text.find('{')
        json_end = response_text.rfind('}')
        if json_start == -1 or json_end == -1:
            logger.error("JSON 형식을 찾을 수 없습니다. 응답 일부: %s", response_text[:200])
            return None

        json_str = response_text[json_start:json_end + 1]
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            # 제어 문자 제거 후 재파싱 시도
            import re
            clean_json = re.sub(r'[\x00-\x1F\x7F]', '', json_str)
            try:
                return json.loads(clean_json)
            except Exception as e:
                logger.error("JSON 파싱 실패: %s", e)
                return None

    except Exception as e:
        logger.error(f"Claude API 호출 중 오류: {e}")
        return None

# 한국경제 RSS 수집 함수 (최신 기사만)
@error_handler
def fetch_kor_rss_news():
    """
    한경 RSS 피드를 통해 '지난 3일(72시간)' 이내에 발행된
    금융·경제 관련 기사만 모아서 반환합니다. 
    (본문 중 '사진=...' 이후 텍스트는 전부 잘라냅니다.)
    """
    import pandas as pd
    from datetime import datetime, timedelta

    rss_urls = [
        "https://www.hankyung.com/feed/finance",
        "https://www.hankyung.com/feed/realestate",
        "https://www.hankyung.com/feed/economy"
    ]

    # 항상 “지금으로부터 3일 전”을 기준으로 삼는다
    now = datetime.now()
    last_saved = now - timedelta(days=3)
    logger.info(f"한경RSS: 지난 3일 기준(3일 전 시각) → {last_saved}")

    # 필터링할 한글 키워드 (필요하다면 적절히 확장)
    finance_terms = [
        "금융", "증권", "주식", "ETF", "채권",
        "대출", "대차", "수익", "투자", "가상자산",
        "비트코인", "코스피", "코스닥", "국채", "원자재",
        "부동산"
    ]

    filtered_items = []
    for url in rss_urls:
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            logger.error(f"RSS 파싱 오류 ({url}): {e}")
            continue

        for entry in feed.entries:
            # 발행일 정보를 datetime 객체로 변환
            try:
                if entry.get('published_parsed'):
                    published_date = datetime(*entry.published_parsed[:6])
                else:
                    # 발행 정보가 없으면 건너뜀
                    continue
            except Exception:
                continue

            # ① 지난 3일보다 이전에 발행된 기사면 스킵
            if published_date < last_saved:
                continue

            title = entry.get('title', '').strip()
            link = entry.get('link', '').strip()
            summary = entry.get('summary', '').strip()

            # ② 1차 필터링: “제목+요약”에 finance_terms 중 하나라도 포함되어야 통과
            combined_text = (title + " " + summary).lower()
            if not any(term in combined_text for term in finance_terms):
                continue

            # ③ 본문 크롤링 (newspaper)
            try:
                article = Article(link, language='ko')
                article.download()
                article.parse()
                raw_description = article.text.strip()
            except Exception as e:
                logger.error(f"[한경] 본문 크롤링 실패: {link} - {e}")
                continue

            # ④ 사진 출처(“사진=…”) 제거
            description = clean_description(raw_description)

            # ⑤ 2차 필터링: “제목+요약+정제된 본문” 전체에 finance_terms 중 하나라도 포함되어야 통과
            full_text = (title + " " + summary + " " + description).lower()
            if not any(term in full_text for term in finance_terms):
                continue

            # ⑥ 해시 생성 및 저장 리스트에 추가
            article_hash = hashlib.sha1(description.encode('utf-8')).hexdigest()[:45]
            filtered_items.append({
                'created_at': now,
                'description': description,
                'published_date': published_date,
                'source': '한국경제',
                'title': title,
                'url': link,
                'summary': summary,
                'article_hash': article_hash
            })

            time.sleep(0.1)

    # (1) 중복 제거
    unique_items = []
    seen_hashes = set()
    for item in filtered_items:
        if item['article_hash'] not in seen_hashes:
            unique_items.append(item)
            seen_hashes.add(item['article_hash'])

    # (2) 최대 20개만 선택 (원하는 만큼 변경 가능)
    MAX_KOR = 20
    limited_items = unique_items[:MAX_KOR]

    df = pd.DataFrame(limited_items)
    logger.info(f"한경RSS(3일 기준) 필터링 후 수집 기사 수: {len(df)} (금융상품 관련 최대 {MAX_KOR}개)")
    return df

# CNBC RSS 수집 함수 (최신 기사만 + 번역까지)
@error_handler
def fetch_eng_rss_news():
    """
    CNBC RSS에서 '지난 3일(72시간)' 이내에 발행된 영어 금융·경제 관련 기사만
    크롤링 후 한국어 번역하여 반환합니다.
    (본문 중 '사진=...' 이후 텍스트를 삭제)
    """
    import pandas as pd
    from datetime import datetime, timedelta

    rss_urls = [
        "https://www.cnbc.com/id/20910258/device/rss/rss.html",  # Finance
        "https://www.cnbc.com/id/10000664/device/rss/rss.html",  # Economy
        "https://www.cnbc.com/id/10000115/device/rss/rss.html"   # Business
    ]

    # 항상 “지금으로부터 3일 전”을 기준으로 삼는다
    now = datetime.now()
    last_saved = now - timedelta(days=3)
    logger.info(f"CNBCRSS: 지난 3일 기준(3일 전 시각) → {last_saved}")

    # 영어 키워드 목록 (필요하다면 더 확장)
    finance_terms_en = [
        "loan", "deposit", "investment", "insurance", "fund",
        "etf", "bond", "equity", "rate", "fed", "gdp", "stock",
        "market", "interest", "mortgage", "credit", "earnings",
        "yield", "dividend", "profit", "tariff", "tax", "policy",
        "inflation", "cpi", "jobs", "recession", "unemployment",
        "central bank", "quantitative easing", "cryptocurrency",
        "bitcoin", "venture capital"
    ]

    filtered_items = []
    client = CLAUDE_CLIENT
    if client is None:
        logger.error("Claude 클라이언트 없음 → 빈 DataFrame 반환")
        return pd.DataFrame()

    for url in rss_urls:
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            logger.error(f"RSS 파싱 오류 ({url}): {e}")
            continue

        for entry in feed.entries:
            # published 파싱 (RFC2822 → datetime)
            try:
                published = email.utils.parsedate_to_datetime(entry.published)
                published_naive = published.replace(tzinfo=None)
            except Exception:
                continue

            # ① 지난 3일보다 이전 발행이라면 스킵
            if published_naive < last_saved:
                continue

            link = entry.get('link', '').strip()
            title_en = entry.get('title', '').strip()
            summary_en = entry.get('description', '').strip()

            # ② 1차 필터링: 제목+요약
            combined_title_summary = (title_en + " " + summary_en).lower()
            if not any(term in combined_title_summary for term in finance_terms_en):
                continue

            # ③ 본문 크롤링
            try:
                article = Article(link, language='en')
                article.download()
                article.parse()
                raw_content_en = article.text.strip()
            except Exception as e:
                logger.error(f"[CNBC] 본문 크롤링 실패: {link} - {e}")
                continue

            # ④ 사진 출처(“사진=...”) 제거 (한국어로 번역하지 않은 순수 영어 본문에도 혹시 포함될 수 있으므로 처리)
            content_en = clean_description(raw_content_en)

            # ⑤ 2차 필터링: 제목+요약+정제된 본문 전체
            combined_full = (title_en + " " + summary_en + " " + content_en).lower()
            if not any(term in combined_full for term in finance_terms_en):
                continue

            # ⑥ 한국어 번역
            if len(content_en) < 50:
                korean_title = title_en
                korean_content = content_en
            else:
                prompt = (
                    f"다음 영문 뉴스를 자연스럽고 정확한 한국어로 번역해 주세요.\n\n"
                    f"제목: {title_en}\n\n"
                    f"내용:\n{content_en[:5000]}\n\n"
                    f'JSON 형식으로 반환해주세요:\n'
                    f'{{"translated_title": "번역된 제목", "translated_content": "번역된 내용"}}'
                )
                result = call_claude_api(prompt, max_tokens=4000, temperature=0.0)
                if result and "translated_title" in result and "translated_content" in result:
                    korean_title = result["translated_title"]
                    korean_content = result["translated_content"]
                else:
                    logger.warning(f"[CNBC] 번역 실패 → 영어 원문 유지: {link}")
                    korean_title = title_en
                    korean_content = content_en

            article_hash = hashlib.sha1(content_en.encode('utf-8')).hexdigest()[:45]
            filtered_items.append({
                'created_at': now,
                'description': korean_content,
                'published_date': published_naive,
                'source': 'CNBC',
                'title': korean_title,
                'url': link,
                'summary': summary_en,
                'article_hash': article_hash
            })

            time.sleep(0.1)

    # (1) 중복 제거
    unique_items = []
    seen_hashes = set()
    for item in filtered_items:
        if item['article_hash'] not in seen_hashes:
            unique_items.append(item)
            seen_hashes.add(item['article_hash'])

    # (2) 최대 20개만 선택 (필요 시 변경 가능)
    MAX_ENG = 20
    limited_items = unique_items[:MAX_ENG]

    df = pd.DataFrame(limited_items)
    logger.info(f"CNBCRSS(3일 기준) 필터링 후 수집 기사 수: {len(df)} (금융상품 관련 최대 {MAX_ENG}개)")
    return df


# 뉴스 저장 함수
@error_handler
def save_news_to_database(df):
    """
    수집된 DataFrame을 DB에 저장. 중복 URL/해시 건너뜀.
    """
    if df.empty:
        logger.warning("저장할 뉴스 데이터가 없습니다.")
        return []

    inserted_news_ids = []
    skipped = 0

    with get_db_connection() as conn:
        cursor = conn.cursor()
        # DB에 이미 있는 URL/해시 불러오기
        cursor.execute("SELECT article_hash, url FROM news")
        existing = cursor.fetchall()
        existing_hashes = {row['article_hash'] for row in existing}
        existing_urls = {row['url'] for row in existing}

        for _, row in df.iterrows():
            url = row['url']
            article_hash = row['article_hash']

            if article_hash in existing_hashes or url in existing_urls:
                skipped += 1
                continue

            # DB INSERT
            try:
                cursor.execute(
                    """
                    INSERT INTO news 
                    (title, description, created_at, source, published_date, url, article_hash, summary, keywords)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row.get('title', ''),
                        row.get('description', ''),
                        row.get('created_at'),
                        row.get('source', ''),
                        row.get('published_date'),
                        row.get('url'),
                        row.get('article_hash'),
                        row.get('summary', ''),
                        ''  # 초기 keywords 빈 문자열
                    )
                )
                news_id = cursor.lastrowid
                inserted_news_ids.append(news_id)
                existing_hashes.add(article_hash)
                existing_urls.add(url)
            except Exception as e:
                logger.error(f"DB 저장 오류 (URL: {url}): {e}")
                continue

        conn.commit()
        cursor.close()

    logger.info(f"DB에 저장된 새 기사: {len(inserted_news_ids)}개 (건너뜀: {skipped}개)")
    return inserted_news_ids

# 요약/키워드 생성 함수 (Clᴀude 사용)
@error_handler
def preprocess_news_with_claude(news_id, title, description):
    """
    기사 내용을 기반으로 Claude API를 이용해 금융/경제 관련성 검증, 요약 및 키워드 추출
    """
    # 본문 길이 기준을 50자로 완화하여 짧은 기사도 시도해 봅니다.
    if not description or len(description) < 1000:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 처리할 수 없습니다. (길이: {len(description) if description else 0})")
        return {"summary": "", "keywords": []}

    prompt = (
        f"""
        당신은 금융/경제 뉴스 분석 전문가입니다.
        다음 기사의 금융/경제 교육적 가치를 분석하고, 인사이트를 얻을 수 있는 핵심 내용을 정리해주세요.
    
        기사 제목: {title}\n\n
        기사 내용: {description[:10000]}\n\n
    
        1. 먼저 이 기사가 경제, 금융, 투자 등에 실질적으로 영향을 미치는 내용을 담고 있는지 평가해주세요.
        2. 이 기사에서 금융 이해력이 부족한 사람들이 배울 수 있는 가장 핵심적인 금융/경제 키워드 3개를 추출해주세요.
        3. 금융 지식이 부족한 사람도 이해할 수 있도록 기사 내용을 불릿포인트 형식으로 명확하게 요약해주세요.
    
        키워드는 반드시 기사의 가장 핵심적인 금융/경제 용어나 개념을 명사 형태로만 생성해주세요.
    
        반드시 다음 JSON 형식으로 응답해주세요:
        {{
            "summary": "기사 요약",
            "keywords": ["키워드1", "키워드2", "키워드3"]
        }}
    
        중요: 오직 JSON 형식만 제공해주세요. 다른 텍스트나 설명은 포함하지 마세요.
    """
    )

    result = call_claude_api(prompt, max_tokens=1000, temperature=0.0)
    if not result or not isinstance(result, dict):
        logger.error(f"기사 ID {news_id}: Claude 응답이 없거나 JSON 형식이 아닙니다.")
        return {"summary": "", "keywords": []}

    # Claude가 돌려준 값을 그대로 사용
    summary = result.get("summary", "")
    keywords = result.get("keywords", [])

    # 응답이 str/리스트 형태가 아닌 경우에는 빈 값 처리
    if not isinstance(summary, str):
        logger.warning(f"기사 ID {news_id}: summary 필드가 문자열이 아닙니다 → 빈 문자열로 설정")
        summary = ""
    if not isinstance(keywords, list):
        logger.warning(f"기사 ID {news_id}: keywords 필드가 리스트가 아닙니다 → 빈 리스트로 설정")
        keywords = []

    if summary == "":
        logger.warning(f"기사 ID {news_id}: summary가 비어 있습니다.")
    if len(keywords) == 0:
        logger.warning(f"기사 ID {news_id}: keywords 리스트가 비어 있습니다.")

    return {"summary": summary, "keywords": keywords}

# 퀴즈 생성 함수 (Clᴀude 사용)
@error_handler
def generate_quiz_with_claude(news_id, title, description):
    """
    기사 내용을 기반으로 5문제 이상 객관식 퀴즈 생성.
    """
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}: 본문이 짧아 퀴즈 생성 불가")
        return None
    
    prompt = (
        f"""
        당신은 금융/경제 교육을 위한 퀴즈 전문가입니다. 다음 기사를 기반으로 은행 및 증권사의 금융상품에대한 지식이 없는 학습자들이 금융 지식을 이해할 수 있는 교육용 퀴즈를 만들어주세요.
    
        기사 제목: {title}\n\n
        기사 내용: {description[:10000]}\n\n
    
        은행 및 증권사의 금융상품에대한 지식이 없는 학습자들이 금융 지식을 이해할 수 있는 교육용 퀴즈를 만들어주세요.
        이 기사의 주요 금융/경제 지식을 이해하기 위한 객관식 퀴즈를 제작해주세요. 반드시 최소 5개 이상의 퀴즈를 제작하고, 평이한 한국어로 출제하고, 다음 형식을 정확히 따라주세요.
        다음 JSON 형식으로 응답해주세요:
        {{
            "quizzes": [
                {{
                    "newsquiz_content": "퀴즈 질문",
                    "newsquiz_choice_a": "선택지 A",
                    "newsquiz_choice_b": "선택지 B",
                    "newsquiz_choice_c": "선택지 C",
                    "newsquiz_choice_d": "선택지 D",
                    "newsquiz_correct_ans": "A", # A, B, C, D 중 하나
                    "newsquiz_score": 5,
                    "newsquiz_level": "NORMAL", # EASY, NORMAL, HARD 중 하나
                    "reason": "정답 이유 설명"
                }},
                # 추가 퀴즈...
            ]
        }}
        """
    )
    
    quiz_data = call_claude_api(prompt, max_tokens=4000, temperature=0.7)
    if not quiz_data or "quizzes" not in quiz_data:
        logger.warning(f"기사 ID {news_id}: 퀴즈 생성 실패")
        return None
    
    # 각 퀴즈의 필수 필드 보정
    required_fields = [
        "newsquiz_content", "newsquiz_choice_a", "newsquiz_choice_b", 
        "newsquiz_choice_c", "newsquiz_choice_d", "newsquiz_correct_ans", 
        "newsquiz_score", "newsquiz_level", "reason"
    ]
    
    valid_quizzes = []
    for idx, quiz in enumerate(quiz_data.get("quizzes", [])):
        for field in required_fields:
            if field not in quiz:
                # 기본값 지정
                if field == "newsquiz_score":
                    quiz[field] = 5
                elif field == "newsquiz_level":
                    quiz[field] = "NORMAL"
                elif field == "newsquiz_correct_ans":
                    quiz[field] = "A"
                else:
                    quiz[field] = ""
        
        # 정답 형식 검사
        if quiz["newsquiz_correct_ans"] not in ["A", "B", "C", "D"]:
            quiz["newsquiz_correct_ans"] = "A"

        valid_quizzes.append(quiz)

    quiz_data["quizzes"] = valid_quizzes
    return quiz_data

# 퀴즈 DB 저장 함수
@error_handler
def save_quiz_to_database(news_id, quiz_data):
    """
    생성된 퀴즈를 DB에 저장.
    """
    if not quiz_data or "quizzes" not in quiz_data:
        return 0

    saved_count = 0
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for quiz in quiz_data["quizzes"]:
            try:
                cursor.execute(
                    """
                    INSERT INTO news_quiz 
                    (news_id, newsquiz_content, newsquiz_choice_a, newsquiz_choice_b,
                     newsquiz_choice_c, newsquiz_choice_d, newsquiz_correct_ans,
                     newsquiz_score, newsquiz_level, reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        news_id,
                        quiz["newsquiz_content"],
                        quiz["newsquiz_choice_a"],
                        quiz["newsquiz_choice_b"],
                        quiz["newsquiz_choice_c"],
                        quiz["newsquiz_choice_d"],
                        quiz["newsquiz_correct_ans"],
                        quiz["newsquiz_score"],
                        quiz["newsquiz_level"],
                        quiz["reason"]
                    )
                )
                saved_count += 1
            except Exception as e:
                logger.error(f"퀴즈 저장 오류: {e}")
                continue

        conn.commit()
        cursor.close()

    logger.info(f"기사 ID {news_id}: {saved_count}개 퀴즈 저장 완료")
    return saved_count

# 요약·키워드 + 퀴즈 생성 후 저장 루틴
@error_handler
def generate_and_save_quizzes(news_ids):
    """
    뉴스 ID 리스트에 대해 요약·키워드 추출 및 퀴즈 생성 → DB 저장.
    """
    if not news_ids:
        logger.warning("처리할 뉴스 ID가 없습니다.")
        return

    with get_db_connection() as conn:
        cursor = conn.cursor()
        processed_cnt = 0
        quiz_cnt = 0

        for news_id in news_ids:
            cursor.execute("SELECT title, description FROM news WHERE news_id = %s", (news_id,))
            row = cursor.fetchone()
            if not row:
                continue

            title = row['title']
            description = row['description']

            # 1) 요약 + 키워드
            processed = preprocess_news_with_claude(news_id, title, description)
            if processed:
                try:
                    cursor.execute(
                        "UPDATE news SET summary=%s, keywords=%s WHERE news_id=%s",
                        (processed["summary"],
                         json.dumps(processed["keywords"], ensure_ascii=False),  # ← 한글 그대로 저장
                         news_id)
                    )

                    conn.commit()
                    processed_cnt += 1
                except Exception as e:
                    logger.error(f"기사 ID {news_id} 요약/키워드 업데이트 오류: {e}")

            # 2) 퀴즈 생성 (최소 5문제)
            quiz_data = generate_quiz_with_claude(news_id, title, description)
            retry = 0
            while (not quiz_data or len(quiz_data.get("quizzes", [])) < 5) and retry < 2:
                logger.warning(f"기사 ID {news_id} 퀴즈 부족, 재시도 {retry+1}")
                time.sleep(1)
                quiz_data = generate_quiz_with_claude(news_id, title, description)
                retry += 1

            if quiz_data:
                saved = save_quiz_to_database(news_id, quiz_data)
                quiz_cnt += saved

            time.sleep(1)  # API 제한 방지

        cursor.close()

    logger.info(f"요약·키워드 {processed_cnt}개 처리, 퀴즈 {quiz_cnt}개 생성 완료")

# 메인 ETL 프로세스
def main():
    logger.info("=== ETL 시작 ===")
    try:
        kor_df = fetch_kor_rss_news()
        eng_df = fetch_eng_rss_news()

        combined = pd.concat([kor_df, eng_df], ignore_index=True) if not kor_df.empty or not eng_df.empty else pd.DataFrame()
        combined = combined.drop_duplicates(subset=['url']) if not combined.empty else combined

        if combined.empty:
            logger.warning("수집된 신규 뉴스가 없습니다.")
            return

        news_ids = save_news_to_database(combined)
        if news_ids:
            generate_and_save_quizzes(news_ids)

        logger.info("=== ETL 완료 ===")
    except Exception as e:
        logger.error(f"ETL 전체 오류: {e}")

if __name__ == "__main__":
    main()