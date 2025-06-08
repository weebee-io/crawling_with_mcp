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
    """뉴스 기사 본문을 정제하여 순수 기사 내용만 추출
    
    다음과 같은 불필요한 내용을 제거:    
    1. '사진=' 이후 텍스트
    2. '<편집자 주>' 및 관련 내용
    3. '기자 페이지', '원문 보기' 등의 참조 문구
    4. 기자 연락처 정보 (이메일 등)
    5. 불필요한 줄바꿈과 공백
    6. 기사 끝에 붙는 안내 문구
    """
    if not isinstance(text, str):
        return text
    
    # 1. '사진=' 이후 내용 자르기
    photo_pattern = re.compile(r'사진\s*=\s*[^\n]*')
    photo_match = photo_pattern.search(text)
    if photo_match:
        text = text[: photo_match.start()].strip()
    
    # 2. 편집자 주 제거
    editor_note_pattern = re.compile(r'<[^>]*편집[^>]*주[^>]*>.*?(?=<|$)', re.DOTALL)
    text = editor_note_pattern.sub('', text)
    
    # 3. 참조 문구 제거
    reference_patterns = [
        r'\b원문\s*보기\b.*$',
        r'\b기자\s*페이지\b.*$',
        r'(?:\d+년\s*)?\d+월\s*\d+일\s*기자\b.*$',
        r'.*\b구독과\s*응원을\s*눌러\s*주시면\b.*$',
        r'.*\b재미있는\s*종목\s*기사\s*많이\s*쓰겠습니다\b.*$',
        r'.*\b기사를\s*매번\s*빠르게\s*확인하실\s*수\s*있습니다\b.*$',
    ]
    for pattern in reference_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
    
    # 4. 이메일 주소 및 기자 정보 제거
    email_pattern = re.compile(r'\S+@\S+\.\S+\s*')
    text = email_pattern.sub('', text)
    reporter_pattern = re.compile(r'\b[가-힣]{2,3}\s*기자\b\s*$')
    text = reporter_pattern.sub('', text)
    
    # 5. 기자 바이라인 제거
    byline_pattern = re.compile(r'^[가-힣]{2,3}\s+기자\s+', re.MULTILINE)
    text = byline_pattern.sub('', text)
    
    # 6. 코인데스크/추가 광고 제거
    ad_patterns = [
        r'\[\s*코인데스크\s*\].*?(?=\n\n|비트코인|가상화폐|이더리움|$)',
        r'.*\b무료구독\b.*$',
        r'.*\b구독신청\b.*$',
        r'.*\b구독하기\b.*$',
        r'.*\b알림설정\b.*$',
        r'.*\b추천종목\b.*$',
        r'.*\b무료체험\b.*$',
    ]
    for pattern in ad_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
        
    # 7. 사이트별 특수 문구 제거
    site_keywords = [
        r'\b[가-힣]+\s+자료사진\s*=\s*.*?(?=\n|$)',  # 자료사진 출처 문구
        r'^[가-힣]+\s+\b투자투자투자\b.*?(?=\n|$)',  # 기호 반복
        r'^[가-힣]+\s+\b구독신청\b.*?(?=\n|$)',  # 구독 관련
    ]
    for pattern in site_keywords:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)

    # 8. 특수 태그 제거
    text = re.sub(r'<[^>]+>', '', text)
    
    # 9. 기사 본문 문단 정리 및 출처 제거
    lines = text.split('\n')
    cleaned_lines = []
    content_started = False
    current_paragraph = []
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:  # 빈 줄 처리
            if current_paragraph:  # 기존 문단 있으면 추가
                cleaned_lines.append(' '.join(current_paragraph))
                current_paragraph = []
            continue
            
        # 사진/자료/출처 설명 문장 스킵
        if any(term in line for term in ['사진=', '자료=', '출처=', '사진제공=']):
            continue
        
        # 기사 본문으로 판단
        if len(line) > 30 or content_started or i > 2:  # 처음 뮇줄 이후에는 본문으로 간주
            content_started = True
            
            # 마지막 문단 관련 스킵 조건
            if i >= len(lines) - 3 and any(term in line for term in ['세종=', '에디터=', '중앙일보=', '한국경제=', '연탕=']):
                continue
                
            # 이메일 주소나 기자명+기자 형태 텍스트 제외
            if '@' in line or re.search(r'[가-힣]{2,3}\s+기자', line):
                continue
                
            current_paragraph.append(line)  # 현재 문단에 추가

    # 마지막 문단 처리
    if current_paragraph:
        cleaned_lines.append(' '.join(current_paragraph))
    
    # 10. 문단형 유지하여 재구성 - 문단 사이에 줄바꿈 두 번
    text = '\n\n'.join(cleaned_lines)
    
    # 11. 문단 내 공백 정리 (문단 작업은 유지)
    text = re.sub(r'[ \t]+', ' ', text).strip()  # 가로 공백만 하나로 정리
    
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
        "https://www.hankyung.com/feed/economy",
        "https://www.hankyung.com/feed/stock",       # 주식 추가
        "https://www.hankyung.com/feed/international", # 금융 관련 국제뉴스 추가
        "https://www.hankyung.com/feed/it"           # IT경제 추가
    ]
    
    # 기간 확장: 지금으로부터 7일 전까지의 기사 포함
    now = datetime.now()
    last_saved = now - timedelta(days=7)  # 3일에서 7일로 확장
    logger.info(f"한경 RSS: 지난 7일 기준(7일 전 시각) → {last_saved}")

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

            # ④ 본문 크롤링 - 다양한 방법 시도
            import requests
            from bs4 import BeautifulSoup
            import re
            
            raw_description = ""
            success = False
            
            # 방법 1: newspaper 라이브러리 사용 (최대 3회 재시도)
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries and not success:
                try:
                    article = Article(link, language='ko')
                    article.download()
                    time.sleep(0.5)  # 다운로드 후 짧은 대기
                    article.parse()
                    raw_description = article.text.strip()
                    
                    # 내용이 충분히 있으면 성공
                    if len(raw_description) > 500:
                        logger.info(f"[한경] newspaper로 성공적으로 크롤링: {link} - 길이: {len(raw_description)}")
                        success = True
                        break
                    
                    logger.warning(f"[한경] newspaper 본문이 너무 짧음, 재시도 {retry_count+1}/3: {link} - 길이: {len(raw_description)}")
                except Exception as e:
                    logger.error(f"[한경] newspaper 크롤링 실패, 재시도 {retry_count+1}/3: {link} - {e}")
                
                retry_count += 1
                time.sleep(1)  # 재시도 전 대기
            
            # 방법 2: 한경 사이트 특화 크롤링 (newspaper가 실패한 경우)
            if not success:
                try:
                    logger.info(f"[한경] 특화 크롤링 시도: {link}")
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
                    }
                    response = requests.get(link, headers=headers, timeout=10)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 한경 기사 본문 추출 - 여러 형태의 구조 체크
                    article_body = None
                    
                    # 한경 뉴스 사이트의 다양한 레이아웃 구조에 대응
                    # 다양한 CSS 선택자로 본문 영역 찾기
                    selectors = [
                        '.article-body', '#articletxt', '.text-article', 
                        '.article-text', '.article-view-content', '.article-body-content',
                        '.news-body-text', '#articleBody', '#newsView', '#newsContent',
                        '.cont_art', '.art_cont', '.article_cont', '.atc-content',
                        '#article_body', '.article_view_contents', '#adiContents'
                    ]
                    
                    # 각 선택자를 순회하며 본문 찾기
                    article_body = None
                    for selector in selectors:
                        article_body = soup.select_one(selector)
                        if article_body and len(article_body.get_text().strip()) > 200:
                            logger.info(f"[한경] 선택자 '{selector}'로 본문 발견")
                            break
                    
                    # 선택자로 본문을 찾지 못한 경우
                    if not article_body or len(article_body.get_text().strip()) < 200:
                        logger.warning(f"[한경] 선택자로 기사 본문을 찾지 못함: {link}")
                        
                        # 대안 방법 1: 긴 글이 있는 p 태그 추출
                        paragraphs = soup.select('p')
                        valid_paragraphs = [p.get_text().strip() for p in paragraphs 
                                          if p.get_text().strip() and len(p.get_text().strip()) > 50]
                        
                        if valid_paragraphs and len('\n\n'.join(valid_paragraphs)) > 300:
                            raw_description = '\n\n'.join(valid_paragraphs)
                            success = True
                            logger.info(f"[한경] p 태그 추출로 성공: {link} - 길이: {len(raw_description)}")
                        else:
                            # 대안 방법 2: 긴 텍스트 블록 찾기
                            content_blocks = []
                            for tag in soup.select('div, article, section'):
                                text = tag.get_text().strip()
                                if len(text) > 300 and not any(term in text.lower() for term in ['cookie', 'javascript', 'copyright']):
                                    content_blocks.append(text)
                            
                            if content_blocks:
                                # 가장 긴 텍스트 블록 선택
                                longest_block = max(content_blocks, key=len)
                                raw_description = longest_block
                                success = True
                                logger.info(f"[한경] 긴 텍스트 블록 추출 성공: {link} - 길이: {len(raw_description)}")
                    if article_body:
                        # 기사 본문 영역 찾기 성공
                        logger.info(f"[한경] 본문 영역 발견: {link} - HTML 길이: {len(str(article_body))}")
                        
                        # 한국경제 특수 처리: br 태그를 줄바꿈으로 대체
                        # 1. article_body 내의 HTML 문자열 가져오기
                        html_content = str(article_body)
                        
                        # <br> 또는 <br/> 태그를 줄바꿈으로 변환
                        html_content = re.sub(r'<br\s*/?>\s*<br\s*/?>|<br\s*/>', '\n', html_content)
                        
                        # 수정된 HTML로 다시 파싱
                        soup_body = BeautifulSoup(html_content, 'html.parser')
                        
                        # 사용하지 않는 요소 제거 (캡션, 광고, 사진 설명 등)
                        for tag in soup_body.select('figcaption, .figure-caption, .ad_wrap, .ad_box, .caption, aside'):
                            tag.decompose()
                        
                        # 문단 추출 전략 1: 직접 br 태그가 있는 텍스트 블록 찾기
                        main_text_blocks = []
                        for text_block in soup_body.findAll(text=True):
                            if not text_block.parent.name in ['style', 'script', 'head', 'meta']:
                                # 텍스트가 충분히 길고 의미 있는 경우만 추가
                                cleaned_text = text_block.strip()
                                if len(cleaned_text) > 30:
                                    main_text_blocks.append(cleaned_text)
                        
                        if main_text_blocks and len('\n\n'.join(main_text_blocks)) > 300:
                            raw_description = '\n\n'.join(main_text_blocks)
                            success = True
                            logger.info(f"[한경] 텍스트 블록 추출 성공: {link} - 길이: {len(raw_description)}")
                            
                        # 문단 추출 전략 2: p 태그 중심으로 추출
                        if not success or len(raw_description) < 300:
                            paragraphs = soup_body.select('p')
                            if paragraphs and len(paragraphs) >= 2:
                                paragraph_texts = [p.get_text().strip() for p in paragraphs 
                                                if p.get_text().strip() and len(p.get_text().strip()) > 20]
                                raw_description = '\n\n'.join(paragraph_texts)
                                
                                if len(raw_description) > 300:
                                    success = True
                                    logger.info(f"[한경] 기사 p 태그 추출 성공: {link} - 길이: {len(raw_description)}")
                        
                        # 문단 추출 전략 3: 전체 텍스트에서 줄바꿈으로 구분하기
                        if not success or len(raw_description) < 300:
                            # 전체 텍스트 가져오기
                            full_text = soup_body.get_text().strip()
                            
                            # 줄바꿈으로 문단 구분
                            paragraphs = [p.strip() for p in re.split(r'\n+', full_text) if p.strip()]
                            valid_paragraphs = [p for p in paragraphs if len(p) > 20]
                            
                            if valid_paragraphs:
                                raw_description = '\n\n'.join(valid_paragraphs)
                                success = True
                                logger.info(f"[한경] 전체 텍스트 문단 분할 성공: {link} - 길이: {len(raw_description)}")
                            else:
                                # 마지막 수단: 전체 텍스트 그대로 사용
                                raw_description = full_text
                                if len(raw_description) > 200:
                                    success = True
                                    logger.info(f"[한경] 전체 텍스트 사용: {link} - 길이: {len(raw_description)}")

                        # 이미 위 코드에서 success 설정이 완료되었음
                except Exception as e:
                    logger.error(f"[한경] 특화 크롤링 실패: {link} - {e}")
            
            # 방법 3: 비상용 - 둘 다 실패하면 RSS에서 제공한 요약문 사용
            if not success and summary and len(summary) > 100:
                logger.warning(f"[한경] 크롤링 실패, RSS 요약문 사용: {link}")
                raw_description = summary
                success = True
            
            # 여전히 실패하면 스킵
            if not success or len(raw_description) < 50:
                logger.error(f"[한경] 본문 크롤링 완전 실패: {link}")
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
    MAX_KOR = 50
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
        "https://www.cnbc.com/id/10000115/device/rss/rss.html",  # Business
        "https://www.cnbc.com/id/19854910/device/rss/rss.html",  # Investing
        "https://www.cnbc.com/id/10000946/device/rss/rss.html",  # Markets
        "https://www.cnbc.com/id/20409666/device/rss/rss.html",  # Real Estate
        "https://www.cnbc.com/id/15839069/device/rss/rss.html",  # Technology
        "https://www.cnbc.com/id/15839135/device/rss/rss.html"   # Finance/Banks
    ]

    # 항상 “지금으로부터 7일 전”을 기준으로 삼는다
    now = datetime.now()
    last_saved = now - timedelta(days=7)
    logger.info(f"CNBCRSS: 지난 7일 기준(7일 전 시각) → {last_saved}")

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

            # ③ 본문 크롤링 - newspaper 라이브러리 시도
            max_retries = 3
            retry_count = 0
            raw_content_en = ""
            success = False
            
            # 1. newspaper 라이브러리 시도 (최대 3회)
            while retry_count < max_retries and not success:
                try:
                    article = Article(link, language='en')
                    article.download()
                    time.sleep(0.5)  # 다운로드 후 짧은 대기
                    article.parse()
                    raw_content_en = article.text.strip()
                    
                    # 내용이 충분히 있으면 성공
                    if len(raw_content_en) > 500:  # 더 긴 내용을 요구 (500자 이상)
                        success = True
                        logger.info(f"[CNBC] newspaper 크롤링 성공: {link} - 길이: {len(raw_content_en)}")
                    else:
                        logger.warning(f"[CNBC] 본문이 너무 짧음, 재시도 {retry_count+1}/3: {link} - 길이: {len(raw_content_en)}")
                except Exception as e:
                    logger.error(f"[CNBC] newspaper 크롤링 실패, 재시도 {retry_count+1}/3: {link} - {e}")
                
                retry_count += 1
                time.sleep(1)  # 재시도 전 대기
            
            # 2. newspaper 실패 시 BeautifulSoup 직접 크롤링 시도
            if not success:
                logger.info(f"[CNBC] BeautifulSoup 크롤링 시도: {link}")
                try:
                    import requests
                    from bs4 import BeautifulSoup
                    import re
                    
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                    response = requests.get(link, headers=headers, timeout=10)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # CNBC 기사 구조에 맞는 선택자들 시도
                    selectors = [
                        '.ArticleBody-articleBody', '.group', '.article-main', '.article-text',
                        '.article__content', '.article-body__content', '.ArticleBody-subtitle',
                        '.RenderKeyPoints-list', '.ArticleBody-wrapper', '.content', 
                        '[data-module="ArticleBody"]', '.cnbc-body'
                    ]
                    
                    article_body = None
                    # 각 선택자 시도
                    for selector in selectors:
                        elements = soup.select(selector)
                        if elements:
                            for element in elements:
                                text = element.get_text().strip()
                                if len(text) > 300:  # 충분히 긴 텍스트면 채택
                                    article_body = element
                                    logger.info(f"[CNBC] 선택자 '{selector}'로 본문 발견")
                                    break
                        if article_body:
                            break
                    
                    if article_body:
                        logger.info(f"[CNBC] 본문 영역 발견: {link} - HTML 길이: {len(str(article_body))}")
                        
                        # br 태그를 줄바꿈으로 처리하기 위한 특수 처리
                        html_content = str(article_body)
                        html_content = re.sub(r'<br\s*/?>\s*<br\s*/?>|<br\s*/>', '\n', html_content)
                        
                        # 수정된 HTML 다시 파싱
                        soup_body = BeautifulSoup(html_content, 'html.parser')
                        
                        # 사용하지 않는 요소 제거 (캡션, 광고, 사진 설명 등)
                        for tag in soup_body.select('figcaption, .figure-caption, .ad, .inline-media-container, .caption, aside'):
                            tag.decompose()
                        
                        # 문단 추출 1: 텍스트 블록 찾기
                        main_text_blocks = []
                        for text_block in soup_body.findAll(text=True):
                            if not text_block.parent.name in ['style', 'script', 'head', 'meta']:
                                cleaned_text = text_block.strip()
                                if len(cleaned_text) > 40 and not any(term in cleaned_text.lower() for term in ['cookie', 'subscribe', 'newsletter', 'copyright', 'rights reserved']):
                                    main_text_blocks.append(cleaned_text)
                        
                        if main_text_blocks and len('\n\n'.join(main_text_blocks)) > 300:
                            raw_content_en = '\n\n'.join(main_text_blocks)
                            success = True
                            logger.info(f"[CNBC] 텍스트 블록 추출 성공: {link} - 길이: {len(raw_content_en)}")
                        
                        # 문단 추출 2: p 태그 중심으로 추출
                        if not success or len(raw_content_en) < 300:
                            paragraphs = soup_body.select('p')
                            if paragraphs and len(paragraphs) >= 2:
                                paragraph_texts = [p.get_text().strip() for p in paragraphs 
                                                 if p.get_text().strip() and len(p.get_text().strip()) > 20]
                                raw_content_en = '\n\n'.join(paragraph_texts)
                                
                                if len(raw_content_en) > 300:
                                    success = True
                                    logger.info(f"[CNBC] p 태그 추출 성공: {link} - 길이: {len(raw_content_en)}")
                        
                        # 문단 추출 3: 전체 텍스트 처리
                        if not success or len(raw_content_en) < 300:
                            # 전체 텍스트에서 줄바꿈 처리
                            full_text = soup_body.get_text().strip()
                            paragraphs = [p.strip() for p in re.split(r'\n+', full_text) if p.strip()]
                            valid_paragraphs = [p for p in paragraphs if len(p) > 20]
                            
                            if valid_paragraphs:
                                raw_content_en = '\n\n'.join(valid_paragraphs)
                                success = True
                                logger.info(f"[CNBC] 전체 텍스트 줄바꿈 분할 성공: {link} - 길이: {len(raw_content_en)}")
                    
                    # 본문 영역을 찾지 못한 경우 긴 p 태그들 추출 시도
                    if not success:
                        paragraphs = soup.select('p')
                        valid_paragraphs = [p.get_text().strip() for p in paragraphs 
                                          if p.get_text().strip() and len(p.get_text().strip()) > 50]
                        
                        if valid_paragraphs and len('\n\n'.join(valid_paragraphs)) > 300:
                            raw_content_en = '\n\n'.join(valid_paragraphs)
                            success = True
                            logger.info(f"[CNBC] 모든 p 태그 추출 성공: {link} - 길이: {len(raw_content_en)}")
                        else:
                            # 마지막 방법: 긴 텍스트 블록 찾기
                            content_blocks = []
                            for tag in soup.select('div, article, section'):
                                text = tag.get_text().strip()
                                if len(text) > 300 and not any(term in text.lower() for term in ['cookie', 'javascript', 'copyright']):
                                    content_blocks.append(text)
                            
                            if content_blocks:
                                longest_block = max(content_blocks, key=len)
                                raw_content_en = longest_block
                                success = True
                                logger.info(f"[CNBC] 긴 텍스트 블록 추출 성공: {link} - 길이: {len(raw_content_en)}")
                
                except Exception as e:
                    logger.error(f"[CNBC] BeautifulSoup 크롤링 실패: {link} - {e}")
            
            # 3. 모든 시도 후에도 내용이 없으면 RSS 요약으로 대체
            if not success or len(raw_content_en) < 100:
                # RSS 피드의 요약문 사용 (더 이상의 fallback)
                if summary_en and len(summary_en) > 50:
                    raw_content_en = summary_en
                    logger.warning(f"[CNBC] 크롤링 실패로 RSS 요약문 사용: {link} - 길이: {len(raw_content_en)}")
                else:
                    logger.error(f"[CNBC] 본문 크롤링 최종 실패: {link}")
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
    MAX_ENG = 50
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
    # 금융 키워드 목록 (finance_terms) 가져오기
    finance_terms = [
        "금융", "증권", "주식", "ETF", "채권",
        "대출", "대차", "수익", "투자", "가상자산",
        "비트코인", "코스피", "코스닥", "국채", "원자재",
        "부동산"
    ]
    
    # 본문 길이 기준을 50자로 완화하여 짧은 기사도 처리합니다.
    if not description or len(description) < 50:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 기본값으로 처리합니다. (길이: {len(description) if description else 0})")
        # 짧은 본문에도 기본 요약과 키워드 제공
        return {
            "summary": f"{title}. 이 기사는 금융/경제와 관련된 정보를 담고 있습니다.",
            "keywords": "금융, 투자, 시장"
        }

    prompt = (
        f"""
        당신은 금융/경제 뉴스 분석 전문가입니다.
        다음 기사의 금융/경제 교육적 가치를 분석하고, 인사이트를 얻을 수 있는 핵심 내용을 정리해주세요.
    
        기사 제목: {title}\n\n
        기사 내용: {description[:10000]}\n\n
        
        다음 작업을 진행해주세요:
        1. 이 기사가 경제, 금융, 투자 등에 실질적으로 영향을 미치는 내용을 담고 있는지 평가해주세요.
        2. 금융 지식이 부족한 사람도 이해할 수 있도록 기사 내용을 불릿포인트 형식으로 명확하게 요약해주세요.
        3. 이 기사에서 가장 핵심적인 금융/경제 키워드를 3-5개 추출해주세요. 키워드는 쉼표(,)로 구분된 문자열 형태로 작성해주세요.
        
        키워드는 반드시 다음 목록 중에서 1개 이상을 포함해야 합니다: {', '.join(finance_terms)}
    
        반드시 다음 JSON 형식으로 응답해주세요:
        {{
            "summary": "기사 요약",
            "keywords": "키워드1, 키워드2, 키워드3"
        }}
    
        중요: 오직 JSON 형식만 제공해주세요. 다른 텍스트나 설명은 포함하지 마세요.
    """
    )

    result = call_claude_api(prompt, max_tokens=1000, temperature=0.0)
    if not result or not isinstance(result, dict):
        logger.error(f"기사 ID {news_id}: Claude 응답이 없거나 JSON 형식이 아닙니다.")
        return {
            "summary": f"{title}. 이 기사는 금융/경제와 관련된 중요한 정보를 다루고 있습니다.", 
            "keywords": "금융, 투자, 시장"
        }

    # Claude가 돌려준 값을 처리
    summary = result.get("summary", "")
    keywords = result.get("keywords", "")

    # 응답이 적절한 형태가 아닌 경우 기본값으로 대체
    if not isinstance(summary, str) or not summary:
        logger.warning(f"기사 ID {news_id}: summary 필드가 비어있거나 문자열이 아닙니다 → 기본값으로 설정")
        summary = f"{title}. 이 기사는 금융/경제와 관련된 중요한 정보를 다루고 있습니다."
    
    # keywords가 리스트인 경우 문자열로 변환
    if isinstance(keywords, list):
        keywords = ", ".join(keywords)
    
    # keywords가 문자열이 아니거나 비어있는 경우 기본값 설정
    if not isinstance(keywords, str) or not keywords:
        logger.warning(f"기사 ID {news_id}: keywords 필드가 비어있거나 문자열이 아닙니다 → 기본값으로 설정")
        keywords = "금융, 투자, 시장"
        
    # 키워드에 finance_terms의 항목이 하나도 없으면 추가
    if not any(term in keywords for term in finance_terms):
        logger.warning(f"기사 ID {news_id}: keywords에 finance_terms 항목이 없습니다 → 추가")
        keywords = f"{keywords}, 금융, 투자"

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
                     processed["keywords"],  # 이미 문자열 형태로 변경했으므로 그대로 저장
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