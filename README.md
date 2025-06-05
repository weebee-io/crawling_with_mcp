````markdown
# 📊 금융·경제 뉴스 ETL 파이프라인

> **개요**:  
> 한경RSS(국문)와 CNBCRSS(영문)에서 지난 3일 이내의 금융·경제 뉴스를 수집하여  
> • 본문 크롤링 → 잡음 제거 → 저장  
> • 영문 기사 한국어 번역 → 저장  
> • Claude API로 요약·핵심 키워드(3개) 추출 → 저장  
> • Claude API로 객관식 퀴즈(5문제 이상) 생성 → 저장  
> 이후 결과를 MySQL(AWS RDS)에 적재하고, Airflow DAG을 통해 12시간마다 자동 실행됩니다.

---

## 🔑 핵심 기능

1. **RSS 동시 수집 (한경RSS + CNBCRSS)**  
   - 최근 72시간 내 발행된 기사만 필터링  
   - “금융·경제” 키워드 포함 여부 2단계 검사 (제목·요약 → 본문)  
   - 최대 20개씩 수집

2. **본문 정제 (`clean_description`)**  
   - `newspaper` 라이브러리로 본문 크롤링  
   - 정규표현식으로 “사진=…” 이후 텍스트 제거  
   - 잡음 없는 순수 기사 본문 저장

3. **영문 기사 번역 (Claude API)**  
   - 본문 50자 이상 시 JSON 형식 번역 요청  
   - 번역 실패 시 원문 유지

4. **요약·키워드 추출 (Claude API)**  
   - 금융·경제 전문가 역할  
   - 핵심 용어 3개(명사) + 이해하기 쉬운 불릿 요약 (JSON)

5. **객관식 퀴즈 생성 (Claude API)**  
   - 최소 5문제 이상 생성  
   - “은행·증권사 금융상품” 학습자 대상  
   - JSON 포맷 → DB 적재

6. **MySQL 저장 (AWS RDS 권장)**  
   - **news** 테이블:  
     ```sql
     CREATE TABLE news (
       news_id INT AUTO_INCREMENT PRIMARY KEY,
       title VARCHAR(512) NOT NULL,
       description TEXT NOT NULL,
       created_at DATETIME NOT NULL,
       source VARCHAR(20) NOT NULL,        -- '한국경제' 또는 'CNBC'
       published_date DATETIME NOT NULL,
       url VARCHAR(1024) NOT NULL,
       article_hash VARCHAR(50) NOT NULL,
       summary TEXT,
       keywords JSON,
       UNIQUE(article_hash), UNIQUE(url)
     );
     ```
   - **news_quiz** 테이블:  
     ```sql
     CREATE TABLE news_quiz (
       quiz_id INT AUTO_INCREMENT PRIMARY KEY,
       news_id INT NOT NULL,
       newsquiz_content TEXT NOT NULL,
       newsquiz_choice_a VARCHAR(255) NOT NULL,
       newsquiz_choice_b VARCHAR(255) NOT NULL,
       newsquiz_choice_c VARCHAR(255) NOT NULL,
       newsquiz_choice_d VARCHAR(255) NOT NULL,
       newsquiz_correct_ans CHAR(1) NOT NULL,
       newsquiz_score INT NOT NULL,
       newsquiz_level ENUM('EASY','NORMAL','HARD') NOT NULL,
       reason TEXT,
       FOREIGN KEY(news_id) REFERENCES news(news_id)
     );
     ```

7. **Airflow DAG (12시간마다 실행)**  
   - **collect_and_save_news** → **generate_quizzes** 순서  
   - XCom을 통해 `news_id` 목록 공유  
   - 실패 시 5분 후 1회 재시도  

---

## 🏗️ 아키텍처

```mermaid
flowchart LR
    A[Airflow Scheduler<br>(12시간마다)] --> B(collect_and_save_news)
    B --> C1(fetch_kor_rss_news)
    B --> C2(fetch_eng_rss_news)
    C1 --> D1[본문 크롤링 (한국어)]
    C1 --> D2[clean_description → 필터링]
    D2 --> E1[DB 저장 (news 테이블)]
    C2 --> F1[본문 크롤링 (영문)]
    F1 --> F2[clean_description → 필터링]
    F2 --> G1[클라우드 번역 (Claude)]
    G1 --> H1[DB 저장 (news 테이블)]
    B --> I[새로 저장된 news_id 리스트 → XCom]
    I --> J(generate_quizzes)
    J --> K1[preprocess_news_with_claude → 요약·키워드]
    K1 --> L1[DB 업데이트 (summary, keywords)]
    J --> K2[generate_quiz_with_claude → 객관식 퀴즈]
    K2 --> L2[DB 저장 (news_quiz 테이블)]
````

---

## 📁 코드 구조

```
.
├── news_etl.py 
│   ├─ clean_description()  
│   ├─ fetch_kor_rss_news()  
│   ├─ fetch_eng_rss_news()  
│   ├─ save_news_to_database()  
│   ├─ preprocess_news_with_claude()  
│   ├─ generate_quiz_with_claude()  
│   └─ generate_and_save_quizzes()
├── news_etl_dag.py        

---

## ⚙️ 설치 및 실행

### 1. 환경 준비

* **Python 3.8+**
* **MySQL 5.7+** (AWS RDS 추천)
* **Airflow 2.x**
* **Claude API 키 (Anthropic)**

### 2. 프로젝트 클론 & 의존성 설치

```bash
git clone https://github.com/<your-org>/news-etl-pipeline.git
cd news-etl-pipeline
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. 환경 변수 설정

* 프로젝트 루트에 `.env` 파일 생성
* `.env.example` 참고하여 입력

  ```
  ANTHROPIC_API_KEY=<your_claude_api_key>
  DB_IP=<your_db_endpoint>
  DB_NAME=<your_db_name>
  DB_USERNAME=<your_db_user>
  DB_PASSWORD=<your_db_password>
  ```

### 4. 데이터베이스 세팅

1. MySQL에 아래 스키마 생성

   ```sql
   -- news 테이블
   CREATE TABLE news (
     news_id INT AUTO_INCREMENT PRIMARY KEY,
     title VARCHAR(512) NOT NULL,
     description TEXT NOT NULL,
     created_at DATETIME NOT NULL,
     source VARCHAR(20) NOT NULL,
     published_date DATETIME NOT NULL,
     url VARCHAR(1024) NOT NULL,
     article_hash VARCHAR(50) NOT NULL,
     summary TEXT,
     keywords JSON,
     UNIQUE(article_hash), UNIQUE(url)
   );
   -- news_quiz 테이블
   CREATE TABLE news_quiz (
     quiz_id INT AUTO_INCREMENT PRIMARY KEY,
     news_id INT NOT NULL,
     newsquiz_content TEXT NOT NULL,
     newsquiz_choice_a VARCHAR(255) NOT NULL,
     newsquiz_choice_b VARCHAR(255) NOT NULL,
     newsquiz_choice_c VARCHAR(255) NOT NULL,
     newsquiz_choice_d VARCHAR(255) NOT NULL,
     newsquiz_correct_ans CHAR(1) NOT NULL,
     newsquiz_score INT NOT NULL,
     newsquiz_level ENUM('EASY','NORMAL','HARD') NOT NULL,
     reason TEXT,
     FOREIGN KEY(news_id) REFERENCES news(news_id)
   );
   ```

### 5. ETL 스크립트 수동 실행

```bash
# (가상환경 활성화 필요)
python news_etl.py
```

* 로그에서 “한경RSS 수집: n개”, “CNBCRSS 수집: m개”, “DB 저장: k개” 등 확인
* MySQL에 뉴스·요약·키워드·퀴즈 자동 적재

### 6. Airflow DAG 등록

1. `news_etl.py`와 `news_etl_dag.py`를 Airflow DAG 폴더로 복사

   ```bash
   cp news_etl.py news_etl_dag.py ~/airflow/dags/
   ```
2. Airflow Web UI(`localhost:8080`) 접속 → `news_etl_dag` 활성화
3. 스케줄러가 매 12시간마다 DAG 실행

   * 수동 트리거: “Trigger DAG” 클릭

---

## 📑 요약

* **주기**: 12시간마다 자동 실행
* **범위**: 지난 3일(72시간) 이내 금융·경제 뉴스 20개 (국문+영문)
* **데이터 흐름**: RSS → 크롤링 → 정제 → 번역 → 요약·키워드 추출 → 퀴즈 생성 → MySQL 저장
* **기술 스택**: Python, Airflow, feedparser, newspaper, Anthropic Claude, MySQL(AWS RDS)

---