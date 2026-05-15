import os
import re
import json
import time
import sqlite3
import concurrent.futures
from datetime import datetime

import feedparser
import requests
import anthropic
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template
from dotenv import load_dotenv

load_dotenv(override=True)

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "ssbr.db")

# ── 검색 쿼리 목록 ──────────────────────────────────────────────────
SEARCH_QUERIES = [
    "SSBR rubber tire 2025",
    "solution styrene butadiene rubber production",
    "functionalized SSBR silica coupling",
    "electric vehicle EV tire low rolling resistance",
    "tire silica compound performance polymer",
    "EU tire labeling regulation 2025",
    "ETRMA tire sustainability carbon",
    "Michelin Bridgestone Continental tire innovation material",
    "wet grip tire performance new material",
    "synthetic rubber chemical polymer technology",
]


# ── SQLite 초기화 ───────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at      TEXT    NOT NULL,
            collected_count INTEGER NOT NULL DEFAULT 0,
            analyzed_count  INTEGER NOT NULL DEFAULT 0,
            elapsed_seconds REAL    NOT NULL DEFAULT 0,
            market_trend    TEXT,
            top_keywords    TEXT,
            key_insight     TEXT
        );

        CREATE TABLE IF NOT EXISTS articles (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id       INTEGER NOT NULL REFERENCES sessions(id),
            title            TEXT,
            source           TEXT,
            published        TEXT,
            link             TEXT,
            relevance_score  INTEGER,
            core_keywords    TEXT,
            summary          TEXT,
            business_insight TEXT
        );
    """)
    conn.commit()
    conn.close()


# ── DB 저장 ─────────────────────────────────────────────────────────
def save_session(created_at, collected_count, analyzed_count, elapsed_seconds,
                 session_summary, articles):
    if not isinstance(session_summary, dict):
        session_summary = {}
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute(
            """INSERT INTO sessions
               (created_at, collected_count, analyzed_count, elapsed_seconds,
                market_trend, top_keywords, key_insight)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                created_at,
                collected_count,
                analyzed_count,
                elapsed_seconds,
                session_summary.get("market_trend", ""),
                json.dumps(session_summary.get("top_keywords", []), ensure_ascii=False),
                session_summary.get("key_insight", ""),
            ),
        )
        session_id = cur.lastrowid

        for art in articles:
            conn.execute(
                """INSERT INTO articles
                   (session_id, title, source, published, link,
                    relevance_score, core_keywords, summary, business_insight)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    art.get("title", ""),
                    art.get("source", ""),
                    art.get("published", ""),
                    art.get("link", "#"),
                    art.get("relevance_score", 0),
                    json.dumps(art.get("core_keywords", []), ensure_ascii=False),
                    art.get("summary", ""),
                    art.get("business_insight", ""),
                ),
            )

        conn.commit()
        return session_id
    finally:
        conn.close()


# ── DB 조회 ─────────────────────────────────────────────────────────
def get_sessions():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """SELECT id, created_at, collected_count, analyzed_count,
                      elapsed_seconds, market_trend, top_keywords, key_insight
               FROM sessions ORDER BY id DESC LIMIT 50"""
        ).fetchall()
        result = []
        for r in rows:
            top_kw = []
            try:
                top_kw = json.loads(r["top_keywords"] or "[]")
            except Exception:
                pass
            result.append({
                "id": r["id"],
                "created_at": r["created_at"],
                "collected_count": r["collected_count"],
                "analyzed_count": r["analyzed_count"],
                "elapsed_seconds": r["elapsed_seconds"],
                "market_trend": r["market_trend"],
                "top_keywords": top_kw,
                "key_insight": r["key_insight"],
            })
        return result
    finally:
        conn.close()


def get_session_detail(session_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        session = conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if not session:
            return None

        top_kw = []
        try:
            top_kw = json.loads(session["top_keywords"] or "[]")
        except Exception:
            pass

        arts = conn.execute(
            """SELECT * FROM articles WHERE session_id = ?
               ORDER BY relevance_score DESC""",
            (session_id,),
        ).fetchall()

        article_list = []
        for a in arts:
            kw = []
            try:
                kw = json.loads(a["core_keywords"] or "[]")
            except Exception:
                pass
            article_list.append({
                "id": a["id"],
                "title": a["title"],
                "source": a["source"],
                "published": a["published"],
                "link": a["link"],
                "relevance_score": a["relevance_score"],
                "core_keywords": kw,
                "summary": a["summary"],
                "business_insight": a["business_insight"],
            })

        return {
            "id": session["id"],
            "created_at": session["created_at"],
            "collected_count": session["collected_count"],
            "analyzed_count": session["analyzed_count"],
            "elapsed_seconds": session["elapsed_seconds"],
            "market_trend": session["market_trend"],
            "top_keywords": top_kw,
            "key_insight": session["key_insight"],
            "articles": article_list,
        }
    finally:
        conn.close()


# ── 키워드 트렌드 조회 ──────────────────────────────────────────────
def get_keyword_trend(limit=10):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        sessions = conn.execute(
            "SELECT id, created_at, top_keywords FROM sessions ORDER BY id DESC LIMIT 10"
        ).fetchall()

        trend = []
        for s in sessions:
            kw = []
            try:
                kw = json.loads(s["top_keywords"] or "[]")
            except Exception:
                pass
            trend.append({"created_at": s["created_at"], "keywords": kw})
        return trend
    finally:
        conn.close()


# ── 뉴스 수집 ──────────────────────────────────────────────────────
def fetch_google_news(query: str, max_results: int = 8) -> list[dict]:
    try:
        encoded = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        articles = []
        for entry in feed.entries[:max_results]:
            title = entry.get("title", "").strip()
            if not title:
                continue
            raw_summary = entry.get("summary", "")
            clean_summary = BeautifulSoup(raw_summary, "html.parser").get_text()[:600]
            articles.append({
                "title": title,
                "link": entry.get("link", "#"),
                "source": entry.get("source", {}).get("title", "Google News"),
                "published": entry.get("published", ""),
                "summary": clean_summary,
            })
        return articles
    except Exception as e:
        print(f"[WARN] fetch error for '{query}': {e}")
        return []


def collect_all_articles() -> list[dict]:
    all_articles: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_google_news, q): q for q in SEARCH_QUERIES}
        for future in concurrent.futures.as_completed(futures):
            try:
                all_articles.extend(future.result())
            except Exception as e:
                print(f"[WARN] thread error: {e}")

    seen: set[str] = set()
    unique: list[dict] = []
    for a in all_articles:
        key = a["title"][:100].lower()
        if key not in seen:
            seen.add(key)
            unique.append(a)
    return unique


# ── Claude 분석 ────────────────────────────────────────────────────
SYSTEM_PROMPT = """당신은 타이어 소재 산업 및 고기능성 합성고무(SSBR) 시장 동향 분석에 특화된 수석 AI 비즈니스 애널리스트입니다.
당신의 목표는 글로벌 뉴스를 분석하여 '양말단 SSBR(Functionalized SSBR)'의 매출 확대와 차세대 제품 개발에 직결되는 핵심 인사이트를 도출하는 것입니다."""

USER_PROMPT_TEMPLATE = """아래 기사 목록을 전부 분석하여 양말단 SSBR 비즈니스와의 연관성 점수를 매기고 모든 기사를 반환하십시오.

**연관성 점수 기준 (0~100점):**
- 90~100점: 시장 니즈, 규제 동향, 경쟁 동향, 차세대 기술 중 2개 이상 직접 부합
- 70~89점: 위 기준 중 1개 강하게 부합 (EV 타이어, Low Rolling Resistance, Wet Grip, 실리카 등)
- 40~69점: SSBR 또는 타이어 소재 산업에 간접적으로 연관
- 0~39점: 연관성 낮음 (일반 자동차, 제조업 등)

**연관성 항목:**
- 시장 니즈: 전기차(EV) 타이어, Low Rolling Resistance, Wet Grip, 내마모성 요구사항
- 규제 동향: 유럽/북미 환경 규제, 탄소 배출 저감 정책, 타이어 라벨링 제도 강화
- 경쟁 동향: 화학사의 SSBR 생산 능력 확대, 기능성 고무 신제품 개발 및 특허 출원
- 차세대 기술: 실리카 친화성 향상, 폴리머 구조 제어, 양말단 기능화 기술 동향

**분석 대상 기사 ({count}건):**
{articles}

**출력 형식 (JSON만 출력, 다른 텍스트 없음):**
{{
  "session_summary": {{
    "market_trend": "이번 분석에서 나타난 전반적인 시장 동향 2문장 요약 (한국어)",
    "top_keywords": ["키워드1", "키워드2", "키워드3", "키워드4", "키워드5"],
    "key_insight": "양말단 SSBR 비즈니스 관점 핵심 시사점 1문장 (한국어)"
  }},
  "articles": [
    {{
      "title": "기사 원문 제목 (번역하지 말 것)",
      "source": "출처명",
      "relevance_score": 85,
      "core_keywords": ["키워드1", "키워드2", "키워드3"],
      "summary": "SSBR 비즈니스 관점의 핵심 내용 3문장 이내 요약 (한국어)",
      "business_insight": "양말단 SSBR 매출 확대 또는 차세대 제품 개발 시사점과 HPM 사업부 대응 방안 2문장 이내 (한국어)"
    }}
  ]
}}

모든 기사를 빠짐없이 포함하십시오. 반드시 JSON 형식만 출력하십시오."""


def analyze_with_claude(articles: list[dict]) -> dict:
    client = anthropic.Anthropic()

    articles_text = "\n\n".join(
        f"[{i+1}] 제목: {a['title']}\n출처: {a['source']}\n날짜: {a['published']}\n내용: {a['summary']}"
        for i, a in enumerate(articles[:20])
    )

    prompt = USER_PROMPT_TEMPLATE.format(count=len(articles[:20]), articles=articles_text)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=16000,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            break
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < max_retries - 1:
                wait = 10 * (attempt + 1)
                print(f"[WARN] API 과부하 (529), {wait}초 후 재시도... ({attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                raise

    response_text = message.content[0].text.strip()
    print(f"[DEBUG] Claude 응답 길이: {len(response_text)}자, stop_reason: {message.stop_reason}")

    response_text = re.sub(r"```(?:json)?\s*", "", response_text).strip()

    start = response_text.find("{")
    end = response_text.rfind("}") + 1
    if start == -1 or end == 0:
        print("[WARN] JSON 구조를 찾을 수 없음")
        return {"session_summary": {}, "articles": []}

    try:
        parsed = json.loads(response_text[start:end])
        print(f"[DEBUG] 파싱된 기사 수: {len(parsed.get('articles', []))}")
    except json.JSONDecodeError as e:
        print(f"[WARN] JSON parse error: {e}")
        print(f"[WARN] Raw response (앞 500자): {response_text[:500]}")
        print(f"[WARN] Raw response (뒤 200자): {response_text[-200:]}")
        return {"session_summary": {}, "articles": []}

    # 원본 링크 및 날짜 재매핑
    title_map = {a["title"][:100].lower(): a for a in articles}
    for art in parsed.get("articles", []):
        key = art.get("title", "")[:100].lower()
        original = title_map.get(key, {})
        art["link"] = original.get("link", "#")
        art["published"] = original.get("published", "")

    return parsed


# ── 라우트 ──────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/collect", methods=["POST"])
def collect():
    try:
        t0 = time.time()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        articles = collect_all_articles()
        collected_count = len(articles)

        if not articles:
            return jsonify({"error": "뉴스를 수집할 수 없습니다. 네트워크 상태를 확인하세요."}), 503

        result = analyze_with_claude(articles)
        filtered = result.get("articles", [])
        session_summary = result.get("session_summary", {})
        elapsed = round(time.time() - t0, 1)

        session_id = save_session(
            created_at=now,
            collected_count=collected_count,
            analyzed_count=len(filtered),
            elapsed_seconds=elapsed,
            session_summary=session_summary,
            articles=filtered,
        )

        return jsonify({
            "session_id": session_id,
            "articles": filtered,
            "session_summary": session_summary,
            "stats": {
                "collected": collected_count,
                "filtered": len(filtered),
                "elapsed": elapsed,
                "timestamp": now,
            },
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        if isinstance(e, anthropic.AuthenticationError):
            return jsonify({"error": "Anthropic API 키가 유효하지 않습니다. .env 파일의 ANTHROPIC_API_KEY를 확인하세요."}), 401
        return jsonify({"error": f"서버 오류: {str(e)}"}), 500


@app.route("/api/history", methods=["GET"])
def history():
    try:
        sessions = get_sessions()
        return jsonify({"sessions": sessions})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/history/<int:session_id>", methods=["GET"])
def history_detail(session_id):
    try:
        detail = get_session_detail(session_id)
        if not detail:
            return jsonify({"error": "이력을 찾을 수 없습니다."}), 404
        return jsonify(detail)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/history/<int:session_id>", methods=["DELETE"])
def history_delete(session_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM articles WHERE session_id = ?", (session_id,))
        conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# gunicorn 등 외부 실행 환경에서도 DB 초기화 보장
init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
