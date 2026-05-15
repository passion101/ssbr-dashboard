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
app.config["TEMPLATES_AUTO_RELOAD"] = True

DB_PATH = os.path.join(os.path.dirname(__file__), "ssbr.db")

# ── 고객사 정의 ────────────────────────────────────────────────────
CUSTOMERS = {
    "Hankook":     {"kor": "한국타이어", "color": "#f97316"},
    "Kumho":       {"kor": "금호타이어",  "color": "#3b82f6"},
    "Nexen":       {"kor": "넥센타이어",  "color": "#10b981"},
    "Michelin":    {"kor": "Michelin",   "color": "#eab308"},
    "Bridgestone": {"kor": "Bridgestone","color": "#ef4444"},
    "Continental": {"kor": "Continental","color": "#8b5cf6"},
}

# ── 소재 정의 ──────────────────────────────────────────────────────
MATERIALS = {
    "SSBR":      {"desc": "Solution SBR",        "color": "#3b82f6"},
    "SBR":       {"desc": "Emulsion SBR",        "color": "#f97316"},
    "NBR":       {"desc": "Nitrile BR",           "color": "#10b981"},
    "BR":        {"desc": "Butadiene Rubber",     "color": "#8b5cf6"},
    "Silica":    {"desc": "실리카 컴파운드",        "color": "#06b6d4"},
    "Bio-Rubber":{"desc": "바이오 기반 고무",       "color": "#84cc16"},
}

# ── 검색 쿼리 ──────────────────────────────────────────────────────
CUSTOMER_QUERIES = [
    "Hankook tire rubber compound material 2025",
    "Kumho tire synthetic rubber innovation",
    "Nexen tire EV electric vehicle material",
    "Michelin tire sustainability rubber compound",
    "Bridgestone tire rubber technology EV performance",
    "Continental tire silica compound polymer",
]

MATERIAL_QUERIES = [
    "SSBR solution styrene butadiene rubber market 2025",
    "SBR styrene butadiene rubber production demand",
    "NBR nitrile butadiene rubber application",
    "butadiene rubber BR tire compound technology",
    "silica tire compound rolling resistance performance",
    "bio-based rubber sustainable tire material",
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
            analysis_type    TEXT    NOT NULL DEFAULT 'general',
            tag              TEXT,
            title            TEXT,
            source           TEXT,
            published        TEXT,
            link             TEXT,
            relevance_score  INTEGER,
            core_keywords    TEXT,
            summary          TEXT,
            business_insight TEXT,
            sales_insights   TEXT
        );
    """)

    # 기존 DB 마이그레이션: 신규 컬럼 누락 시 추가
    existing = {row[1] for row in conn.execute("PRAGMA table_info(articles)").fetchall()}
    migrations = {
        "analysis_type":  "ALTER TABLE articles ADD COLUMN analysis_type TEXT NOT NULL DEFAULT 'general'",
        "tag":            "ALTER TABLE articles ADD COLUMN tag TEXT",
        "sales_insights": "ALTER TABLE articles ADD COLUMN sales_insights TEXT",
    }
    for col, sql in migrations.items():
        if col not in existing:
            conn.execute(sql)
            print(f"[DB] 컬럼 추가: {col}")

    conn.commit()
    conn.close()


# ── DB 저장 ─────────────────────────────────────────────────────────
def save_session(created_at, collected_count, analyzed_count, elapsed_seconds,
                 session_summary, customer_articles, material_articles):
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

        for art in customer_articles:
            conn.execute(
                """INSERT INTO articles
                   (session_id, analysis_type, tag, title, source, published, link,
                    relevance_score, core_keywords, summary, business_insight, sales_insights)
                   VALUES (?, 'customer', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    art.get("company", ""),
                    art.get("title", ""),
                    art.get("source", ""),
                    art.get("published", ""),
                    art.get("link", "#"),
                    art.get("relevance_score", 0),
                    json.dumps(art.get("core_keywords", []), ensure_ascii=False),
                    art.get("summary", ""),
                    "",
                    json.dumps(art.get("sales_insights", []), ensure_ascii=False),
                ),
            )

        for art in material_articles:
            conn.execute(
                """INSERT INTO articles
                   (session_id, analysis_type, tag, title, source, published, link,
                    relevance_score, core_keywords, summary, business_insight, sales_insights)
                   VALUES (?, 'material', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    art.get("material", ""),
                    art.get("title", ""),
                    art.get("source", ""),
                    art.get("published", ""),
                    art.get("link", "#"),
                    art.get("relevance_score", 0),
                    json.dumps(art.get("core_keywords", []), ensure_ascii=False),
                    art.get("summary", ""),
                    art.get("business_insight", ""),
                    json.dumps([], ensure_ascii=False),
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

            # 고객사/소재 분석 건수 별도 집계
            counts = conn.execute(
                """SELECT analysis_type, COUNT(*) as cnt
                   FROM articles WHERE session_id=? GROUP BY analysis_type""",
                (r["id"],)
            ).fetchall()
            type_counts = {c["analysis_type"]: c["cnt"] for c in counts}

            result.append({
                "id": r["id"],
                "created_at": r["created_at"],
                "collected_count": r["collected_count"],
                "analyzed_count": r["analyzed_count"],
                "customer_count": type_counts.get("customer", 0),
                "material_count": type_counts.get("material", 0),
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

        def fetch_articles(atype):
            rows = conn.execute(
                """SELECT * FROM articles WHERE session_id=? AND analysis_type=?
                   ORDER BY relevance_score DESC""",
                (session_id, atype)
            ).fetchall()
            result = []
            for a in rows:
                kw, si = [], []
                try:
                    kw = json.loads(a["core_keywords"] or "[]")
                except Exception:
                    pass
                raw_si = a["sales_insights"] or ""
                try:
                    si = json.loads(raw_si)
                    if not isinstance(si, list):
                        si = raw_si  # 문자열이면 그대로 사용
                except Exception:
                    si = raw_si  # JSON 파싱 실패 시 원문 문자열 사용
                result.append({
                    "id": a["id"],
                    "tag": a["tag"],
                    "title": a["title"],
                    "source": a["source"],
                    "published": a["published"],
                    "link": a["link"],
                    "relevance_score": a["relevance_score"],
                    "core_keywords": kw,
                    "summary": a["summary"],
                    "business_insight": a["business_insight"],
                    "sales_insights": si,
                })
            return result

        return {
            "id": session["id"],
            "created_at": session["created_at"],
            "collected_count": session["collected_count"],
            "analyzed_count": session["analyzed_count"],
            "elapsed_seconds": session["elapsed_seconds"],
            "market_trend": session["market_trend"],
            "top_keywords": top_kw,
            "key_insight": session["key_insight"],
            "customer_articles": fetch_articles("customer"),
            "material_articles": fetch_articles("material"),
        }
    finally:
        conn.close()


# ── 뉴스 수집 ──────────────────────────────────────────────────────
def fetch_google_news(query: str, max_results: int = 6) -> list[dict]:
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
            clean_summary = BeautifulSoup(raw_summary, "html.parser").get_text()[:400]
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


def collect_articles(queries: list[str]) -> list[dict]:
    all_articles: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(fetch_google_news, q) for q in queries]
        for future in concurrent.futures.as_completed(futures):
            try:
                all_articles.extend(future.result())
            except Exception as e:
                print(f"[WARN] thread error: {e}")

    seen: set[str] = set()
    unique: list[dict] = []
    for a in all_articles:
        key = a["title"][:80].lower()
        if key not in seen:
            seen.add(key)
            unique.append(a)
    return unique


# ── Claude 분석 ────────────────────────────────────────────────────

# 고객사 분석 프롬프트
CUSTOMER_SYSTEM = """당신은 LG화학 HPM 사업부의 타이어 고무 소재(SSBR) 영업 전략 전문가입니다.
글로벌 타이어 회사의 동향을 분석하여 LG화학의 SSBR 소재 영업 기회를 발굴하는 것이 목표입니다."""

CUSTOMER_PROMPT = """아래 기사들을 분석하여 각 기사가 어느 타이어 회사와 관련 있는지 파악하고,
LG화학 HPM 사업부의 영업 인사이트 3가지를 도출하십시오.

**대상 회사:** Hankook(한국타이어), Kumho(금호타이어), Nexen(넥센타이어),
               Michelin, Bridgestone, Continental

**연관성 점수 기준 (0~100점):**
- 100점: 해당 회사가 SSBR·실리카 소재 채택·검토 직접 언급
- 80점: 해당 회사의 EV·고성능 타이어 신제품 출시 또는 소재 투자
- 60점: 해당 회사의 지속가능성·규제 대응·타이어 성능 전략
- 40점: 해당 회사 일반 사업 동향 (시장점유율, 실적 등)
- 20점 미만: 회사 언급 없거나 간접 연관 → 제외

**분석 대상 기사 ({count}건):**
{articles}

**출력 형식 (JSON만, 다른 텍스트 없음):**
{{
  "session_summary": {{
    "market_trend": "고객사 동향 전반 요약 2문장 (한국어)",
    "key_insight": "HPM 사업부 핵심 영업 시사점 1문장 (한국어)"
  }},
  "articles": [
    {{
      "title": "기사 원문 제목 (번역 금지)",
      "source": "출처명",
      "relevance_score": 85,
      "company": "Hankook",
      "core_keywords": ["키워드1", "키워드2"],
      "summary": "SSBR 영업 관점 요약 2문장 (한국어)",
      "score_reason": "연관성 점수 산정 사유 1문장 (한국어)",
      "sales_insights": "LG화학 HPM 영업 공략 포인트·제안 방향·대응 전략을 포함한 3줄 이내 통합 인사이트 (한국어)"
    }}
  ]
}}

연관성 20점 미만 기사는 제외하십시오. 반드시 JSON만 출력하십시오."""

# 소재 분석 프롬프트
MATERIAL_SYSTEM = """당신은 LG화학 HPM 사업부의 합성고무 소재 시장 분석 전문가입니다.
주요 타이어 소재의 시장 동향을 분석하여 LG화학의 소재별 비즈니스 전략에 활용할 인사이트를 도출합니다."""

MATERIAL_PROMPT = """아래 기사들을 분석하여 각 기사가 어느 소재와 관련 있는지 파악하고,
LG화학 HPM 사업부의 비즈니스 인사이트를 도출하십시오.

**대상 소재:**
- SSBR (Solution Styrene Butadiene Rubber): 양말단 기능화 SSBR, 핵심 제품
- SBR (Emulsion SBR): SSBR 경쟁·대체 소재
- NBR (Nitrile Butadiene Rubber): 산업용 고무, 인접 시장
- BR (Butadiene Rubber): 타이어 트레드 혼합 소재
- Silica: 실리카 컴파운드, SSBR 성능 발현 핵심 충전재
- Bio-Rubber: 바이오 기반·친환경 고무, 차세대 소재

**연관성 점수 기준 (0~100점):**
- 100점: 소재 생산·채택·신기술·특허 직접 언급
- 80점: 소재 수요 증가 요인 (EV 전환, 규제 강화, 성능 요구)
- 60점: 소재 시장 규모·전망·가격 동향 보고서
- 40점: 인접 소재·대체 기술 간접 언급
- 20점 미만: 소재 무관 → 제외

**분석 대상 기사 ({count}건):**
{articles}

**출력 형식 (JSON만, 다른 텍스트 없음):**
{{
  "session_summary": {{
    "market_trend": "소재 시장 전반 동향 요약 2문장 (한국어)",
    "key_insight": "HPM 사업부 소재 전략 핵심 시사점 1문장 (한국어)"
  }},
  "articles": [
    {{
      "title": "기사 원문 제목 (번역 금지)",
      "source": "출처명",
      "relevance_score": 85,
      "material": "SSBR",
      "core_keywords": ["키워드1", "키워드2"],
      "score_reason": "연관성 점수 산정 사유 1문장 (한국어)",
      "summary": "소재 비즈니스 관점 요약 2문장 (한국어)",
      "business_insight": "LG화학 HPM 사업부 대응 방안 2문장 (한국어)"
    }}
  ]
}}

연관성 20점 미만 기사는 제외하십시오. 반드시 JSON만 출력하십시오."""


def _call_claude(system_prompt, user_prompt, max_tokens=8192):
    client = anthropic.Anthropic()
    max_retries = 3
    for attempt in range(max_retries):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            break
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < max_retries - 1:
                wait = 15 * (attempt + 1)
                print(f"[WARN] API 과부하(529), {wait}초 후 재시도 ({attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                raise

    raw = message.content[0].text.strip()
    print(f"[DEBUG] 응답 {len(raw)}자, stop_reason={message.stop_reason}")

    raw = re.sub(r"```(?:json)?\s*", "", raw).strip()
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start == -1 or end == 0:
        print("[WARN] JSON 구조 없음")
        return {"session_summary": {}, "articles": []}

    try:
        parsed = json.loads(raw[start:end])
        print(f"[DEBUG] 파싱 기사 수: {len(parsed.get('articles', []))}")
        return parsed
    except json.JSONDecodeError as e:
        print(f"[WARN] JSON 파싱 오류: {e}")
        print(f"[WARN] 응답 앞 300자: {raw[:300]}")
        print(f"[WARN] 응답 뒤 200자: {raw[-200:]}")
        return {"session_summary": {}, "articles": []}


def analyze_customers(articles: list[dict]) -> dict:
    # 토큰 안전: 기사당 입력 ~200자, 출력 ~500자 → 10건 = 약 5000토큰
    batch = articles[:10]
    articles_text = "\n\n".join(
        f"[{i+1}] 제목: {a['title']}\n출처: {a['source']}\n날짜: {a['published']}\n내용: {a['summary']}"
        for i, a in enumerate(batch)
    )
    prompt = CUSTOMER_PROMPT.format(count=len(batch), articles=articles_text)
    result = _call_claude(CUSTOMER_SYSTEM, prompt, max_tokens=8192)

    # 원본 링크 재매핑
    title_map = {a["title"][:80].lower(): a for a in articles}
    for art in result.get("articles", []):
        key = art.get("title", "")[:80].lower()
        orig = title_map.get(key, {})
        art["link"] = orig.get("link", "#")
        art["published"] = orig.get("published", "")

    return result


def analyze_materials(articles: list[dict]) -> dict:
    # 토큰 안전: 10건 = 약 4000토큰 출력
    batch = articles[:10]
    articles_text = "\n\n".join(
        f"[{i+1}] 제목: {a['title']}\n출처: {a['source']}\n날짜: {a['published']}\n내용: {a['summary']}"
        for i, a in enumerate(batch)
    )
    prompt = MATERIAL_PROMPT.format(count=len(batch), articles=articles_text)
    result = _call_claude(MATERIAL_SYSTEM, prompt, max_tokens=8192)

    title_map = {a["title"][:80].lower(): a for a in articles}
    for art in result.get("articles", []):
        key = art.get("title", "")[:80].lower()
        orig = title_map.get(key, {})
        art["link"] = orig.get("link", "#")
        art["published"] = orig.get("published", "")

    return result


# ── 라우트 ──────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/collect", methods=["POST"])
def collect():
    from flask import request as freq
    try:
        t0 = time.time()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        body = freq.get_json(silent=True) or {}
        analysis_type = body.get("analysis_type", "all")  # 'all' | 'customer' | 'material'
        print(f"[INFO] 분석 타입: {analysis_type}")

        customer_raw, material_raw = [], []
        customer_result = {"session_summary": {}, "articles": []}
        material_result = {"session_summary": {}, "articles": []}

        # 선택된 타입에 따라 수집 및 분석
        if analysis_type == "all":
            # 전체: 병렬 수집 + 병렬 분석
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                f_cust = ex.submit(collect_articles, CUSTOMER_QUERIES)
                f_mat  = ex.submit(collect_articles, MATERIAL_QUERIES)
                customer_raw = f_cust.result()
                material_raw = f_mat.result()

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                f_ca = ex.submit(analyze_customers, customer_raw)
                f_ma = ex.submit(analyze_materials, material_raw)
                customer_result = f_ca.result()
                material_result = f_ma.result()

        elif analysis_type == "customer":
            customer_raw    = collect_articles(CUSTOMER_QUERIES)
            customer_result = analyze_customers(customer_raw)

        elif analysis_type == "material":
            material_raw    = collect_articles(MATERIAL_QUERIES)
            material_result = analyze_materials(material_raw)

        collected_count = len(customer_raw) + len(material_raw)
        print(f"[INFO] 고객사 수집 {len(customer_raw)}건, 소재 수집 {len(material_raw)}건")

        if collected_count == 0:
            return jsonify({"error": "뉴스를 수집할 수 없습니다. 네트워크 상태를 확인하세요."}), 503

        customer_articles = customer_result.get("articles", [])
        material_articles = material_result.get("articles", [])

        # 세션 요약 병합
        session_summary = {
            "market_trend": customer_result.get("session_summary", {}).get("market_trend", ""),
            "key_insight": material_result.get("session_summary", {}).get("key_insight", ""),
            "top_keywords": [],
        }

        elapsed = round(time.time() - t0, 1)
        analyzed_count = len(customer_articles) + len(material_articles)

        session_id = save_session(
            created_at=now,
            collected_count=collected_count,
            analyzed_count=analyzed_count,
            elapsed_seconds=elapsed,
            session_summary=session_summary,
            customer_articles=customer_articles,
            material_articles=material_articles,
        )

        return jsonify({
            "session_id": session_id,
            "customer_articles": customer_articles,
            "customer_summary": customer_result.get("session_summary", {}),
            "material_articles": material_articles,
            "material_summary": material_result.get("session_summary", {}),
            "stats": {
                "collected": collected_count,
                "customer_count": len(customer_articles),
                "material_count": len(material_articles),
                "elapsed": elapsed,
                "timestamp": now,
            },
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        if isinstance(e, anthropic.AuthenticationError):
            return jsonify({"error": "API 키가 유효하지 않습니다."}), 401
        return jsonify({"error": f"서버 오류: {str(e)}"}), 500


@app.route("/api/history", methods=["GET"])
def history():
    try:
        return jsonify({"sessions": get_sessions()})
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


# ── 고객사/소재 메타 정보 API ──────────────────────────────────────
@app.route("/api/meta", methods=["GET"])
def meta():
    return jsonify({"customers": CUSTOMERS, "materials": MATERIALS})


# ── 태그별 인사이트 조회 API ────────────────────────────────────────
@app.route("/api/insights", methods=["GET"])
def insights():
    from flask import request as freq
    tag = freq.args.get("tag", "").strip()
    if not tag:
        return jsonify({"error": "tag 파라미터가 필요합니다."}), 400
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT a.id, a.analysis_type, a.tag, a.title, a.source,
                      a.published, a.link, a.relevance_score,
                      a.core_keywords, a.summary, a.business_insight,
                      a.sales_insights,
                      s.id AS sess_id, s.created_at AS session_date
               FROM articles a
               JOIN sessions s ON a.session_id = s.id
               WHERE a.tag = ?
               ORDER BY s.id DESC, a.relevance_score DESC""",
            (tag,)
        ).fetchall()
        result = []
        for a in rows:
            kw = []
            try:
                kw = json.loads(a["core_keywords"] or "[]")
            except Exception:
                pass
            raw_si = a["sales_insights"] or ""
            try:
                si = json.loads(raw_si)
                if not isinstance(si, list):
                    si = raw_si
            except Exception:
                si = raw_si
            result.append({
                "session_id":      a["sess_id"],
                "session_date":    a["session_date"],
                "analysis_type":   a["analysis_type"],
                "tag":             a["tag"],
                "title":           a["title"],
                "source":          a["source"],
                "published":       a["published"],
                "link":            a["link"],
                "relevance_score": a["relevance_score"],
                "core_keywords":   kw,
                "summary":         a["summary"],
                "business_insight":a["business_insight"],
                "sales_insights":  si,
            })
        conn.close()
        return jsonify({"tag": tag, "articles": result, "count": len(result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# gunicorn 등 외부 실행 환경에서도 DB 초기화 보장
init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
