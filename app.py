import os
import re
import json
import time
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

    # 제목 기준 중복 제거
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

USER_PROMPT_TEMPLATE = """아래 기사 목록을 분석하여 양말단 SSBR 비즈니스와의 연관성이 80점 이상인 기사만 선별하십시오.

**선별 기준 (1개 이상 강하게 부합 시 선별):**
- 시장 니즈: 전기차(EV) 타이어, Low Rolling Resistance, Wet Grip, 내마모성 요구사항
- 규제 동향: 유럽/북미 환경 규제, 탄소 배출 저감 정책, 타이어 라벨링 제도 강화
- 경쟁 동향: 화학사의 SSBR 생산 능력 확대, 기능성 고무 신제품 개발 및 특허 출원
- 차세대 기술: 실리카 친화성 향상, 폴리머 구조 제어, 양말단 기능화 기술 동향

**분석 대상 기사 ({count}건):**
{articles}

**출력 형식 (JSON만 출력, 다른 텍스트 없음):**
{{
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

연관성 80점 미만 기사는 출력에서 완전히 제외하십시오. 반드시 JSON 형식만 출력하십시오."""


def analyze_with_claude(articles: list[dict]) -> dict:
    client = anthropic.Anthropic()

    articles_text = "\n\n".join(
        f"[{i+1}] 제목: {a['title']}\n출처: {a['source']}\n날짜: {a['published']}\n내용: {a['summary']}"
        for i, a in enumerate(articles[:35])
    )

    prompt = USER_PROMPT_TEMPLATE.format(count=len(articles[:35]), articles=articles_text)

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    response_text = message.content[0].text.strip()

    json_match = re.search(r"\{[\s\S]*\}", response_text)
    if not json_match:
        return {"articles": []}

    parsed = json.loads(json_match.group())

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

        articles = collect_all_articles()
        collected_count = len(articles)

        if not articles:
            return jsonify({"error": "뉴스를 수집할 수 없습니다. 네트워크 상태를 확인하세요."}), 503

        result = analyze_with_claude(articles)
        filtered = result.get("articles", [])

        return jsonify({
            "articles": filtered,
            "stats": {
                "collected": collected_count,
                "filtered": len(filtered),
                "elapsed": round(time.time() - t0, 1),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        if isinstance(e, anthropic.AuthenticationError):
            return jsonify({"error": "Anthropic API 키가 유효하지 않습니다. .env 파일의 ANTHROPIC_API_KEY를 확인하세요."}), 401
        return jsonify({"error": f"서버 오류: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
