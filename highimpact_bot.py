"""
高インパクトジャーナル 消化器関連新着 Bot
==========================================
- NEJM, Lancet, Gut, Gastroenterology 等のトップジャーナルを監視
- 消化器関連の論文のみをフィルタして抽出
- Gemini API で日本語要約 + 臨床的インパクトを評価
- Discord Webhook で通知
"""

import os
import json
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import requests
import google.generativeai as genai

# ============================================================
# 設定
# ============================================================
HIGHIMPACT_WEBHOOK_URL = os.environ["HIGHIMPACT_WEBHOOK_URL"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]

genai.configure(api_key=GEMINI_API_KEY)
GEMINI_MODEL = "gemini-2.0-flash"

NOTIFIED_FILE = Path(__file__).parent / "notified_highimpact_pmids.json"

SEARCH_DAYS = 3  # 直近3日分 (週3回実行を想定)
MAX_RESULTS = 20

# ============================================================
# 監視対象ジャーナル
# Journal[ta] = Title Abbreviation で検索
# ============================================================
JOURNALS = {
    # --- 総合医学トップジャーナル ---
    "N Engl J Med": {
        "name": "New England Journal of Medicine",
        "abbr": "NEJM",
        "tier": "総合",
        "color": 0xC0392B,
    },
    "Lancet": {
        "name": "The Lancet",
        "abbr": "Lancet",
        "tier": "総合",
        "color": 0x2980B9,
    },
    "JAMA": {
        "name": "JAMA",
        "abbr": "JAMA",
        "tier": "総合",
        "color": 0x8E44AD,
    },
    "BMJ": {
        "name": "BMJ",
        "abbr": "BMJ",
        "tier": "総合",
        "color": 0x2C3E50,
    },
    "Nat Med": {
        "name": "Nature Medicine",
        "abbr": "Nat Med",
        "tier": "総合",
        "color": 0xE74C3C,
    },
    # --- 消化器専門トップジャーナル ---
    "Gastroenterology": {
        "name": "Gastroenterology",
        "abbr": "Gastroenterology",
        "tier": "消化器",
        "color": 0x27AE60,
    },
    "Gut": {
        "name": "Gut",
        "abbr": "Gut",
        "tier": "消化器",
        "color": 0x16A085,
    },
    "J Hepatol": {
        "name": "Journal of Hepatology",
        "abbr": "J Hepatol",
        "tier": "肝臓",
        "color": 0xD35400,
    },
    "Hepatology": {
        "name": "Hepatology",
        "abbr": "Hepatology",
        "tier": "肝臓",
        "color": 0xE67E22,
    },
    "Lancet Gastroenterol Hepatol": {
        "name": "Lancet Gastroenterology & Hepatology",
        "abbr": "Lancet GH",
        "tier": "消化器",
        "color": 0x2471A3,
    },
    "Am J Gastroenterol": {
        "name": "American Journal of Gastroenterology",
        "abbr": "AJG",
        "tier": "消化器",
        "color": 0x1ABC9C,
    },
    "Clin Gastroenterol Hepatol": {
        "name": "Clinical Gastroenterology and Hepatology",
        "abbr": "CGH",
        "tier": "消化器",
        "color": 0x3498DB,
    },
    "J Crohns Colitis": {
        "name": "Journal of Crohn's and Colitis",
        "abbr": "JCC",
        "tier": "IBD",
        "color": 0x9B59B6,
    },
    "Inflamm Bowel Dis": {
        "name": "Inflammatory Bowel Diseases",
        "abbr": "IBD",
        "tier": "IBD",
        "color": 0xA569BD,
    },
    "Gastrointest Endosc": {
        "name": "Gastrointestinal Endoscopy",
        "abbr": "GIE",
        "tier": "内視鏡",
        "color": 0x5DADE2,
    },
    "Endoscopy": {
        "name": "Endoscopy",
        "abbr": "Endoscopy",
        "tier": "内視鏡",
        "color": 0x48C9B0,
    },
    # --- Nature / Cell 系サブジャーナル ---
    "Nat Rev Gastroenterol Hepatol": {
        "name": "Nature Reviews Gastroenterology & Hepatology",
        "abbr": "Nat Rev GH",
        "tier": "レビュー",
        "color": 0xCB4335,
    },
    "Cell Host Microbe": {
        "name": "Cell Host & Microbe",
        "abbr": "Cell H&M",
        "tier": "マイクロバイオーム",
        "color": 0x1F618D,
    },
}

# 総合ジャーナル (NEJM, Lancet等) は消化器関連のみフィルタ
GI_FILTER_QUERY = (
    '("Gastrointestinal Diseases"[MeSH] OR "Liver Diseases"[MeSH] '
    'OR "Pancreatic Diseases"[MeSH] OR "Biliary Tract Diseases"[MeSH] '
    'OR "Gastrointestinal Neoplasms"[MeSH] OR "Inflammatory Bowel Diseases"[MeSH] '
    'OR "Gastrointestinal Microbiome"[MeSH] OR "Endoscopy, Gastrointestinal"[MeSH] '
    'OR "gastro*"[Title] OR "hepat*"[Title] OR "liver"[Title] '
    'OR "pancrea*"[Title] OR "colon*"[Title] OR "colorectal"[Title] '
    'OR "esophag*"[Title] OR "intestin*"[Title] OR "bowel"[Title] '
    'OR "celiac"[Title] OR "cirrhosis"[Title] OR "endoscop*"[Title] '
    'OR "biliary"[Title] OR "gallbladder"[Title] OR "microbiome"[Title] '
    'OR "microbiota"[Title] OR "ulcerative colitis"[Title] OR "Crohn"[Title])'
)

GENERAL_JOURNALS = {"N Engl J Med", "Lancet", "JAMA", "BMJ", "Nat Med"}

# ============================================================
# PubMed E-utilities
# ============================================================
ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def search_journal(journal_ta: str, reldate: int, apply_gi_filter: bool) -> list[str]:
    query = f'"{journal_ta}"[ta]'
    if apply_gi_filter:
        query = f'{query} AND {GI_FILTER_QUERY}'

    params = {
        "db": "pubmed",
        "term": query,
        "retmax": MAX_RESULTS,
        "datetype": "edat",
        "reldate": reldate,
        "retmode": "json",
        "sort": "date",
    }
    resp = requests.get(ESEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    pmids = data.get("esearchresult", {}).get("idlist", [])
    return pmids


def fetch_articles(pmids: list[str]) -> list[dict]:
    if not pmids:
        return []

    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
        "rettype": "abstract",
    }
    resp = requests.get(EFETCH_URL, params=params, timeout=30)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    articles = []

    for article_elem in root.findall(".//PubmedArticle"):
        pmid = _text(article_elem, ".//PMID")
        title = _full_text(article_elem, ".//ArticleTitle")

        # Abstract
        abstract_parts = []
        for at in article_elem.findall(".//AbstractText"):
            label = at.get("Label", "")
            text = "".join(at.itertext()).strip()
            if label:
                abstract_parts.append(f"[{label}] {text}")
            else:
                abstract_parts.append(text)
        abstract = "\n".join(abstract_parts)

        if not abstract:
            abstract_node = article_elem.find(".//Abstract")
            if abstract_node is not None:
                abstract = "".join(abstract_node.itertext()).strip()

        if not abstract:
            print(f"  [SKIP] PMID {pmid}: abstractなし")
            continue

        journal = _full_text(article_elem, ".//Journal/Title")
        journal_ta = _text(article_elem, ".//Journal/ISOAbbreviation")

        authors = []
        for author in article_elem.findall(".//Author")[:3]:
            last = _text(author, "LastName")
            fore = _text(author, "ForeName")
            if last:
                authors.append(f"{last} {fore}".strip())
        if len(article_elem.findall(".//Author")) > 3:
            authors.append("et al.")

        doi = ""
        for aid in article_elem.findall(".//ArticleId"):
            if aid.get("IdType") == "doi":
                doi = aid.text or ""

        # Publication Type
        pub_types = []
        for pt in article_elem.findall(".//PublicationType"):
            if pt.text:
                pub_types.append(pt.text.strip())

        articles.append({
            "pmid": pmid,
            "title": title,
            "abstract": abstract,
            "journal": journal,
            "journal_ta": journal_ta,
            "authors": ", ".join(authors),
            "doi": doi,
            "pub_types": pub_types,
        })

    return articles


def _text(elem, path: str) -> str:
    node = elem.find(path)
    if node is not None and node.text:
        return node.text.strip()
    return ""


def _full_text(elem, path: str) -> str:
    node = elem.find(path)
    if node is not None:
        return "".join(node.itertext()).strip()
    return ""


# ============================================================
# Gemini API で要約生成
# ============================================================
def summarize_article(article: dict) -> dict:
    model = genai.GenerativeModel(GEMINI_MODEL)

    pub_type_str = ", ".join(article["pub_types"]) if article["pub_types"] else "不明"

    prompt = f"""あなたは消化器内科の専門医向けに高インパクトジャーナルの最新論文を紹介する医学ライターです。
以下の論文を日本語で要約してください。

## 出力フォーマット（厳守）

HEADLINE: （臨床的に最も重要なポイントを1行で。キャッチーかつ正確に。）

STUDY_TYPE: （研究の種類を1語で。例: RCT / メタ解析 / コホート研究 / 症例対照研究 / レビュー / レター / 基礎研究）

SUMMARY: （4〜6文。研究の背景→方法→主要結果→結論を含める。数値データがあれば含める。）

CLINICAL_IMPACT: （1〜2文。この研究が臨床現場にどう影響するか、明日からの診療が変わるかを簡潔に。）

## 論文情報
タイトル: {article['title']}
ジャーナル: {article['journal']}
著者: {article['authors']}
Publication Type: {pub_type_str}

Abstract:
{article['abstract']}
"""

    response = model.generate_content(prompt)
    text = response.text

    # パース
    headline = ""
    study_type = ""
    summary = ""
    clinical_impact = ""

    lines = text.split("\n")
    current_section = None
    section_lines = {"SUMMARY": [], "CLINICAL_IMPACT": []}

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("HEADLINE:"):
            headline = stripped.replace("HEADLINE:", "").strip()
            current_section = None
        elif stripped.startswith("STUDY_TYPE:"):
            study_type = stripped.replace("STUDY_TYPE:", "").strip()
            current_section = None
        elif stripped.startswith("SUMMARY:"):
            content = stripped.replace("SUMMARY:", "").strip()
            if content:
                section_lines["SUMMARY"].append(content)
            current_section = "SUMMARY"
        elif stripped.startswith("CLINICAL_IMPACT:"):
            content = stripped.replace("CLINICAL_IMPACT:", "").strip()
            if content:
                section_lines["CLINICAL_IMPACT"].append(content)
            current_section = "CLINICAL_IMPACT"
        elif current_section and stripped:
            section_lines[current_section].append(stripped)

    summary = " ".join(section_lines["SUMMARY"]).strip()
    clinical_impact = " ".join(section_lines["CLINICAL_IMPACT"]).strip()

    return {
        "headline": headline or article["title"],
        "study_type": study_type or "不明",
        "summary": summary or "（要約生成に失敗しました）",
        "clinical_impact": clinical_impact or "",
    }


# ============================================================
# Discord 通知
# ============================================================
def send_discord_notification(article: dict, result: dict, journal_config: dict):
    pubmed_url = f"https://pubmed.ncbi.nlm.nih.gov/{article['pmid']}/"
    doi_url = f"https://doi.org/{article['doi']}" if article["doi"] else ""

    links = f"[PubMed]({pubmed_url})"
    if doi_url:
        links += f"  |  [Full Text]({doi_url})"

    tier_emoji = {
        "総合": "👑",
        "消化器": "🏥",
        "肝臓": "🫁",
        "IBD": "🔥",
        "内視鏡": "🔬",
        "レビュー": "📚",
        "マイクロバイオーム": "🦠",
    }
    emoji = tier_emoji.get(journal_config["tier"], "📄")

    fields = [
        {
            "name": f"📊 研究タイプ: {result['study_type']}",
            "value": result["summary"][:1024],
            "inline": False,
        },
    ]

    if result["clinical_impact"]:
        fields.append({
            "name": "💡 臨床的インパクト",
            "value": result["clinical_impact"][:1024],
            "inline": False,
        })

    fields.extend([
        {
            "name": "📄 論文情報",
            "value": f"**{article['title'][:200]}**\n"
                     f"_{article['journal']}_  |  {article['authors']}",
            "inline": False,
        },
        {
            "name": "🔗 リンク",
            "value": links,
            "inline": False,
        },
    ])

    embed = {
        "title": f"{emoji} [{journal_config['abbr']}] {result['headline']}"[:256],
        "url": pubmed_url,
        "color": journal_config["color"],
        "fields": fields,
        "footer": {
            "text": f"{journal_config['name']}  |  {journal_config['tier']}  |  PMID: {article['pmid']}",
        },
        "timestamp": datetime.utcnow().isoformat(),
    }

    payload = {
        "username": "High Impact GI Bot",
        "embeds": [embed],
    }

    resp = requests.post(HIGHIMPACT_WEBHOOK_URL, json=payload, timeout=15)
    resp.raise_for_status()
    print(f"[Discord] 通知: [{journal_config['abbr']}] PMID {article['pmid']}")


# ============================================================
# 重複排除
# ============================================================
def load_notified_pmids() -> set[str]:
    if NOTIFIED_FILE.exists():
        data = json.loads(NOTIFIED_FILE.read_text())
        return set(data.get("pmids", []))
    return set()


def save_notified_pmids(pmids: set[str]):
    recent = sorted(pmids)[-3000:]
    NOTIFIED_FILE.write_text(json.dumps({"pmids": recent}, indent=2))


# ============================================================
# メイン処理
# ============================================================
def main():
    print(f"=== High Impact GI Bot 実行: {datetime.now().isoformat()} ===")

    notified = load_notified_pmids()
    count = 0

    for journal_ta, config in JOURNALS.items():
        apply_filter = journal_ta in GENERAL_JOURNALS
        label = f"[{config['abbr']}]"

        if apply_filter:
            print(f"{label} 消化器フィルタ付きで検索...")
        else:
            print(f"{label} 全件検索...")

        pmids = search_journal(journal_ta, reldate=SEARCH_DAYS, apply_gi_filter=apply_filter)
        new_pmids = [p for p in pmids if p not in notified]

        if not new_pmids:
            print(f"{label} 新着なし")
            time.sleep(0.4)
            continue

        print(f"{label} 新着 {len(new_pmids)} 件")

        articles = fetch_articles(new_pmids)
        time.sleep(0.4)

        for article in articles:
            try:
                result = summarize_article(article)
                send_discord_notification(article, result, config)
                notified.add(article["pmid"])
                count += 1
                time.sleep(2)  # Discord rate limit
            except Exception as e:
                print(f"[Error] PMID {article['pmid']}: {e}")

    save_notified_pmids(notified)
    print(f"=== 完了: {count} 件通知 ===")


if __name__ == "__main__":
    main()
