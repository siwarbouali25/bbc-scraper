# oumaima-scrap.py
import os
import time
import csv
import hashlib
import json
import requests
import feedparser
import pandas as pd
from bs4 import BeautifulSoup
from readability import Document
from dateutil import parser as dtparse
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# ================= CONFIG =================
FEEDS = {
    "Politics": "https://www.theguardian.com/politics/rss",
}

MAX_PER_FEED  = 50
PAUSE_SECONDS = 1.2
OUTPUT_CSV    = "articles_simple_oumaima.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

STRIP_QUERY_PARAMS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content"}

# ================= UTILS =================
def normalize_url(u: str) -> str:
    p = urlparse(u)
    q = {k: v for k, v in parse_qsl(p.query, keep_blank_values=True)
         if k not in STRIP_QUERY_PARAMS}
    clean = p._replace(
        scheme=p.scheme.lower(),
        netloc=p.netloc.lower(),
        path=p.path.rstrip("/"),
        query=urlencode(q, doseq=True),
        fragment=""
    )
    return urlunparse(clean)

def fetch(url, timeout=20):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[ERROR] fetch {url} -> {e}")
        return ""

def clean_join(paras):
    out = []
    for p in paras:
        txt = p.get_text(" ", strip=True)
        if not txt or len(txt) < 3:
            continue
        cls = " ".join(p.get("class", [])).lower()
        if any(bad in cls for bad in ["promo","share","related","advert","cookie"]):
            continue
        bad = False
        for anc in p.parents:
            if getattr(anc, "name", None) in ("figure","figcaption","aside","header","footer","nav"):
                bad = True
                break
            acl = " ".join(anc.get("class", [])).lower() if hasattr(anc, "get") else ""
            if any(x in acl for x in ["promo","related","share","advert","cookie"]):
                bad = True
                break
        if bad: 
            continue
        out.append(txt)
    return "\n\n".join(out).strip()

# ================= EXTRACTION =================
def parse_article(url, category):
    html = fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")

    # Title
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else (soup.find("meta", property="og:title") or {}).get("content") or ""

    # Author
    author_meta = soup.find("meta", attrs={"name": "author"})
    author = author_meta.get("content") if author_meta else None

    # Image
    image = (soup.find("meta", property="og:image") or {}).get("content")

    # Tags
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    tags_list = [t.strip().lower() for t in (meta_kw.get("content","").split(",")) if t.strip()] if meta_kw else []
    tags = ", ".join(tags_list) if tags_list else None

    # Published date
    date_raw = None
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        date_raw = time_tag.get("datetime")
    try:
        published_date = dtparse.parse(date_raw).isoformat() if date_raw else None
    except Exception:
        published_date = None

    # Body extraction via Readability
    content_text = ""
    try:
        content_html = Document(html).summary(html_partial=True)
        content_text = BeautifulSoup(content_html, "lxml").get_text(" ", strip=True)
    except Exception:
        content_text = ""

    if len(content_text) < 200:
        paras = soup.select("p")
        if paras:
            content_text = clean_join(paras)

    if len(content_text.strip()) < 100:
        return None

    # IDs & hash
    norm_url = normalize_url(url)
    id_article = hashlib.sha1(norm_url.encode()).hexdigest()[:12]
    content_hash = hashlib.sha1((title + "|" + content_text[:4000]).encode("utf-8", "ignore")).hexdigest()

    return {
        "id_article": id_article,
        "title": title,
        "tags": tags,
        "content": content_text.strip(),
        "url": norm_url,
        "category": category,
        "source": "Reuters",
        "author": author,
        "image": image,
        "published_date": published_date,
        "content_hash": content_hash,
    }

# ================= STORAGE =================
def ensure_csv(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        pd.DataFrame(columns=[
            "id_article","title","tags","content","url","category","source",
            "author","image","published_date","content_hash"
        ]).to_csv(path, index=False)

def load_existing_keys(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set(), set()
    try:
        df = pd.read_csv(path, usecols=["id_article","content_hash"])
        return set(df["id_article"].astype(str)), set(df["content_hash"].astype(str))
    except Exception:
        return set(), set()

# ================= MAIN =================
def main():
    ensure_csv(OUTPUT_CSV)
    seen_ids, seen_content = load_existing_keys(OUTPUT_CSV)
    seen_run_ids, seen_run_content = set(), set()
    new_rows = []

    for category, feed_url in FEEDS.items():
        print(f"[FEED] {category} -> {feed_url}")
        feed = feedparser.parse(feed_url)
        print(f"[INFO] Found {len(feed.entries)} entries in {category}")
        for e in feed.entries[:MAX_PER_FEED]:
            link = e.get("link")
            if not link:
                continue
            row = parse_article(link, category)
            if not row:
                continue
            if (row["id_article"] in seen_ids) or (row["id_article"] in seen_run_ids):
                continue
            if (row["content_hash"] in seen_content) or (row["content_hash"] in seen_run_content):
                continue
            new_rows.append(row)
            seen_run_ids.add(row["id_article"])
            seen_run_content.add(row["content_hash"])
            print(f"✓ {row['title'][:80]}…")
            time.sleep(PAUSE_SECONDS)

    if new_rows:
        pd.DataFrame(new_rows).to_csv(
            OUTPUT_CSV, mode="a", header=False, index=False, quoting=csv.QUOTE_MINIMAL
        )
        print(f"[DONE] Appended {len(new_rows)} articles to {OUTPUT_CSV}")
    else:
        print("[DONE] No new articles found.")

if __name__ == "__main__":
    pd.set_option("display.width", 160)
    main()
