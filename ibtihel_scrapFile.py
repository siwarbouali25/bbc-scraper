# scraper.py
import os, time, csv, hashlib, json
import requests, feedparser, pandas as pd
from bs4 import BeautifulSoup
from readability import Document
from dateutil import parser as dtparse
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# ===================== CONFIG =====================
FEEDS = {
      "Politics": "https://feeds.washingtonpost.com/rss/politics",
    "World": "https://feeds.washingtonpost.com/rss/world",
    "Business": "https://feeds.washingtonpost.com/rss/business",
    "Technology": "https://feeds.washingtonpost.com/rss/business/technology",
    "Science": "https://feeds.washingtonpost.com/rss/national/science",
    "Health": "https://feeds.washingtonpost.com/rss/health",
    "Sport": "https://feeds.washingtonpost.com/rss/sports",
    "Entertainment": "https://feeds.washingtonpost.com/rss/entertainment",
    "Culture": "https://feeds.washingtonpost.com/rss/arts",
    "Society": "https://feeds.washingtonpost.com/rss/lifestyle"
}
MAX_PER_FEED   = 60          # safety cap per feed per run
PAUSE_SECONDS  = 1.2         # politeness delay between article fetches
TIMEOUT        = 20
OUTPUT_CSV     = "articles_simple_ibtihel.csv"

HEADERS = {
    "User-Agent": "bty-hourly-scraper/1.0 (+contact@example.com)",
    "Accept-Language": "en;q=0.9, fr;q=0.8"
}

# Tracking params to strip when normalizing URLs
STRIP_QUERY_PARAMS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "at_medium","at_campaign","at_custom1","ns_mchannel","ns_source","ns_campaign"
}

# ===================== UTILS =====================
def normalize_url(u: str) -> str:
    """Normalize URL: lowercase host, strip fragment & tracking params, trim trailing slash."""
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

def fetch(url, timeout=TIMEOUT):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r

def clean_join(paras):
    """Join <p> nodes into paragraphs; skip empties and obvious non-body items."""
    out = []
    for p in paras:
        txt = p.get_text(" ", strip=True)
        if not txt or len(txt) < 3:
            continue
        # Skip common non-body containers via class hints
        cls = " ".join(p.get("class", [])).lower()
        if any(bad in cls for bad in ["promo","share","related","advert","cookie"]):
            continue
        # Skip if inside non-body ancestors
        bad = False
        for anc in p.parents:
            if getattr(anc, "name", None) in ("figure","figcaption","aside","header","footer","nav"):
                bad = True; break
            acl = " ".join(anc.get("class", [])).lower() if hasattr(anc, "get") else ""
            if any(x in acl for x in ["promo","related","share","advert","cookie"]):
                bad = True; break
        if bad: 
            continue
        out.append(txt)
    return "\n\n".join(out).strip()

# ===================== EXTRACTION =====================
def parse_article(url, category):
    """Return article dict or None if extraction fails/thin."""
    try:
        html = fetch(url).text
    except Exception as e:
        print(f"[skip fetch] {url} -> {e}")
        return None

    soup = BeautifulSoup(html, "lxml")

    # Canonical & normalized URLs
    canonical = (soup.find("link", rel="canonical") or {}).get("href") or url
    canonical = normalize_url(canonical)
    norm_url  = normalize_url(url)

    # Title
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else (soup.find("meta", property="og:title") or {}).get("content") or ""

    # Author (BBC often omits)
    author_meta = soup.find("meta", attrs={"name": "byl"}) or soup.find("meta", attrs={"name": "author"})
    author = author_meta.get("content") if author_meta else None

    # Image
    image = (soup.find("meta", property="og:image") or {}).get("content")

    # Tags
    meta_kw = soup.find("meta", attrs={"name": "news_keywords"}) or soup.find("meta", attrs={"name": "keywords"})
    tags_list = [t.strip().lower() for t in (meta_kw.get("content","").split(",")) if t.strip()] if meta_kw else []
    tags = ", ".join(tags_list) if tags_list else None

    # Published date
    date_raw = None
    for tag, attrs, attr in [
        ("meta", {"property": "article:published_time"}, "content"),
        ("meta", {"name": "OriginalPublicationDate"}, "content"),
        ("time", {}, "datetime"),
    ]:
        el = soup.find(tag, attrs)
        if el and el.get(attr):
            date_raw = el.get(attr); break
    try:
        published_date = dtparse.parse(date_raw).isoformat() if date_raw else None
    except Exception:
        published_date = None

    # ----- Body extraction: Readability → BBC selectors → JSON-LD → AMP -----
    content_text = ""
    # 1) Readability
    try:
        content_html = Document(html).summary(html_partial=True)
        content_text = BeautifulSoup(content_html, "lxml").get_text(" ", strip=True)
    except Exception:
        content_text = ""

    # 2) BBC selectors
    if len(content_text) < 800:
        paras = (soup.select('[data-component="text-block"] p') or
                 soup.select("article p") or
                 soup.select("main p") or
                 soup.select('[class*="RichTextComponentWrapper"] p'))
        if paras:
            txt = clean_join(paras)
            if len(txt) > len(content_text):
                content_text = txt

    # 3) JSON-LD articleBody
    if len(content_text) < 800:
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(s.string or "")
            except Exception:
                continue
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if isinstance(obj, dict) and obj.get("@type") in ("NewsArticle","Article"):
                    body = obj.get("articleBody")
                    if isinstance(body, str) and len(body) > len(content_text):
                        content_text = body.strip()
            if len(content_text) >= 800:
                break

    # 4) AMP fallback
    if len(content_text) < 800:
        amp = (soup.find("link", rel="amphtml") or {}).get("href")
        if amp:
            try:
                amp_html = fetch(amp).text
                amp_soup = BeautifulSoup(amp_html, "lxml")
                amp_paras = amp_soup.select("article p") or amp_soup.select("main p") or amp_soup.select("p")
                amp_text = clean_join(amp_paras)
                if len(amp_text) > len(content_text):
                    content_text = amp_text
            except Exception:
                pass

    # Thin pages are skipped
    if len(content_text.strip()) < 200:
        return None

    # ----- De-dup keys -----
    # Prefer canonical URL; fallback to normalized request URL
    id_source = canonical or norm_url
    id_article = hashlib.sha1(id_source.encode()).hexdigest()[:12]

    # Content hash catches same story under different URLs
    content_hash = hashlib.sha1((title + "|" + content_text[:4000]).encode("utf-8", "ignore")).hexdigest()

     return {
        "id_article": id_article,
        "title": title,
        "tags": tags,
        "content": content_text.strip(),
        "url": canonical or norm_url,
        "category": category,
        "source": "Washington Post",   # ✅ fixed source name
        "author": author,
        "image": image,
        "published_date": published_date,
        "content_hash": content_hash,
    }

# ===================== DEDUPE STORAGE =====================
def ensure_csv(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        pd.DataFrame(columns=[
            "id_article","title","tags","content","url","category","source","author","image","published_date","content_hash"
        ]).to_csv(path, index=False)

def load_existing_keys(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set(), set()
    try:
        df = pd.read_csv(path, usecols=["id_article","content_hash"])
        return set(df["id_article"].astype(str)), set(df["content_hash"].astype(str))
    except Exception:
        # fallback if older file lacks content_hash
        try:
            df = pd.read_csv(path, usecols=["id_article"])
            return set(df["id_article"].astype(str)), set()
        except Exception:
            return set(), set()

# ===================== MAIN =====================
def main():
    ensure_csv(OUTPUT_CSV)
    seen_ids, seen_content = load_existing_keys(OUTPUT_CSV)
    seen_run_ids, seen_run_content = set(), set()

    new_rows = []

    for category, feed_url in FEEDS.items():
        print(f"[feed] {category} → {feed_url}")
        feed = feedparser.parse(feed_url)
        for e in feed.entries[:MAX_PER_FEED]:
            link = e.get("link")
            if not link:
                continue
            # Normalize RSS link early to reduce duplicates before fetch
            link = normalize_url(link)
            try:
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
            except Exception as ex:
                print("[skip]", link, "->", ex)

    if new_rows:
        pd.DataFrame(new_rows).to_csv(
            OUTPUT_CSV, mode="a", header=False, index=False, quoting=csv.QUOTE_MINIMAL
        )
        print(f"💾 Appended {len(new_rows)} new rows to {OUTPUT_CSV}")
    else:
        print("No new rows.")

if __name__ == "__main__":
    pd.set_option("display.width", 160)
    main()
