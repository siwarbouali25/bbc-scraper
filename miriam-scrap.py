# scraper.py
import os, time, csv, hashlib, json
import requests, feedparser, pandas as pd
from bs4 import BeautifulSoup
from readability import Document
from dateutil import parser as dtparse
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# ===================== CONFIG =====================
# You can use strings OR lists for each category.
FEEDS = {
    "politics": [
        "https://feeds.bbci.co.uk/news/politics/rss.xml",
        "https://www.theguardian.com/politics/rss",
        "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
        "https://rss.cnn.com/rss/edition_world.rss",
    ],
    "entertainment": [
        "https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
        "https://rss.cnn.com/rss/edition_entertainment.rss",
        "https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml",
    ],
    # Single URL still works fine:
    # "business": "https://feeds.bbci.co.uk/news/business/rss.xml",
}

MAX_PER_FEED   = 60           # safety cap per feed per run
PAUSE_SECONDS  = 1.2          # politeness delay between article fetches
TIMEOUT        = 20
OUTPUT_CSV     = "bbc_articles_simple_miriam.csv"

HEADERS = {
    "User-Agent": "bbc-hourly-scraper/1.0 (+contact@example.com)",
    "Accept-Language": "en;q=0.9, fr;q=0.8"
}

# Tracking params to strip when normalizing URLs
STRIP_QUERY_PARAMS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "at_medium","at_campaign","at_custom1","ns_mchannel","ns_source","ns_campaign"
}

# Optional: map domains to friendly source names
DOMAIN_SOURCE_MAP = {
    "bbc.co.uk": "BBC", "bbc.com": "BBC",
    "cnn.com": "CNN",
    "reuters.com": "Reuters",
    "aljazeera.com": "Al Jazeera",
    "npr.org": "NPR",
    "theguardian.com": "The Guardian",
    "nytimes.com": "NYTimes",
    "espn.com": "ESPN",
    "skysports.com": "Sky Sports",
    "eurosport.com": "Eurosport",
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

def load_feed(feed_url: str):
    """Fetch RSS/Atom with headers, then parse with feedparser (more reliable for some sites)."""
    try:
        resp = requests.get(feed_url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return feedparser.parse(resp.content)
    except Exception as ex:
        print("[skip feed]", feed_url, "->", ex)
        return None

def entry_link(e):
    """Robustly extract an article URL from a feed entry."""
    # feedparser returns FeedParserDict (mapping-like)
    if isinstance(e, dict):
        if e.get("link"):
            return e.get("link")
        links = e.get("links") or []
        if links and isinstance(links, list) and isinstance(links[0], dict):
            href = links[0].get("href")
            if href:
                return href
        return e.get("id") or None
    try:
        if getattr(e, "link", None):
            return e.link
        links = getattr(e, "links", [])
        if links and isinstance(links, list) and isinstance(links[0], dict) and links[0].get("href"):
            return links[0]["href"]
        return getattr(e, "id", None)
    except Exception:
        return None

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

def infer_source(u: str) -> str:
    """Infer human-friendly source name from URL domain."""
    try:
        host = urlparse(u).netloc.lower()
        for prefix in ("www.", "edition.", "amp."):
            if host.startswith(prefix):
                host = host[len(prefix):]
        for dom, name in DOMAIN_SOURCE_MAP.items():
            if host.endswith(dom):
                return name
        # fallback: host without port
        return host.split(":")[0]
    except Exception:
        return "Unknown"

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

    # Author (often omitted)
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

    # ----- Body extraction: Readability → site selectors → JSON-LD → AMP -----
    content_text = ""
    # 1) Readability
    try:
        content_html = Document(html).summary(html_partial=True)
        content_text = BeautifulSoup(content_html, "lxml").get_text(" ", strip=True)
    except Exception:
        content_text = ""

    # 2) Generic selectors (works for many sites including BBC)
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

    # Infer source dynamically
    source_name = infer_source(canonical or norm_url)

    return {
        "id_article": id_article,
        "title": title,
        "tags": tags,
        "content": content_text.strip(),
        "url": canonical or norm_url,   # store canonical when available
        "category": category,
        "source": source_name,
        "author": author,
        "image": image,
        "published_date": published_date,
        "content_hash": content_hash,   # kept to de-dup across runs
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
        return set(df["id_article"].astyp_]()
