"""
Fast, resumable, polite news scraper.

- Many worker threads, but a per-host rate limiter (robots.txt + --delay) keeps each site polite.
- Saves every batch to the CSV AND to a PostgreSQL table (pgAdmin database). Use --no-db for CSV only.
- Resumable with plain files: article_urls.csv (discovered URLs), articles.csv (saved articles),
  failed_urls.txt (skipped URLs). Stop/restart any time, nothing is refetched.
- Discovery walks ALL sitemaps (incl. gzip + indexes), robots.txt Sitemap: lines, RSS, then listing pages.
- Stops when --total articles are saved (not when 10 sources x per-source is reached).
- Does NOT bypass 401/403/429, CAPTCHAs or paywalls: it backs off and skips.
- CSV has website_name, url, title, heading, paragraph (full article text), author, date, scraped_at (UTC), domain.
"""
import argparse
import csv
import getpass
import gzip
import json
import logging
import math
import os
import random
import re
import shutil
import threading
import time
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag
from urllib.robotparser import RobotFileParser

import feedparser
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None

csv.field_size_limit(50_000_000)     # long articles can exceed the default 131072-char CSV field limit

try:
    import lxml  # noqa: F401
    PARSER = "lxml"          # ~3-5x faster than html.parser
except ImportError:
    PARSER = "html.parser"

UA = "NewsResearchBot/1.0 (educational article metadata collection)"
HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
FIELDS = ["website_name", "url", "title", "heading", "paragraph", "author", "date", "scraped_at", "domain"]
ARTICLE_HINT = re.compile(r"/(?:20\d{2}/\d{1,2}/|article|news|story|technology|ai|machine-learning|science)/|\.html?$", re.I)
SKIP = re.compile(r"/(?:tag|tags|category|categories|author|authors|search|login|signup|about|contact|privacy|terms|video|videos|podcast|topics|newsletter)(?:/|$)|\.(?:jpg|png|gif|pdf|css|js)(?:$|\?)", re.I)
MAX_HTML_BYTES = 8_000_000
MAX_TRIES = 3


def parse_date(s):
    """Turn a site's date string into a timezone-aware datetime, or None (never invents a date)."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        try:
            d = parsedate_to_datetime(s)
        except (TypeError, ValueError):
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)      # assumption: no time zone given -> UTC
    return d


def migrate_csv(path):
    """Older articles.csv files have fewer columns. Rewrite them once with the current header
    (a .bak copy is kept); scraped_at stays empty for old rows, domain is filled from the URL."""
    if not path.exists() or path.stat().st_size == 0:
        return
    with path.open(encoding="utf-8-sig", newline="") as f:
        header = next(csv.reader(f), [])
    if header == FIELDS:
        return
    logging.info("Upgrading %s to the new column layout (backup: %s.bak)", path, path.name)
    shutil.copy2(path, str(path) + ".bak")
    tmp = Path(str(path) + ".tmp")
    with path.open(encoding="utf-8-sig", newline="") as src, tmp.open("w", encoding="utf-8-sig", newline="") as dst:
        w = csv.DictWriter(dst, fieldnames=FIELDS)
        w.writeheader()
        for r in csv.DictReader(src):
            row = {k: r.get(k, "") or "" for k in FIELDS}
            if not row["domain"] and row["url"]:
                row["domain"] = (urlparse(row["url"]).hostname or "").lower()
            w.writerow(row)
    os.replace(tmp, path)


def to_db_row(r):
    """CSV-style dict -> tuple for the table columns (empty text becomes NULL)."""
    def clean(v):
        return (v or "").replace("\x00", "").strip() or None     # PostgreSQL text cannot hold NUL bytes
    return (clean(r.get("website_name")), r["url"], clean(r.get("title")), clean(r.get("author")),
            parse_date(r.get("date")), clean(r.get("paragraph")))


class PgStore:
    """Saves articles into the PostgreSQL table (columns: website_name, url, title, author, publish_date, content)."""
    COLS = ("website_name", "url", "title", "author", "publish_date", "content")

    def __init__(self, a):
        self.params = dict(host=a.pg_host, port=a.pg_port, dbname=a.pg_db, user=a.pg_user,
                           password=a.pg_password, connect_timeout=10)
        self.table = a.pg_table
        self.conn = None
        self.connect()

    def connect(self):
        self.close()
        self.conn = psycopg2.connect(**self.params)

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def existing_urls(self):
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT url FROM {}").format(sql.Identifier(self.table)))
            urls = {row[0] for row in cur}
        self.conn.commit()
        return urls

    def insert(self, rows):
        """Insert CSV-style dicts; a URL that is already in the table is skipped (no error)."""
        data = [to_db_row(r) for r in rows]
        query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT (url) DO NOTHING").format(
            sql.Identifier(self.table), sql.SQL(", ").join(map(sql.Identifier, self.COLS)))
        for attempt in (1, 2):
            try:
                with self.conn.cursor() as cur:
                    execute_values(cur, query, data, page_size=500)
                self.conn.commit()
                return
            except Exception:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                if attempt == 2:
                    raise
                self.connect()                  # connection may have dropped: reconnect once and retry


class HostGate:
    """Per-host rate limiter. Reserves time slots so threads never hammer one host."""

    def __init__(self, delay):
        self.delay = delay
        self.lock = threading.Lock()
        self.next_slot = {}
        self.blocked_until = {}
        self.extra = defaultdict(float)

    def wait(self, host, crawl_delay=0.0):
        with self.lock:
            now = time.monotonic()
            gap = max(self.delay, crawl_delay) + self.extra[host]
            slot = max(now, self.next_slot.get(host, 0), self.blocked_until.get(host, 0))
            self.next_slot[host] = slot + gap * random.uniform(1.0, 1.3)
        if slot > now:
            time.sleep(slot - now)

    def penalize(self, host, seconds):
        """Host said slow down (429/503/403): pause it and permanently widen its gap a bit."""
        with self.lock:
            self.blocked_until[host] = max(self.blocked_until.get(host, 0), time.monotonic() + seconds)
            self.extra[host] = min(self.extra[host] + 0.5, 10.0)


class Scraper:
    def __init__(self, args):
        self.a = args
        self.gate = HostGate(args.delay)
        self.local = threading.local()
        self.robots = {}
        self.robots_lock = threading.Lock()
        self.lock = threading.Lock()
        self.urls_path = Path(args.urls)
        self.failed_path = Path(args.failed)
        self.known = set()                  # every discovered URL
        self.discovered = set()             # sites whose discovery already finished
        self.done = set()                   # URLs already saved in the output CSV
        self.failed = set()                 # URLs we gave up on
        self.pending = defaultdict(deque)   # site -> URLs still to fetch
        self.tries = defaultdict(int)
        self.pg = None                      # PgStore when saving to PostgreSQL

    # ------------------------------------------------------------------ HTTP
    def session(self):
        s = getattr(self.local, "s", None)
        if s is None:
            s = requests.Session()
            s.headers.update(HEADERS)
            adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8)
            s.mount("http://", adapter)
            s.mount("https://", adapter)
            self.local.s = s
        return s

    def robots_for(self, url):
        p = urlparse(url)
        root = f"{p.scheme}://{p.netloc}"
        with self.robots_lock:
            if root in self.robots:
                return self.robots[root]
            rp = RobotFileParser()
            try:
                r = self.session().get(root + "/robots.txt", timeout=12)
                if r.status_code == 200:
                    rp.parse(r.text.splitlines())
                elif r.status_code in (404, 410):
                    rp.parse([])            # RFC 9309: no robots.txt = no restrictions
                else:
                    rp = None               # 401/403/5xx: unknown -> be conservative, skip host
                    logging.warning("robots.txt HTTP %s for %s; skipping host", r.status_code, root)
            except requests.RequestException as exc:
                rp = None
                logging.warning("Cannot read robots.txt for %s: %s", root, exc)
            self.robots[root] = rp
            return rp

    def allowed(self, url):
        if urlparse(url).scheme not in ("http", "https"):
            return False
        rp = self.robots_for(url)
        return rp is not None and rp.can_fetch(UA, url)

    def crawl_delay(self, url):
        rp = self.robots_for(url)
        try:
            return float(rp.crawl_delay(UA) or 0) if rp else 0.0
        except (TypeError, ValueError):
            return 0.0

    def fetch(self, url):
        """Returns (response, status) with status in ok | retry | fail."""
        if not self.allowed(url):
            return None, "fail"
        host = urlparse(url).netloc
        for attempt in range(MAX_TRIES):
            self.gate.wait(host, self.crawl_delay(url))
            try:
                r = self.session().get(url, timeout=(8, 20))
            except requests.RequestException as exc:
                logging.debug("Request error %s: %s", url, exc)
                time.sleep(2 ** attempt)
                continue
            code = r.status_code
            if code == 200:
                return r, "ok"
            if code in (429, 503):
                try:
                    wait_s = min(float(r.headers.get("Retry-After", "")), 300)
                except ValueError:
                    wait_s = 30 * (attempt + 1)
                self.gate.penalize(host, wait_s)
                continue
            if code in (401, 403):
                self.gate.penalize(host, 30)    # back off, never try to bypass
                return None, "fail"
            if 500 <= code < 600:
                time.sleep(2 ** attempt)
                continue
            return None, "fail"                 # 404 etc.
        return None, "retry"

    # ------------------------------------------------------------ URL helpers
    @staticmethod
    def same_site(url, base):
        a = (urlparse(url).hostname or "").removeprefix("www.")
        b = (urlparse(base).hostname or "").removeprefix("www.")
        return a == b

    @staticmethod
    def clean(url):
        return urldefrag(url)[0].split("?")[0].rstrip("/")

    def links(self, html, page_url, base):
        soup = BeautifulSoup(html, PARSER)
        found = []
        for a in soup.select("a[href]"):
            url = self.clean(urljoin(page_url, a.get("href", "")))
            if self.same_site(url, base) and not SKIP.search(urlparse(url).path):
                found.append(url)
        return list(dict.fromkeys(found))

    # -------------------------------------------------------------- discovery
    def save_urls(self, name, urls):
        """Append newly discovered URLs to article_urls.csv and queue them."""
        with self.lock:
            new = [u for u in urls if u not in self.known]
            if new:
                is_new_file = not self.urls_path.exists() or self.urls_path.stat().st_size == 0
                with self.urls_path.open("a", encoding="utf-8-sig", newline="") as f:
                    w = csv.writer(f)
                    if is_new_file:
                        w.writerow(["website_name", "url"])
                    w.writerows((name, u) for u in new)
                self.known.update(new)
                self.pending[name].extend(new)
            self.discovered.add(name)

    def discover(self, name, base, target):
        found, seen = [], set()

        def add(u):
            u = self.clean(u)
            if u not in seen and self.same_site(u, base) and not SKIP.search(urlparse(u).path):
                seen.add(u)
                found.append(u)

        full = lambda: len(found) >= target  # noqa: E731

        # 1) sitemaps (robots.txt directives + common paths), BFS through indexes
        rp = self.robots_for(base)
        sitemaps = list(rp.site_maps() or []) if rp else []
        sitemaps += [urljoin(base, "/sitemap.xml"), urljoin(base, "/sitemap_index.xml"), urljoin(base, "/news-sitemap.xml")]
        queue, checked = deque(dict.fromkeys(sitemaps)), set()
        while queue and len(checked) < self.a.max_sitemaps and not full():
            sm = queue.popleft()
            if sm in checked:
                continue
            checked.add(sm)
            resp, st = self.fetch(sm)
            if resp is None:
                continue
            data = resp.content
            try:
                if data[:2] == b"\x1f\x8b":
                    data = gzip.decompress(data)
                root = ET.fromstring(data)
            except (ET.ParseError, OSError, EOFError):
                continue
            locs = [n.text.strip() for n in root.iter() if n.tag.endswith("}loc") and n.text]
            if root.tag.endswith("sitemapindex"):
                queue.extend(locs)
            else:
                for u in locs:
                    if ARTICLE_HINT.search(urlparse(u).path):
                        add(u)
                    if full():
                        break
            if len(checked) % 20 == 0:
                logging.info("%s: %s candidates after %s sitemaps", name, len(found), len(checked))

        # 2) RSS / Atom
        for path in ("/feed", "/rss", "/rss.xml", "/feed.xml", "/atom.xml"):
            if full():
                break
            resp, _ = self.fetch(urljoin(base, path))
            if resp is not None:
                for entry in feedparser.parse(resp.content).entries:
                    if entry.get("link"):
                        add(entry.link)

        # 3) listing pages
        lq, visited = deque([(base, 0)]), set()
        while lq and len(visited) < self.a.max_listing_pages and not full():
            page, depth = lq.popleft()
            if page in visited:
                continue
            visited.add(page)
            resp, _ = self.fetch(page)
            if resp is None:
                continue
            for link in self.links(resp.text, page, base):
                if ARTICLE_HINT.search(urlparse(link).path):
                    add(link)
                elif depth < 2 and re.search(r"/(?:page/\d+|news|ai|technology|science|latest|archive)(?:/|$)", link, re.I):
                    lq.append((link, depth + 1))

        self.save_urls(name, found)
        logging.info("%s: discovery done, %s candidate URLs", name, len(found))

    def discovery_phase(self, sources):
        # candidates to collect per site; extra headroom because many candidate URLs fail (not articles, blocked...)
        target = self.a.candidates_per_site or math.ceil(self.a.total / len(sources) * 2)
        todo = list(sources) if self.a.rediscover else [s for s in sources if s["name"] not in self.discovered]
        if not todo:
            return
        with ThreadPoolExecutor(max_workers=len(todo)) as ex:
            futs = {ex.submit(self.discover, s["name"], s["url"], target): s for s in todo}
            for fut, s in futs.items():
                try:
                    fut.result()
                except Exception as exc:
                    logging.warning("Discovery failed for %s: %s", s["name"], exc)

    # ------------------------------------------------------------- extraction
    @staticmethod
    def metadata(soup, *names):
        for name in names:
            tag = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"):
                return tag["content"].strip()
        return ""

    @staticmethod
    def jsonld_articles(soup):
        out = []

        def walk(obj):
            if isinstance(obj, list):
                for x in obj:
                    walk(x)
            elif isinstance(obj, dict):
                typ = obj.get("@type", [])
                types = typ if isinstance(typ, list) else [typ]
                if any(t in ("Article", "NewsArticle", "BlogPosting", "TechArticle") for t in types):
                    out.append(obj)
                for val in obj.values():
                    if isinstance(val, (list, dict)):
                        walk(val)

        for script in soup.select('script[type="application/ld+json"]'):
            try:
                walk(json.loads(script.string or script.get_text()))
            except (ValueError, TypeError):
                continue
        return out

    @staticmethod
    def author_name(v):
        if isinstance(v, str):
            return v
        if isinstance(v, dict):
            return v.get("name", "")
        if isinstance(v, list):
            return ", ".join(filter(None, [Scraper.author_name(x) for x in v]))
        return ""

    @staticmethod
    def ld_value(structured, key):
        """First non-empty value of `key` across ALL JSON-LD article objects (not just the first one)."""
        for obj in structured:
            v = obj.get(key)
            if v:
                return v
        return ""

    def find_author(self, soup, structured):
        cands = [self.author_name(self.ld_value(structured, "author")),
                 self.metadata(soup, "author", "article:author", "parsely-author", "sailthru.author", "dc.creator", "DC.creator")]
        for c in cands:
            c = (c or "").strip()
            if c and not c.lower().startswith("http"):      # some sites put a profile URL here, not a name
                return c
        for sel in ('[rel="author"]', '[itemprop="author"]', '[class*="byline"]', '[class*="author-name"]', '[data-testid="byline"]'):
            tag = soup.select_one(sel)
            if tag:
                txt = re.sub(r"^\s*by\s+", "", tag.get_text(" ", strip=True), flags=re.I)
                if 2 <= len(txt) <= 120:
                    return txt
        return ""                                            # never invent a missing author

    def find_date(self, soup, structured):
        d = self.ld_value(structured, "datePublished")
        if isinstance(d, str) and d.strip():
            return d.strip()
        d = self.metadata(soup, "article:published_time", "og:article:published_time", "datePublished", "pubdate",
                          "publishdate", "date", "dc.date", "DC.date.issued", "parsely-pub-date", "sailthru.date")
        if d:
            return d
        t = soup.find("time", datetime=True)
        return t.get("datetime", "").strip() if t else ""    # never invent a missing date

    def extract(self, name, url):
        resp, st = self.fetch(url)
        if resp is None:
            return st, None
        if "html" not in resp.headers.get("Content-Type", "").lower() or len(resp.content) > MAX_HTML_BYTES:
            return "fail", None
        soup = BeautifulSoup(resp.text, PARSER)
        structured = self.jsonld_articles(soup)
        h1 = soup.select_one("h1")
        heading = h1.get_text(" ", strip=True) if h1 else ""
        title = (self.metadata(soup, "og:title", "twitter:title") or self.ld_value(structured, "headline")
                 or (soup.title.get_text(" ", strip=True) if soup.title else "") or heading)
        author = self.find_author(soup, structured)
        date = self.find_date(soup, structured)
        ld = structured[0] if structured else {}
        body = soup.select_one("article") or soup.select_one("main") or soup.body
        paragraphs = []
        if body:
            for bad in body.select("script,style,nav,footer,aside,form,header,figcaption"):
                bad.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in body.select("p")]
            paragraphs = [p for p in paragraphs if len(p) >= 45]
        if self.a.max_paragraphs > 0:
            paragraphs = paragraphs[:self.a.max_paragraphs]
        paragraph = "\n".join(paragraphs)          # full article text
        if not paragraph and isinstance(ld.get("articleBody"), str):
            paragraph = ld["articleBody"].strip()
        if not heading or not paragraph:
            return "fail", None
        return "done", {"website_name": name, "url": url, "title": str(title), "heading": heading,
                        "paragraph": paragraph, "author": author, "date": str(date),
                        "scraped_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                        "domain": (urlparse(url).hostname or "").lower()}

    # ------------------------------------------------------------------- run
    def load_state(self, output, db_urls=()):
        """Rebuild progress from the plain files, so a restart continues where it stopped."""
        by_site = defaultdict(list)
        if self.urls_path.exists() and self.urls_path.stat().st_size > 0:
            with self.urls_path.open(encoding="utf-8-sig", newline="") as f:
                for r in csv.DictReader(f):
                    u = r.get("url")
                    if u and u not in self.known:
                        self.known.add(u)
                        by_site[r.get("website_name", "")].append(u)
        if output.exists() and output.stat().st_size > 0:
            with output.open(encoding="utf-8-sig", newline="") as f:
                self.done = {r["url"] for r in csv.DictReader(f) if r.get("url")}
        self.done |= set(db_urls)
        if self.failed_path.exists():
            self.failed = {ln.strip() for ln in self.failed_path.read_text(encoding="utf-8").splitlines() if ln.strip()}
        for site, urls in by_site.items():
            self.discovered.add(site)
            self.pending[site].extend(u for u in urls if u not in self.done and u not in self.failed)
        logging.info("Loaded state: %s URLs known, %s already saved, %s failed", len(self.known), len(self.done), len(self.failed))

    def sync_csv_to_db(self, output, db_urls):
        """Copy articles that are in the CSV but not yet in the database (e.g. from earlier runs)."""
        if not output.exists() or output.stat().st_size == 0:
            return
        buf, total = [], 0
        try:
            with output.open(encoding="utf-8-sig", newline="") as f:
                for r in csv.DictReader(f):
                    if r.get("url") and r["url"] not in db_urls:
                        buf.append(r)
                        if len(buf) >= 500:
                            self.pg.insert(buf)
                            total += len(buf)
                            buf = []
            if buf:
                self.pg.insert(buf)
                total += len(buf)
        except Exception as exc:
            logging.warning("Could not copy the old CSV rows into the database: %s", exc)
            return
        if total:
            logging.info("Copied %s articles from %s into the database", total, output)

    def save_to_db(self, rows):
        try:
            self.pg.insert(rows)
            return True
        except Exception as exc:
            logging.error("Database save FAILED (%s). The articles are safe in the CSV and will be copied "
                          "to the database on the next run.", exc)
            return False

    def next_url(self, site):
        q = self.pending[site]
        while q:
            u = q.popleft()
            if u not in self.done and u not in self.failed:
                return u
        return None

    def run(self):
        sources = json.load(open(self.a.sources, encoding="utf-8"))["sources"]
        names = [s["name"] for s in sources]
        output = Path(self.a.output)
        migrate_csv(output)
        db_urls = set()
        if not self.a.no_db:
            try:
                self.pg = PgStore(self.a)
                db_urls = self.pg.existing_urls()
            except Exception as exc:
                raise SystemExit(f"Cannot use the PostgreSQL table '{self.a.pg_table}' in database '{self.a.pg_db}': {exc}\n"
                                 "Check host/port/user/password/database/table name, or run with --no-db to save to CSV only.")
            logging.info("Connected to PostgreSQL database '%s' (%s articles already in table '%s')",
                         self.a.pg_db, len(db_urls), self.a.pg_table)
        self.load_state(output, db_urls)
        if self.pg:
            self.sync_csv_to_db(output, db_urls)

        if not self.a.skip_discovery:
            self.discovery_phase(sources)

        saved = len(self.done)
        logging.info("Starting extraction: %s saved so far, target %s", saved, self.a.total)
        new_file = not output.exists() or output.stat().st_size == 0
        started, session_saved = time.monotonic(), 0
        inflight, per_site = {}, defaultdict(int)
        batch_rows, failed_buf = [], []
        interrupted = False
        stats = {"n": 0, "author": 0, "date": 0}

        with output.open("a", encoding="utf-8-sig", newline="") as f, ThreadPoolExecutor(max_workers=self.a.workers) as ex:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if new_file:
                writer.writeheader()

            def flush_batch():
                """Write the buffered articles to the CSV in one go (plus any newly failed URLs)."""
                if failed_buf:
                    with self.failed_path.open("a", encoding="utf-8") as ff:
                        ff.write("\n".join(failed_buf) + "\n")
                    failed_buf.clear()
                if not batch_rows:
                    return
                writer.writerows(batch_rows)
                f.flush()
                os.fsync(f.fileno())
                db_ok = self.save_to_db(batch_rows) if self.pg else None
                where = "CSV" if db_ok is None else ("CSV + database" if db_ok else "CSV (DATABASE FAILED)")
                self.done.update(r["url"] for r in batch_rows)
                n = len(batch_rows)
                batch_rows.clear()
                rate = session_saved / max(time.monotonic() - started, 1)
                eta = (self.a.total - saved) / rate / 3600 if rate else 0
                logging.info("Batch of %s saved to %s -> %s/%s saved (%.1f articles/s, ETA %.1f h) | with author %.0f%%, with date %.0f%%",
                             n, where, saved, self.a.total, rate, eta,
                             100 * stats["author"] / max(stats["n"], 1), 100 * stats["date"] / max(stats["n"], 1))

            try:
                while True:
                    # keep every host busy, but never more than --per-host requests at once per host
                    if saved + len(inflight) < self.a.total:
                        for site in names:
                            while per_site[site] < self.a.per_host and saved + len(inflight) < self.a.total:
                                url = self.next_url(site)
                                if not url:
                                    break
                                inflight[ex.submit(self.extract, site, url)] = (site, url)
                                per_site[site] += 1
                    if not inflight:
                        break
                    finished, _ = wait(list(inflight), timeout=5, return_when=FIRST_COMPLETED)
                    for fut in finished:
                        site, url = inflight.pop(fut)
                        per_site[site] -= 1
                        try:
                            st, art = fut.result()
                        except Exception as exc:
                            logging.warning("Error parsing %s: %s", url, exc)
                            st, art = "fail", None
                        if st == "done":
                            batch_rows.append(art)
                            saved += 1
                            session_saved += 1
                            stats["n"] += 1
                            stats["author"] += bool(art["author"])
                            stats["date"] += bool(art["date"])
                            if len(batch_rows) >= self.a.batch_size:
                                flush_batch()
                            elif len(batch_rows) % 1000 == 0:
                                logging.info("Collected %s/%s articles for the next batch (%s/%s total)",
                                             len(batch_rows), self.a.batch_size, saved, self.a.total)
                        elif st == "retry":
                            self.tries[url] += 1
                            if self.tries[url] >= MAX_TRIES:
                                self.failed.add(url)
                                failed_buf.append(url)
                            else:
                                self.pending[site].append(url)
                        else:
                            self.failed.add(url)
                            failed_buf.append(url)
            except KeyboardInterrupt:
                interrupted = True
                logging.info("Interrupted - saving the last partial batch, then you can rerun the same command to resume.")
                for fut in inflight:
                    fut.cancel()
            finally:
                flush_batch()                      # never lose the last (<batch-size) articles
        logging.info("Finished: %s articles saved in %s", saved, output)
        if saved < self.a.total and not interrupted:
            logging.warning("Only %s of %s saved: no candidate URLs are left (%s URLs failed/were skipped). To collect more, run again with: "
                            "--rediscover --candidates-per-site 30000  (or add more websites to sources.json).",
                            saved, self.a.total, len(self.failed))

    def close(self):
        if self.pg:
            self.pg.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Fast, resumable, polite multi-source news scraper")
    p.add_argument("--sources", default="sources.json")
    p.add_argument("--output", default="articles.csv")
    p.add_argument("--urls", default="article_urls.csv", help="discovered URLs (used to resume)")
    p.add_argument("--failed", default="failed_urls.txt", help="URLs that failed/blocked (skipped on resume)")
    p.add_argument("--total", type=int, default=100_000, help="stop after this many saved articles (all sources combined)")
    p.add_argument("--workers", type=int, default=40, help="thread pool size")
    p.add_argument("--per-host", type=int, default=4, help="max simultaneous requests per website")
    p.add_argument("--delay", type=float, default=1.0, help="min seconds between requests to the same host")
    p.add_argument("--batch-size", type=int, default=10_000, help="write to the CSV (and database) after this many articles")
    p.add_argument("--max-sitemaps", type=int, default=400)
    p.add_argument("--max-paragraphs", type=int, default=0, help="keep at most N paragraphs per article (0 = all)")
    p.add_argument("--max-listing-pages", type=int, default=30)
    p.add_argument("--skip-discovery", action="store_true")
    p.add_argument("--candidates-per-site", type=int, default=0,
                   help="how many article URLs to collect per website in discovery (0 = automatic from --total)")
    p.add_argument("--rediscover", action="store_true", help="search for more URLs again on all sites (adds only new ones)")
    p.add_argument("--no-db", action="store_true", help="(default) save to CSV only, skip PostgreSQL")
    p.add_argument("--db", action="store_true", help="ALSO save to PostgreSQL (off by default)")
    p.add_argument("--pg-host", default="localhost")
    p.add_argument("--pg-port", type=int, default=5432)
    p.add_argument("--pg-db", default="scraper", help="PostgreSQL database name")
    p.add_argument("--pg-user", default="postgres")
    p.add_argument("--pg-password", default=None, help="better not to type it here: leave empty and you will be asked")
    p.add_argument("--pg-table", default="newsscraper")
    args = p.parse_args()
    args.no_db = not args.db          # CSV only unless --db is given
    if args.batch_size < 1:
        p.error("--batch-size must be >= 1")
    if args.delay < 1:
        p.error("Use --delay >= 1 second")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not args.no_db:
        if psycopg2 is None:
            p.error("psycopg2 is not installed. Run: pip install psycopg2-binary   (or use --no-db)")
        if not args.pg_password:
            args.pg_password = os.environ.get("PGPASSWORD") or getpass.getpass(f"PostgreSQL password for user '{args.pg_user}': ")
    s = Scraper(args)
    try:
        s.run()
    finally:
        s.close()