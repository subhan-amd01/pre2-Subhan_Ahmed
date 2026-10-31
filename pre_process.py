"""
LimeoX-style preprocessing for scraped news articles.

Input columns : website_name, url, title, heading, paragraph, author, date, scraped_at, domain
Main output   : preprocessed_output/news_clean.csv   (same 9 columns, cleaned)
Also written  : news_review.csv, news_rejected.csv, news_quality_report.csv,
                news_preprocessing_summary.json   (as in the LimeoX notebook, sections 11-14)

Usage:
    python preprocess_news.py new_articles.csv
    python preprocess_news.py new_articles.csv --out preprocessed_output

Policy (from the LimeoX notebook):
  * raw file is never modified
  * missing author / date is NOT fatal (author -> informational, date -> review)
  * rows are rejected only for: invalid URL, missing title, missing content, duplicate
  * nothing is invented or truncated; content stays complete
  * dates are parsed with format="mixed", converted to UTC, written as ISO strings
"""

import argparse
import html
import json
import re
import unicodedata
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import numpy as np
import pandas as pd

# ----------------------------------------------------------------- settings
COLUMNS = ["website_name", "url", "title", "heading", "paragraph",
           "author", "date", "scraped_at", "domain"]

# the notebook's logical names -> this project's column names
ALIASES = {
    "url": ["news_url", "article_url", "link", "source_url", "shared_url", "news_link"],
    "title": ["headline", "article_title", "news_title"],
    "paragraph": ["content", "text", "article_content", "body", "article_text",
                  "full_text", "news_content"],
    "author": ["writer", "byline", "article_author", "authors"],
    "date": ["published_time", "time", "published", "published_date",
             "publication_time", "datetime", "timestamp"],
}

MISSING_TOKENS = {"", "n/a", "na", "null", "none", "nan", "-", "--"}
TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term",
                   "utm_content", "fbclid", "gclid"}
MIN_TITLE_CHARS = 8
MIN_CONTENT_CHARS = 200      # tune after inspecting real articles
ERROR_REGEX = re.compile(
    r"access denied|enable javascript|captcha|robot check|page not found|"
    r"403 forbidden|subscribe to continue", re.I)
INVISIBLE = dict.fromkeys(map(ord, "\u200b\u200c\u200d\u2060\ufeff\u00ad"), None)


# ----------------------------------------------------------------- cleaning
def standardize_name(name):
    name = unicodedata.normalize("NFKC", str(name)).strip().lower()
    return re.sub(r"[^a-z0-9]+", "_", name).strip("_")


def is_present(s):
    s = s.astype(object)
    return s.notna() & s.map(lambda v: not (isinstance(v, str) and v.strip() == ""))


def coalesce(frame, canonical, aliases):
    """Merge a canonical column and its aliases into one (first non-blank wins)."""
    names = [canonical] + aliases
    pos = [i for i, c in enumerate(frame.columns) if c in names]
    if not pos:
        return frame
    pos.sort(key=lambda i: names.index(frame.columns[i]))
    block = frame.iloc[:, pos].astype(object)
    merged = block.iloc[:, 0]
    for k in range(1, block.shape[1]):
        merged = merged.where(is_present(merged), block.iloc[:, k])
    rest = frame.iloc[:, [i for i in range(frame.shape[1]) if i not in pos]]
    return pd.concat([pd.DataFrame({canonical: merged}), rest], axis=1)


def normalize_missing(v):
    if pd.isna(v):
        return pd.NA
    if isinstance(v, str):
        v = v.replace("\u00a0", " ").strip()
        if v.casefold() in MISSING_TOKENS:
            return pd.NA
    return v


class _Text(HTMLParser):
    SKIP = {"script", "style", "noscript"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP:
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in self.SKIP and self.skip:
            self.skip -= 1

    def handle_data(self, data):
        if not self.skip and data and data.strip():
            self.parts.append(data.strip())


def clean_text(value):
    """strip HTML -> unescape -> NFKC -> drop invisible chars -> straight quotes -> 1 space."""
    if pd.isna(value):
        return pd.NA
    text = str(value)
    p = _Text()
    try:
        p.feed(text)
        p.close()
        text = " ".join(p.parts)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text).translate(INVISIBLE)
    text = (text.replace("\u201c", '"').replace("\u201d", '"')
                .replace("\u2018", "'").replace("\u2019", "'"))
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else pd.NA


def normalize_url(value):
    if pd.isna(value):
        return pd.NA
    v = str(value).strip()
    if not v or not re.match(r"^https?://", v, re.I) or re.search(r"\s", v):
        return pd.NA
    try:
        p = urlparse(v)
        if not p.hostname or "." not in p.hostname:
            return pd.NA
        scheme, host = p.scheme.lower(), p.hostname.lower()
        netloc = host
        if p.port and not ((scheme == "http" and p.port == 80) or
                           (scheme == "https" and p.port == 443)):
            netloc = f"{host}:{p.port}"
        path = re.sub(r"/{2,}", "/", p.path or "/")
        query = urlencode([(k, x) for k, x in parse_qsl(p.query, keep_blank_values=True)
                           if k.lower() not in TRACKING_PARAMS], doseq=True)
        return urlunparse((scheme, netloc, path, "", query, ""))
    except (ValueError, TypeError):
        return pd.NA


def parse_ts(series):
    try:
        return pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    except (TypeError, ValueError):
        return pd.to_datetime(series, errors="coerce", utc=True)


def word_count(v):
    return len(re.findall(r"\b[\w'-]+\b", v)) if isinstance(v, str) else 0


# ----------------------------------------------------------------- pipeline
def preprocess(raw_df):
    df = raw_df.copy(deep=True)
    df.columns = [standardize_name(c) for c in df.columns]
    for canonical, aliases in ALIASES.items():
        df = coalesce(df, canonical, aliases)
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # 1) missing tokens + text cleaning
    for col in COLUMNS:
        df[col] = df[col].map(normalize_missing)
    for col in ["website_name", "title", "heading", "paragraph", "author"]:
        df[col] = df[col].map(clean_text)

    # 2) URL + domain (domain is re-derived from the cleaned URL)
    df["url"] = df["url"].map(normalize_url)
    df["domain"] = df["url"].map(lambda x: urlparse(x).hostname if pd.notna(x) else pd.NA)

    # 3) dates -> UTC ISO string (original text kept in date_raw for the audit files)
    df["date_raw"] = df["date"]
    df["date"] = parse_ts(df["date"])
    df["scraped_at"] = parse_ts(df["scraped_at"])

    # 4) quality features
    df["title_chars"] = df["title"].map(lambda v: len(v) if isinstance(v, str) else 0)
    df["content_chars"] = df["paragraph"].map(lambda v: len(v) if isinstance(v, str) else 0)
    df["content_words"] = df["paragraph"].map(word_count)
    block = df["paragraph"].map(lambda x: bool(ERROR_REGEX.search(x)) if isinstance(x, str) else False)

    # 5) duplicates
    df["dup_url"] = df["url"].notna() & df.duplicated(subset=["url"], keep="first")
    sig = (df["title"].fillna("").astype(str).str.casefold().str.strip() + "|" +
           df["paragraph"].fillna("").astype(str).str.casefold().str.strip())
    df["dup_text"] = sig.duplicated(keep="first") & (df["title"].notna() | df["paragraph"].notna())
    df["is_duplicate"] = df["dup_url"] | df["dup_text"]

    # 6) issues (critical / review / info)
    miss_url, miss_title = df["url"].isna(), df["title"].isna()
    miss_body, miss_date = df["paragraph"].isna(), df["date"].isna()
    miss_author = df["author"].isna()
    critical, review, info = [], [], []
    for i in range(len(df)):
        c, r, n = [], [], []
        if miss_url.iat[i]:
            c.append("missing_or_invalid_url")
        if miss_title.iat[i]:
            c.append("missing_title")
        elif df["title_chars"].iat[i] < MIN_TITLE_CHARS:
            r.append("short_title")
        if miss_body.iat[i]:
            c.append("missing_content")
        elif df["content_chars"].iat[i] < MIN_CONTENT_CHARS:
            r.append("short_content")
        if miss_date.iat[i]:
            r.append("missing_or_invalid_date")
        if block.iat[i]:
            r.append("possible_block_or_paywall")
        if df["dup_url"].iat[i]:
            c.append("duplicate_url")
        if df["dup_text"].iat[i]:
            c.append("duplicate_text")
        if miss_author.iat[i]:
            n.append("missing_author")
        critical.append(";".join(c))
        review.append(";".join(r))
        info.append(";".join(n))
    df["reject_reason"], df["review_issues"], df["info_issues"] = critical, review, info
    return df


def export_csv(frame, path, cols):
    out = frame[cols].copy()
    for c in ["date", "scraped_at"]:
        if c in out.columns:
            out[c] = out[c].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    out.to_csv(path, index=False, encoding="utf-8-sig")


def main():
    ap = argparse.ArgumentParser(description="LimeoX preprocessing for scraped news CSV")
    ap.add_argument("input", help="scraped CSV (or .xlsx)")
    ap.add_argument("--out", default="preprocessed_output")
    args = ap.parse_args()

    src = Path(args.input)
    raw = (pd.read_excel(src) if src.suffix.lower() in {".xlsx", ".xls"}
           else pd.read_csv(src, encoding="utf-8-sig", low_memory=False))
    print(f"Loaded {len(raw):,} rows, columns: {list(raw.columns)}")

    df = preprocess(raw)
    rejected_mask = df["reject_reason"].ne("")
    clean_df, rejected_df = df[~rejected_mask], df[rejected_mask]
    review_df = df[df["review_issues"].ne("") & ~rejected_mask]
    assert len(clean_df) + len(rejected_df) == len(df) == len(raw), "Row counts do not reconcile"

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    extra = ["content_words", "review_issues", "info_issues"]
    export_csv(clean_df, out / "news_clean.csv", COLUMNS + extra)
    export_csv(review_df, out / "news_review.csv", COLUMNS + extra)
    export_csv(rejected_df, out / "news_rejected.csv", COLUMNS + ["date_raw", "reject_reason"])

    # field-level completeness (notebook section 12)
    rows = []
    for col in COLUMNS:
        nn = int(df[col].notna().sum())
        rows.append({"field": col, "total_rows": len(df), "non_missing": nn,
                     "missing": len(df) - nn,
                     "completeness_pct": round(nn / len(df) * 100, 2) if len(df) else np.nan,
                     "distinct_non_missing": int(df[col].nunique(dropna=True))})
    pd.DataFrame(rows).to_csv(out / "news_quality_report.csv", index=False, encoding="utf-8-sig")

    summary = {
        "input_file": str(src), "raw_rows": int(len(raw)),
        "clean_rows": int(len(clean_df)), "review_rows": int(len(review_df)),
        "rejected_rows": int(len(rejected_df)),
        "invalid_or_missing_urls": int(df["url"].isna().sum()),
        "missing_titles": int(df["title"].isna().sum()),
        "missing_content": int(df["paragraph"].isna().sum()),
        "missing_dates": int(df["date"].isna().sum()),
        "missing_authors": int(df["author"].isna().sum()),
        "duplicate_url_rows": int(df["dup_url"].sum()),
        "duplicate_text_rows": int(df["dup_text"].sum()),
        "rejected_by_reason": {k: int(v) for k, v in
                               rejected_df["reject_reason"].str.split(";").explode().value_counts().items()},
    }
    (out / "news_preprocessing_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nClean CSV: {out / 'news_clean.csv'}")


if __name__ == "__main__":
    main()