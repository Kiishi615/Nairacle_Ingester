"""
Standalone daily ingestion pipeline — Scrape → Backfill → Clean → Classify → Upsert.

Orchestrates the full daily workflow with updated Nairametrics selectors:
  1. Scrape new articles (multithreaded Playwright, new 3-section layout)
  2. Backfill articles with missing content (3 retries)
  3. Remove articles that still fail after retries
  4. Deduplicate by URL
  5. Clean boilerplate from content
  6. Classify with Gemini 3.1 Flash-Lite (structured output)
  7. Upsert kept articles to Pinecone vectorstore
  8. Generate pipeline report

Usage:
    python daily_ingest.py                  # full pipeline
    python daily_ingest.py --scrape-only    # just scrape + backfill + clean
    python daily_ingest.py --classify-only  # classify + upsert (skip scrape)
    python daily_ingest.py --limit 5        # classify only N articles (for testing)
"""

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from queue import Queue
from typing import Literal

from dotenv import load_dotenv
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from pydantic import BaseModel, Field

# ═══════════════════════════════════════════════════════════════════
#  INLINE MODELS & LOGGING (standalone — no local imports)
# ═══════════════════════════════════════════════════════════════════


class ArticleClassification(BaseModel):
    """Structured output from LLM classification."""

    decision: Literal["keep", "discard"] = Field(
        description="Whether to keep or discard the article"
    )
    summary: str | None = Field(
        None, description="2-3 sentence summary if kept, null if discarded"
    )


def _setup_logging(log_dir: str = "logs") -> None:
    Path(log_dir).mkdir(exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    fh = RotatingFileHandler(
        Path(log_dir) / "daily_ingest.log", maxBytes=10_000_000, backupCount=5
    )
    fh.setLevel(logging.INFO)
    logging.Formatter.converter = time.gmtime
    fh.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s | %(message)s"
        )
    )
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logger.addHandler(ch)
    for noisy in ("httpx", "httpcore", "openai", "langchain", "pinecone", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


_setup_logging()
log = logging.getLogger("daily_ingest")

# ═══════════════════════════════════════════════════════════════════
#  PATHS & CONSTANTS
# ═══════════════════════════════════════════════════════════════════

PROJECT_DIR = Path(__file__).resolve().parent
ENV_PATH = PROJECT_DIR.parent / ".env"
load_dotenv(ENV_PATH, override=True)
load_dotenv(PROJECT_DIR / ".env", override=True)

# Strip whitespace/newlines from secrets (GitHub Secrets copy-paste artefact)
for _key in ("GOOGLE_API_KEY", "OPENAI_API_KEY", "PINECONE_API_KEY",
             "PINECONE_INDEX_NAME", "TELEGRAM_API_KEY", "TELEGRAM_CHAT_ID"):
    _val = os.environ.get(_key)
    if _val:
        os.environ[_key] = _val.strip()

INPUT_FILE = PROJECT_DIR / "menu_links.json"
VISITED_FILE = PROJECT_DIR / "visited_urls.txt"
MASTER_FILE = PROJECT_DIR / "nairametrics_articles.jsonl"
SCRAPED_FILE = PROJECT_DIR / "scraped_today.jsonl"
CLASSIFIED_FILE = PROJECT_DIR / "classified_today.jsonl"
REPORT_FILE = PROJECT_DIR / "pipeline_report.json"

MAX_RETRIES = 3
RETRY_BACKOFF = 5
PAGE_DELAY = 0.1
NUM_THREADS = 4
MAX_PAGES_PER_CATEGORY = 10
MAX_BACKFILL_RETRIES = 3

CLASSIFY_MODEL = os.getenv("CLASSIFY_MODEL", "google_genai:gemini-3.1-flash-lite")
CLASSIFY_DELAY = 4.5  # seconds between calls (rate-limit safe for free tier)
CLASSIFY_MAX_RETRIES = 3  # retries per article on rate-limit / transient errors
EMBED_BATCH_SIZE = 100

# ── Selectors (easy to update when the site changes) ─────────────
SELECTORS = {
    "post_title": "article.cover p.post-title a",
    "card_title": "div.news-card div.card-title a",
    "story_title": "div.story-row div.story-body p.story-title a",
    "pagination": "ul.page-numbers a.page-numbers",
    "article_title": "h1.post-title",
    "article_content": "div.post-content",
    "article_date": "p.post-date time",
    "article_category": "a.category",
}

# ── Content cleaning patterns ────────────────────────────────────
SECTION_HEADERS = [
    "MoreStories",
    "More Stories",
    "What you should know",
    "What You Should Know",
    "Get up to speed",
    "More insights",
    "What they are saying",
    "What we are saying",
    "What this means",
    "What the statement is saying",
    "What he said",
    "What she said",
    "More Insights",
]
FOOTER_TRIGGERS = [
    "Add Nairametrics on Google News",
    "Follow us for Breaking News",
    "Tags:",
]
MORE_STORIES_RE = re.compile(r"More\s*Stories?\s*\n(?:.*\n)*?\s*\n", re.IGNORECASE)
PARAGRAPH_PREFIXES = [
    "Octa is an international broker that has been providing online trading services",
    "Since its foundation, Octa has won more than",
    "Since its foundation, Octa has also won more than",
    "For media inquiries, please contact:",
    "For media inquiries, please contact MEXC",
    "For further information, please contact:",
    "Disclaimer: This article does not contain or constitute investment advice",
    "Watch out for the 2024 Money Counsellors Annual Report on Pensions.",
]

SYSTEM_PROMPT = """\
You are a financial news filter for a Nigerian stock market analysis system.

CONTEXT:
You are processing articles scraped from Nairametrics, a Nigerian financial \
news outlet covering the Nigerian Stock Exchange (NGX), Central Bank of \
Nigeria (CBN) policy, corporate earnings, macroeconomic data, and sectoral \
developments. Your task is to classify each article as KEEP or DISCARD based \
on whether it contains actionable financial intelligence for equity analysts, \
portfolio managers, or institutional investors focused on Nigerian markets.

KEEP if the article contains at least one of the following:
- A specific NGX-listed company's financials: revenue, profit/loss, EPS, \
dividends, net asset value (NAV), share price movement, or quarterly/annual results.
- A CBN policy decision or directive: MPR changes, CRR adjustments, LDR \
mandates, foreign exchange policy changes, bank licensing decisions, payment \
system regulations, or capital requirement directives.
- Naira/FX movement with specific figures: official window rates, NAFEM \
closing rates, parallel market rates, BDC rates, or interbank rates.
- Oil and gas sector data: Nigerian crude oil production volumes (bpd), \
OPEC+ quota allocations, crude oil benchmark prices with impact on Federation Account revenue.
- A sector-wide regulatory or structural change with direct market consequence.
- Government fiscal action with direct market impact: budget allocations \
with specific figures, FGN bond auctions, Treasury bill rates, Eurobond issuance, \
DMO debt data, tax policy changes, or subsidy removal with quantified fiscal savings.
- A corporate event with financial consequence: M&A, rights issues, share \
buybacks, major contract wins above N1 billion, CEO/CFO changes at listed firms, \
dividend declarations, delisting, sanctions, or license revocations.
- Macroeconomic data releases: NBS inflation figures, GDP growth rates, \
trade balance data, unemployment statistics, PMI readings, CBN reserves data, \
or DMO public debt figures.
- Capital market developments: SEC regulatory changes, NGX ASI movement \
with percentage changes, new IPO filings, FPI inflow/outflow data.

DISCARD if the article is:
- A press release, product launch, app update, or CSR announcement with no hard financial figures.
- An award ceremony, industry ranking, personality profile, or listicle.
- A prediction or opinion with no supporting data or policy reference.
- International news with no direct connection to Nigeria, the NGX, or the naira.
- Under 100 words of substantive financial content.
- Cryptocurrency/Web3 content with no direct NGX or CBN consequence.
- Real estate coverage unless involving an NGX-listed REIT.
- Lifestyle, entertainment, sports, or human-interest content.
- Job listings, career advice, or educational program announcements.

SUMMARY QUALITY RULES:
- Lead with the most important number or policy change.
- Name the specific company, regulator, or data source.
- Include the time period (Q1 2025, FY 2024, etc.).
- Do NOT write generic filler sentences.

Return ONLY valid JSON:
{{"decision": "keep", "summary": "..."}}
or
{{"decision": "discard", "summary": null}}"""

BLOCKED_RESOURCE_TYPES = {"image", "media", "font", "stylesheet"}
BLOCKED_URL_PATTERNS = {
    "google-analytics.com",
    "doubleclick.net",
    "googletag",
    "googletagmanager",
    "facebook.com/tr",
    "facebook.net",
    "fbcdn.net",
    "adservice.google",
    "google.com/ads",
}


def preflight_check():
    """Validate credentials before spending 20+ min scraping."""
    log.info("Preflight: checking credentials...")
    errors = []

    # Check env vars exist
    required = {
        "GOOGLE_API_KEY": "Gemini classification",
        "OPENAI_API_KEY": "OpenAI embeddings",
        "PINECONE_API_KEY": "Pinecone vector store",
        "PINECONE_INDEX_NAME": "Pinecone index",
    }
    for key, purpose in required.items():
        if not os.environ.get(key):
            errors.append(f"{key} is missing (needed for {purpose})")

    if errors:
        for e in errors:
            log.critical("PREFLIGHT FAIL: %s", e)
        sys.exit(1)

    # Quick Pinecone connection test
    try:
        from pinecone import Pinecone
        pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
        index = pc.Index(name=os.environ["PINECONE_INDEX_NAME"])
        stats = index.describe_index_stats()
        log.info("Preflight: Pinecone OK (%d vectors)", stats.total_vector_count)
    except Exception as e:
        log.critical("PREFLIGHT FAIL: Pinecone connection failed: %s", e)
        sys.exit(1)

    # Quick OpenAI connection test
    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        client.models.list()
        log.info("Preflight: OpenAI OK")
    except Exception as e:
        log.critical("PREFLIGHT FAIL: OpenAI connection failed: %s", e)
        sys.exit(1)

    log.info("Preflight: all credentials valid")

# ═══════════════════════════════════════════════════════════════════
#  SHARED STATE & HELPERS
# ═══════════════════════════════════════════════════════════════════

seen = set()
seen_lock = threading.Lock()
write_lock = threading.Lock()
stop_category = threading.Event()
page_queue: Queue = Queue()
new_count = 0
count_lock = threading.Lock()

# Pipeline-wide report stats
report = {
    "timestamp": "",
    "scraped": 0,
    "backfilled": 0,
    "backfill_failures": 0,
    "duplicates_removed": 0,
    "classified_keep": 0,
    "classified_discard": 0,
    "classify_errors": 0,
    "upserted": 0,
    "upsert_errors": 0,
    "pinecone_total": 0,
    "elapsed_minutes": 0.0,
    "errors": [],
}


def is_visited(url):
    with seen_lock:
        return url in seen


def mark_visited(url):
    """Add URL to in-memory set only (for pagination control during scrape).
    File is written later by persist_visited_urls() after pipeline succeeds."""
    with seen_lock:
        seen.add(url)


def persist_visited_urls():
    """Write the full visited set to disk. Called ONLY after pipeline success."""
    with seen_lock:
        urls = sorted(seen)
    try:
        with open(VISITED_FILE, "w", encoding="utf-8") as f:
            for url in urls:
                f.write(url + "\n")
        log.info("Persisted %d visited URLs to %s", len(urls), VISITED_FILE.name)
    except OSError as e:
        log.error("Failed to persist visited URLs: %s", e)


def increment_count():
    global new_count
    with count_lock:
        new_count += 1


def block_heavy_resources(page):
    def _handle(route):
        req = route.request
        if req.resource_type in BLOCKED_RESOURCE_TYPES:
            route.abort()
        elif any(p in req.url for p in BLOCKED_URL_PATTERNS):
            route.abort()
        else:
            route.continue_()

    page.route("**/*", _handle)


def collector(page, tag, timeout=5000):
    try:
        page.wait_for_selector(tag, timeout=timeout)
        contents = page.locator(tag).all_inner_texts()
        return " ".join(contents) if contents else None
    except Exception:
        return None


def goto_with_retry(page, url, retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            return True
        except Exception as e:
            if "closed" in str(e).lower():
                raise
            log.warning("Nav error %s (attempt %d/%d): %s", url, attempt, retries, e)
            if attempt < retries:
                time.sleep(RETRY_BACKOFF * attempt)
    return False


def write_scraped(entry: dict):
    with write_lock:
        try:
            with open(SCRAPED_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as e:
            log.error("Write failed: %s", e)


def load_visited_urls() -> set:
    urls = set()
    for path in [MASTER_FILE, VISITED_FILE]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if path == MASTER_FILE:
                        try:
                            urls.add(json.loads(line).get("url", ""))
                        except json.JSONDecodeError:
                            pass
                    else:
                        urls.add(line)
        except FileNotFoundError:
            pass
    log.info("Loaded %d visited URLs", len(urls))
    return urls


def launch_browser(tag, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        pw = None
        try:
            pw = sync_playwright().start()
            browser = pw.firefox.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="en-GB",
                timezone_id="Africa/Lagos",
            )
            pg = ctx.new_page()
            block_heavy_resources(pg)
            log.info("%s Browser launched (attempt %d)", tag, attempt)
            return pw, ctx, pg
        except Exception as e:
            log.error("%s Launch failed (attempt %d): %s", tag, attempt, e)
            if pw:
                try:
                    pw.stop()
                except Exception:
                    pass
            if attempt < max_attempts:
                time.sleep(30)
    return None


def close_browser(tag, pw, ctx):
    try:
        ctx.close()
    except Exception:
        pass
    try:
        pw.stop()
    except Exception:
        pass


def normalize_url(url: str) -> str:
    """Canonical form: strip trailing slash, fragment, and utm_ query params."""
    from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

    parsed = urlparse(url.strip())
    # Remove fragment and utm_ params
    clean_params = {
        k: v for k, v in parse_qs(parsed.query).items() if not k.startswith("utm_")
    }
    cleaned = parsed._replace(
        path=parsed.path.rstrip("/"),
        query=urlencode(clean_params, doseq=True),
        fragment="",
    )
    return urlunparse(cleaned)


def normalize_date(date_str: str) -> str:
    if not date_str:
        return date_str
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def collect_listing_links(page):
    """Gather article links from all three sections on a listing page."""
    all_links = []
    seen_hrefs = set()
    for section in ("post_title", "card_title", "story_title"):
        sel = SELECTORS[section]
        try:
            locator = page.locator(sel)
            count = locator.count()
            for i in range(count):
                try:
                    href = locator.nth(i).get_attribute("href")
                    title = locator.nth(i).inner_text().strip()
                except Exception:
                    continue
                if href and href not in seen_hrefs:
                    seen_hrefs.add(href)
                    all_links.append({"href": href, "title": title})
        except Exception as e:
            log.debug("No elements for %s (%s): %s", section, sel, e)
    return all_links


def clean_content(text: str) -> str:
    """Strip Nairametrics boilerplate from article content."""
    if not text:
        return text
    text = MORE_STORIES_RE.sub("\n", text)
    for trigger in FOOTER_TRIGGERS:
        idx = text.find(trigger)
        if idx != -1:
            text = text[:idx]
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if any(stripped == h or stripped.rstrip(" \t:") == h for h in SECTION_HEADERS):
            continue
        if any(stripped.startswith(p) for p in PARAGRAPH_PREFIXES):
            continue
        cleaned.append(line)
    text = "\n".join(cleaned)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def load_scraped() -> list[dict]:
    """Load scraped_today.jsonl into a list."""
    entries = []
    if not SCRAPED_FILE.exists():
        return entries
    for line in SCRAPED_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return entries


def save_scraped(entries: list[dict]):
    """Rewrite scraped_today.jsonl from list."""
    with open(SCRAPED_FILE, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


# ═══════════════════════════════════════════════════════════════════
#  STAGE 1: SCRAPE
# ═══════════════════════════════════════════════════════════════════


def scrape_worker(thread_id: int):
    tag = f"[T-{thread_id}]"
    result = launch_browser(tag)
    if not result:
        return
    pw, ctx, page = result

    while True:
        task = page_queue.get()
        if task is None:
            page_queue.task_done()
            break

        current_link, category_name, page_num = task

        if stop_category.is_set():
            page_queue.task_done()
            continue

        log.info("%s %s page %d", tag, category_name, page_num)

        try:
            listing_url = (
                current_link if page_num == 1 else f"{current_link}page/{page_num}/"
            )
            if not goto_with_retry(page, listing_url):
                page_queue.task_done()
                continue
            time.sleep(PAGE_DELAY)

            page_data = collect_listing_links(page)
            count = len(page_data)
            log.info("%s Found %d articles on page %d", tag, count, page_num)

            if count == 0:
                page_queue.task_done()
                continue

            hit_old = any(
                item["href"] and is_visited(item["href"]) for item in page_data
            )

            for item in page_data:
                href = item["href"]
                if not href:
                    continue
                if is_visited(href):
                    log.info("%s Hit visited URL — stopping category", tag)
                    stop_category.set()
                    break

                log.info("%s  -> %s", tag, item["title"][:60])

                if not goto_with_retry(page, href, retries=2):
                    continue
                time.sleep(PAGE_DELAY)

                article_title = collector(page, SELECTORS["article_title"])
                article_content = collector(page, SELECTORS["article_content"])
                needs_backfill = not article_content

                article_date = collector(page, SELECTORS["article_date"])
                if not article_date:
                    article_date = ""

                categories_list = []
                try:
                    page.wait_for_selector(SELECTORS["article_category"], timeout=3000)
                    categories_list = [
                        c.strip()
                        for c in page.locator(
                            SELECTORS["article_category"]
                        ).all_inner_texts()
                    ]
                except Exception:
                    pass

                entry = {
                    "title": article_title,
                    "content": article_content,
                    "date": article_date,
                    "url": href,
                    "categories": categories_list,
                    "needs_backfill": needs_backfill,
                }
                write_scraped(entry)
                mark_visited(href)
                increment_count()

            if not hit_old and all(
                is_visited(item["href"]) for item in page_data if item["href"]
            ):
                stop_category.set()

            page_queue.task_done()

        except Exception as e:
            log.error("%s Browser crash: %s — restarting", tag, e)
            page_queue.task_done()
            close_browser(tag, pw, ctx)
            time.sleep(5)
            result = launch_browser(tag)
            if not result:
                return
            pw, ctx, page = result

    close_browser(tag, pw, ctx)


def run_scrape():
    global seen, new_count
    log.info("=" * 60)
    log.info("STAGE 1: SCRAPE")
    log.info("=" * 60)

    seen = load_visited_urls()
    new_count = 0
    SCRAPED_FILE.write_text("", encoding="utf-8")

    try:
        content = json.loads(INPUT_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.critical("Cannot load %s: %s", INPUT_FILE, e)
        sys.exit(1)

    log.info("%d categories, %d threads", len(content), NUM_THREADS)

    threads = []
    for idx in range(NUM_THREADS):
        t = threading.Thread(target=scrape_worker, args=(idx,), daemon=True)
        threads.append(t)
        t.start()

    # Scout browser for page-count discovery
    scout_page = scout_pw = scout_ctx = None
    try:
        scout_pw = sync_playwright().start()
        scout_browser = scout_pw.firefox.launch(headless=True)
        scout_ctx = scout_browser.new_context(viewport={"width": 1366, "height": 768})
        scout_page = scout_ctx.new_page()
        block_heavy_resources(scout_page)
    except Exception as e:
        log.warning("Scout browser failed: %s", e)

    for link in content:
        current_link = link["URL"]
        category_name = link["Name"]
        stop_category.clear()

        log.info("--- CATEGORY: %s", category_name)

        total_pages = 1
        if scout_page:
            try:
                scout_page.goto(
                    current_link, timeout=60000, wait_until="domcontentloaded"
                )
                time.sleep(2)
                try:
                    last_text = (
                        scout_page.locator(SELECTORS["pagination"])
                        .nth(-2)
                        .inner_text(timeout=3000)
                    )
                    total_pages = int(last_text.replace(",", ""))
                except Exception:
                    total_pages = 1
            except Exception:
                total_pages = 1

        pages = min(total_pages, MAX_PAGES_PER_CATEGORY)
        for p in range(1, pages + 1):
            page_queue.put((current_link, category_name, p))
        page_queue.join()

    if scout_ctx:
        try:
            scout_ctx.close()
        except Exception:
            pass
    if scout_pw:
        try:
            scout_pw.stop()
        except Exception:
            pass

    for _ in range(NUM_THREADS):
        page_queue.put(None)
    for t in threads:
        t.join()

    report["scraped"] = new_count
    log.info("SCRAPE COMPLETE — %d new articles", new_count)
    return new_count


# ═══════════════════════════════════════════════════════════════════
#  STAGE 2: BACKFILL (3 retries per article)
# ═══════════════════════════════════════════════════════════════════


def run_backfill():
    log.info("=" * 60)
    log.info("STAGE 2: BACKFILL (max %d retries)", MAX_BACKFILL_RETRIES)
    log.info("=" * 60)

    entries = load_scraped()
    to_fix = [i for i, e in enumerate(entries) if e.get("needs_backfill")]
    if not to_fix:
        log.info("No articles need backfill — skipping")
        return

    log.info("%d articles need backfill", len(to_fix))

    result = launch_browser("[BF]")
    if not result:
        log.error("Cannot launch backfill browser")
        return
    pw, ctx, page = result

    fixed = 0
    for idx in to_fix:
        entry = entries[idx]
        url = entry.get("url", "")
        if not url:
            continue

        success = False
        for attempt in range(1, MAX_BACKFILL_RETRIES + 1):
            log.info("[BF] Attempt %d/%d for %s", attempt, MAX_BACKFILL_RETRIES, url)

            if not goto_with_retry(page, url):
                continue
            time.sleep(PAGE_DELAY)

            if not (entry.get("content") or "").strip():
                content = collector(page, SELECTORS["article_content"], timeout=8000)
                if content:
                    entries[idx]["content"] = content

            if not (entry.get("title") or "").strip():
                title = collector(page, SELECTORS["article_title"])
                if title:
                    entries[idx]["title"] = title

            if not (entry.get("date") or "").strip():
                date = collector(page, SELECTORS["article_date"])
                if date:
                    entries[idx]["date"] = date

            if not entry.get("categories"):
                try:
                    page.wait_for_selector(SELECTORS["article_category"], timeout=3000)
                    entries[idx]["categories"] = [
                        c.strip()
                        for c in page.locator(
                            SELECTORS["article_category"]
                        ).all_inner_texts()
                    ]
                except Exception:
                    pass

            e = entries[idx]
            all_filled = all(
                [
                    (e.get("content") or "").strip(),
                    (e.get("title") or "").strip(),
                    (e.get("date") or "").strip(),
                    e.get("categories"),
                ]
            )
            entries[idx]["needs_backfill"] = not all_filled

            if all_filled:
                fixed += 1
                success = True
                log.info("[BF] Fixed: %s", url)
                break
            else:
                log.warning("[BF] Still incomplete after attempt %d", attempt)
                time.sleep(2)

    close_browser("[BF]", pw, ctx)
    save_scraped(entries)
    report["backfilled"] = fixed
    log.info("BACKFILL COMPLETE — %d fixed", fixed)


# ═══════════════════════════════════════════════════════════════════
#  STAGE 3: REMOVE FAILURES
# ═══════════════════════════════════════════════════════════════════


def run_remove_failures():
    log.info("=" * 60)
    log.info("STAGE 3: REMOVE BACKFILL FAILURES")
    log.info("=" * 60)

    entries = load_scraped()
    before = len(entries)
    failures = [e for e in entries if e.get("needs_backfill")]

    if not failures:
        log.info("No failures to remove")
        return

    for f in failures:
        log.warning("Removing failed article: %s", f.get("url", "?"))

    kept = [e for e in entries if not e.get("needs_backfill")]
    save_scraped(kept)
    removed = before - len(kept)
    report["backfill_failures"] = removed
    log.info("REMOVED %d articles that failed backfill", removed)


# ═══════════════════════════════════════════════════════════════════
#  STAGE 4: DEDUPLICATE
# ═══════════════════════════════════════════════════════════════════


def run_dedup():
    log.info("=" * 60)
    log.info("STAGE 4: DEDUPLICATE")
    log.info("=" * 60)

    entries = load_scraped()
    before = len(entries)

    seen_urls = {}
    for idx, entry in enumerate(entries):
        url = entry.get("url", "")
        if url:
            seen_urls[url] = idx  # last occurrence wins

    keep_indices = set(seen_urls.values())
    deduped = [e for i, e in enumerate(entries) if i in keep_indices]

    removed = before - len(deduped)
    if removed > 0:
        save_scraped(deduped)
    report["duplicates_removed"] = removed
    log.info("DEDUP — %d duplicates removed, %d remain", removed, len(deduped))


# ═══════════════════════════════════════════════════════════════════
#  STAGE 5: CLEAN CONTENT
# ═══════════════════════════════════════════════════════════════════


def run_clean():
    log.info("=" * 60)
    log.info("STAGE 5: CLEAN CONTENT")
    log.info("=" * 60)

    entries = load_scraped()
    changed = 0
    for i, entry in enumerate(entries):
        original = entry.get("content") or ""
        cleaned = clean_content(original)
        if cleaned != original:
            entries[i]["content"] = cleaned if cleaned else None
            if not cleaned:
                entries[i]["needs_backfill"] = True
            changed += 1

    if changed > 0:
        save_scraped(entries)
    log.info("CLEAN — %d articles had boilerplate removed", changed)


# ═══════════════════════════════════════════════════════════════════
#  STAGE 6: CLASSIFY (Gemini 3.1 Flash-Lite)
# ═══════════════════════════════════════════════════════════════════


def _classify_one(chain, article: dict) -> dict | None:
    """Classify a single article with retry on rate-limit errors."""
    content = (article.get("content") or "").strip()
    title = (article.get("title") or "").strip()

    if not content or len(content) < 50:
        return None

    short = title[:60] + "..." if len(title) > 60 else title

    for attempt in range(1, CLASSIFY_MAX_RETRIES + 1):
        try:
            result: ArticleClassification = chain.invoke(
                {
                    "title": title,
                    "content": content,
                }
            )

            if result.decision == "keep" and result.summary:
                log.info("  KEEP: %s", short)
                log.info("    Summary: %s", result.summary[:120])
                return {
                    "url": article["url"],
                    "title": title,
                    "date": normalize_date(article.get("date", "")),
                    "categories": article.get("categories", []),
                    "summary": result.summary,
                }
            log.info("  DISCARD: %s", short)
            return None

        except Exception as e:
            err_str = str(e).lower()
            is_retryable = any(k in err_str for k in ("429", "rate", "resource_exhausted", "quota", "503", "overloaded"))
            if is_retryable and attempt < CLASSIFY_MAX_RETRIES:
                wait = CLASSIFY_DELAY * (2 ** attempt)
                log.warning("  Rate-limited on %s (attempt %d/%d), waiting %.0fs...",
                            short, attempt, CLASSIFY_MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            log.error("Classification failed for %s: %s", article.get("url", "?"), e)
            return None


def run_classify(limit: int | None = None):
    log.info("=" * 60)
    log.info("STAGE 6: CLASSIFY (%s)", CLASSIFY_MODEL)
    log.info("=" * 60)

    if not SCRAPED_FILE.exists():
        log.error("No scraped file found — run scrape first")
        return []

    articles = load_scraped()
    if limit:
        articles = articles[:limit]

    if not articles:
        log.info("No articles to classify")
        return []

    log.info(
        "Classifying %d articles (rate-limited at %.1fs/req)",
        len(articles),
        CLASSIFY_DELAY,
    )

    # Lazy imports — only needed for classification
    from langchain.chat_models import init_chat_model
    from langchain_core.prompts import ChatPromptTemplate

    log.info("Using model: %s", CLASSIFY_MODEL)
    llm = init_chat_model(CLASSIFY_MODEL, temperature=0)
    classifier = llm.with_structured_output(ArticleClassification)

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            ("human", "ARTICLE TITLE: {title}\n\nARTICLE BODY: {content}"),
        ]
    )

    chain = prompt | classifier

    # Sequential classification with rate limiting for free tier
    kept = []
    discarded = 0
    errors = 0
    backoff = CLASSIFY_DELAY

    for i, article in enumerate(articles, 1):
        log.info(
            "[%d/%d] Classifying: %s",
            i,
            len(articles),
            (article.get("title") or "?")[:60],
        )
        result = _classify_one(chain, article)
        if result:
            kept.append(result)
        elif result is None and (article.get("content") or "").strip():
            discarded += 1
        else:
            errors += 1

        if i % 10 == 0 or i == len(articles):
            log.info(
                "  Progress: %d/%d (kept=%d, discarded=%d)",
                i,
                len(articles),
                len(kept),
                discarded,
            )

        # Rate limiting with adaptive backoff
        time.sleep(backoff)

    # Write classified results
    with open(CLASSIFIED_FILE, "w", encoding="utf-8") as f:
        for r in kept:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    log.info("Saved %d classified articles to %s", len(kept), CLASSIFIED_FILE.name)

    report["classified_keep"] = len(kept)
    report["classified_discard"] = discarded
    report["classify_errors"] = errors
    log.info(
        "CLASSIFY COMPLETE — %d kept, %d discarded, %d errors",
        len(kept),
        discarded,
        errors,
    )
    return kept


# ═══════════════════════════════════════════════════════════════════
#  STAGE 7: UPSERT TO PINECONE
# ═══════════════════════════════════════════════════════════════════


def run_upsert(kept: list[dict] | None = None):
    log.info("=" * 60)
    log.info("STAGE 7: UPSERT TO PINECONE")
    log.info("=" * 60)

    if kept is None:
        if not CLASSIFIED_FILE.exists():
            log.error("No classified file found")
            return
        kept = []
        for line in CLASSIFIED_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    kept.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    if not kept:
        log.info("Nothing to upsert")
        return

    log.info("Upserting %d articles to Pinecone", len(kept))

    from openai import OpenAI
    from pinecone import Pinecone

    openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
    index = pc.Index(name=os.environ["PINECONE_INDEX_NAME"])

    # ── Pinecone-level dedup: normalize URLs and check what already exists ──
    for r in kept:
        r["url"] = normalize_url(r["url"])

    candidate_ids = [r["url"] for r in kept]
    existing_ids = set()
    for i in range(0, len(candidate_ids), 100):
        batch_ids = candidate_ids[i : i + 100]
        try:
            fetch_result = index.fetch(ids=batch_ids)
            existing_ids.update(fetch_result.vectors.keys())
        except Exception as e:
            log.warning("Fetch-check failed (batch %d): %s — will upsert all", i // 100 + 1, e)

    if existing_ids:
        log.info("Skipping %d articles already in Pinecone", len(existing_ids))
        kept = [r for r in kept if r["url"] not in existing_ids]
        report["duplicates_skipped_pinecone"] = len(existing_ids)

    if not kept:
        log.info("All articles already exist in Pinecone — nothing to upsert")
        return

    log.info("%d new articles to upsert after dedup", len(kept))

    upserted = 0
    errors = 0

    for i in range(0, len(kept), EMBED_BATCH_SIZE):
        batch = kept[i : i + EMBED_BATCH_SIZE]
        summaries = [r["summary"] for r in batch]

        try:
            response = openai_client.embeddings.create(
                input=summaries,
                model="text-embedding-3-small",
            )
        except Exception as e:
            log.error("Embedding error at batch %d: %s", i // EMBED_BATCH_SIZE + 1, e)
            errors += len(batch)
            continue

        vectors = []
        for record, emb_obj in zip(batch, response.data):
            vectors.append(
                {
                    "id": record["url"],
                    "values": emb_obj.embedding,
                    "metadata": {
                        "url": record["url"],
                        "title": record["title"],
                        "date": record["date"],
                        "categories": record["categories"],
                        "summary": record["summary"],
                    },
                }
            )

        try:
            index.upsert(vectors=vectors)
            upserted += len(vectors)
        except Exception as e:
            log.error("Upsert error: %s", e)
            errors += len(vectors)
            continue

        log.info(
            "  Batch %d: %d/%d upserted", i // EMBED_BATCH_SIZE + 1, upserted, len(kept)
        )

    # Append to master JSONL (also deduplicated)
    master_urls = set()
    if MASTER_FILE.exists():
        for line in MASTER_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    master_urls.add(normalize_url(json.loads(line).get("url", "")))
                except json.JSONDecodeError:
                    pass

    new_master = 0
    with open(MASTER_FILE, "a", encoding="utf-8") as f:
        for r in kept:
            if r["url"] not in master_urls:
                entry = {
                    "title": r["title"],
                    "content": "",
                    "date": r["date"],
                    "url": r["url"],
                    "categories": r["categories"],
                    "needs_backfill": False,
                }
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                master_urls.add(r["url"])
                new_master += 1
    log.info("Appended %d new entries to master JSONL (%d skipped as dupes)",
             new_master, len(kept) - new_master)

    try:
        stats = index.describe_index_stats()
        # Pinecone has eventual consistency — stats may not reflect
        # the vectors we just upserted, so add them to be accurate.
        report["pinecone_total"] = stats.total_vector_count + upserted
    except Exception:
        report["pinecone_total"] = upserted  # best effort

    report["upserted"] = upserted
    report["upsert_errors"] = errors
    log.info("UPSERT COMPLETE — %d upserted, %d errors", upserted, errors)

    # Remove only the articles that were successfully upserted
    if upserted > 0:
        upserted_urls = {r["url"] for r in kept}
        remaining = [e for e in load_scraped() if normalize_url(e.get("url", "")) not in upserted_urls]
        save_scraped(remaining)
        log.info(
            "Removed %d processed articles from %s (%d remaining)",
            len(upserted_urls),
            SCRAPED_FILE.name,
            len(remaining),
        )


# ═══════════════════════════════════════════════════════════════════
#  STAGE 8: REPORT
# ═══════════════════════════════════════════════════════════════════


def write_report(elapsed: float):
    report["elapsed_minutes"] = round(elapsed / 60, 1)
    report["timestamp"] = datetime.now(timezone.utc).isoformat()

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    log.info("=" * 60)
    log.info("PIPELINE REPORT")
    log.info("=" * 60)
    for k, v in report.items():
        if k != "errors":
            log.info("  %-25s: %s", k, v)
    if report["errors"]:
        log.info("  ERRORS:")
        for err in report["errors"]:
            log.info("    - %s", err)
    log.info("Report written to %s", REPORT_FILE)

    # GitHub Actions Job Summary (if running in CI)
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_file:
        try:
            with open(summary_file, "a", encoding="utf-8") as f:
                f.write("## Daily Ingestion Pipeline Report\n\n")
                f.write(f"**Timestamp:** {report['timestamp']}\n\n")
                f.write("| Metric | Value |\n|--------|-------|\n")
                f.write(f"| Articles Scraped | {report['scraped']} |\n")
                f.write(f"| Backfilled | {report['backfilled']} |\n")
                f.write(
                    f"| Backfill Failures (removed) | {report['backfill_failures']} |\n"
                )
                f.write(f"| Duplicates Removed | {report['duplicates_removed']} |\n")
                f.write(f"| Classified: Keep | {report['classified_keep']} |\n")
                f.write(f"| Classified: Discard | {report['classified_discard']} |\n")
                f.write(f"| Upserted to Pinecone | {report['upserted']} |\n")
                f.write(f"| Pinecone Total Vectors | {report['pinecone_total']} |\n")
                f.write(f"| Elapsed | {report['elapsed_minutes']} min |\n")
                if report["errors"]:
                    f.write(f"\n### Errors\n")
                    for err in report["errors"]:
                        f.write(f"- {err}\n")
            log.info("GitHub Actions Job Summary updated")
        except Exception as e:
            log.warning("Could not write GitHub summary: %s", e)

    # Telegram notification
    send_telegram_report()


def send_telegram_report():
    """Send pipeline report to Telegram bot."""
    import urllib.request

    bot_token = os.getenv("TELEGRAM_API_KEY")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        log.info("Telegram not configured — skipping notification")
        return

    # Build the message
    status = "OK" if not report["errors"] else "ISSUES"
    msg_lines = [
        f"[{status}] *Daily Ingestion Report*",
        f"{report['timestamp'][:19].replace('T', ' ')} UTC",
        "",
        "*Pipeline Results:*",
        f"  Scraped: *{report['scraped']}* articles",
        f"  Backfilled: *{report['backfilled']}*",
        f"  Backfill failures: *{report['backfill_failures']}*",
        f"  Duplicates removed: *{report['duplicates_removed']}*",
        "",
        "*Classification:*",
        f"  Kept: *{report['classified_keep']}*",
        f"  Discarded: *{report['classified_discard']}*",
        f"  Errors: *{report['classify_errors']}*",
        "",
        "*Pinecone:*",
        f"  Upserted: *{report['upserted']}*",
        f"  Errors: *{report['upsert_errors']}*",
        f"  Total vectors: *{report['pinecone_total']}*",
        "",
        f"Elapsed: *{report['elapsed_minutes']}* min",
    ]

    if report.get("duplicates_skipped_pinecone"):
        msg_lines.insert(
            -1,
            f"  Skipped (already in Pinecone): *{report['duplicates_skipped_pinecone']}*",
        )

    if report["errors"]:
        msg_lines.append("")
        msg_lines.append("*Errors:*")
        for err in report["errors"][:5]:  # Cap at 5
            msg_lines.append(f"  - {err[:100]}")

    message = "\n".join(msg_lines)

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = json.dumps(
            {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            url, data=payload, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                log.info("Telegram notification sent successfully")
            else:
                log.warning("Telegram responded with status %d", resp.status)
    except Exception as e:
        log.warning("Failed to send Telegram notification: %s", e)


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="Daily ingestion pipeline")
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Only scrape + backfill + clean, skip classification",
    )
    parser.add_argument(
        "--classify-only",
        action="store_true",
        help="Skip scraping, but process and classify from scraped_today.jsonl",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of articles to classify (for testing)",
    )
    args = parser.parse_args()

    start = time.time()

    log.info("+" + "=" * 58 + "+")
    log.info("|        DAILY INGESTION PIPELINE                         |")
    log.info(
        "|  %s                                        |",
        datetime.now().strftime("%Y-%m-%d %H:%M"),
    )
    log.info("+" + "=" * 58 + "+")

    if not args.classify_only:
        preflight_check()
        run_scrape()
        
    run_backfill()
    run_remove_failures()
    run_dedup()
    run_clean()

    if args.scrape_only:
        log.info("--scrape-only: stopping after scrape + backfill + clean")
        write_report(time.time() - start)
        return

    kept = run_classify(limit=args.limit)
    run_upsert(kept)

    # Only persist visited URLs after everything succeeds
    persist_visited_urls()

    write_report(time.time() - start)


if __name__ == "__main__":
    main()
