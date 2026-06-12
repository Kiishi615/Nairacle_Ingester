# Files Ready to Push

These are the exact files needed for the GitHub Actions daily ingester.
Copy them to a fresh repo or a new branch — don't disrupt your current git.

## File Map

```
your-repo/
├── .github/
│   └── workflows/
│       └── daily_ingest.yml      ← Workflow (cron 8 PM WAT + manual trigger)
├── daily_ingest.py               ← Pipeline script (standalone, 1326 lines)
├── menu_links.json               ← Category URLs to scrape
└── test_telegram.py              ← Quick Telegram bot test
```

## What each file does

| File | Purpose |
|:-----|:--------|
| `daily_ingest.yml` | GitHub Actions workflow — installs deps, runs pipeline, caches visited_urls.txt, uploads artifacts, passes secrets as env vars |
| `daily_ingest.py` | The full pipeline — scrape, backfill, clean, classify (Gemini), upsert (Pinecone), report + Telegram notification |
| `menu_links.json` | List of Nairametrics category URLs to scrape |

## GitHub Secrets Required (6)

| Secret Name | What it is |
|:------------|:-----------|
| GOOGLE_API_KEY | Gemini API key (for classification) |
| OPENAI_API_KEY | OpenAI API key (for embeddings) |
| PINECONE_API_KEY | Pinecone API key |
| PINECONE_INDEX_NAME | Pinecone index name |
| TELEGRAM_API_KEY | Telegram bot token from BotFather |
| TELEGRAM_CHAT_ID | Your Telegram chat ID |

## Nothing else is needed

- visited_urls.txt → handled by Actions cache (persists between runs)
- nairametrics_articles.jsonl → not needed, Pinecone IS the archive
- scraped_today.jsonl, classified_today.jsonl, logs → created at runtime, uploaded as artifacts
