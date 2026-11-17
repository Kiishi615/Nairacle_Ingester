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
| `daily_ingest.yml` | GitHub Actions workflow — installs deps, runs pipeline, commits confirmed visited URLs, uploads artifacts, passes secrets as env vars |
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

- visited_urls.txt → tracked in this repository; updated after confirmed Pinecone ingestion
- nairametrics_articles.jsonl → not needed, Pinecone IS the archive
- scraped_today.jsonl, classified_today.jsonl, logs → created at runtime, uploaded as artifacts

## Visited history and recovery

The committed `visited_urls.txt` is the source of history for each run. Scraping a
URL only marks it seen in memory for that run. A URL is added to the physical file
only after Pinecone acknowledges its upload, or a fetch confirms it already exists.
An explicit discard ends the current attempt without adding a visited entry.
Failed articles stay pending. A missing or invalid model reply counts as a failure.

Each confirmed upload batch checkpoints the file with an atomic replacement.
Only confirmed uploads enter the local master file or leave the pending article
file. The workflow backs up the visited file and pending article records in the
run's `ingestion-state` artifact. It publishes `visited_urls.txt` and
`retry_urls.txt` on the default branch, including when a later pipeline step
fails and the runner can still execute the publication step. Each daily run
reloads pending URLs from the repository and fetches their article text again.
Repository rules must permit the workflow's `GITHUB_TOKEN` to update both files.

Git history preserves the list beyond artifact retention.
