"""Quick test — send a test message via the Telegram bot."""
import json
import os
import urllib.request
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=True)

bot_token = os.getenv("TELEGRAM_API_KEY")
chat_id = os.getenv("TELEGRAM_CHAT_ID")

if not bot_token or not chat_id:
    print(f"Missing config: TELEGRAM_API_KEY={'set' if bot_token else 'MISSING'}, TELEGRAM_CHAT_ID={'set' if chat_id else 'MISSING'}")
    exit(1)

message = """[OK] Daily Ingestion Report (TEST)
2026-06-11 19:40:00 UTC

*Pipeline Results:*
  Scraped: *88* articles
  Backfilled: *0*
  Backfill failures: *0*
  Duplicates removed: *0*

*Classification:*
  Kept: *55*
  Discarded: *33*
  Errors: *0*

*Pinecone:*
  Upserted: *42*
  Errors: *0*
  Total vectors: *6134*

Elapsed: *10.9* min"""

url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
payload = json.dumps({"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}).encode("utf-8")
req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})

with urllib.request.urlopen(req, timeout=10) as resp:
    result = json.loads(resp.read())
    if result.get("ok"):
        print("Message sent! Check your Telegram.")
    else:
        print(f"Failed: {result}")
