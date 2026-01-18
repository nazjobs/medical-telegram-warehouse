import os
import json
import asyncio
import logging
from datetime import datetime
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup Logging
logging.basicConfig(
    filename="logs/scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Configuration
API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
PHONE = os.getenv("TG_PHONE")

CHANNELS = [
    "https://t.me/CheMed123",  # CheMed (Verify actual username if link fails)
    "https://t.me/lobelia4cosmetics",
    "https://t.me/tikvahpharma",
    # Add more from et.tgstat.com/medicine if needed
]

client = TelegramClient("anon", API_ID, API_HASH)


class DateEncoder(json.JSONEncoder):
    """Helper to serialize datetime objects for JSON"""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


async def scrape_channel(channel_url):
    print(f"Scraping {channel_url}...")
    logging.info(f"Started scraping {channel_url}")

    try:
        entity = await client.get_entity(channel_url)
        channel_name = entity.username or entity.id

        # Create directories
        json_dir = f"data/raw/telegram_messages/{datetime.now().strftime('%Y-%m-%d')}"
        img_dir = f"data/raw/images/{channel_name}"
        os.makedirs(json_dir, exist_ok=True)
        os.makedirs(img_dir, exist_ok=True)

        messages_data = []

        # Iterate over messages (Limit to last 100 for testing, increase for prod)
        async for message in client.iter_messages(entity, limit=200):
            msg_data = {
                "message_id": message.id,
                "channel_name": channel_name,
                "date": message.date,
                "message_text": message.text,
                "views": message.views if message.views else 0,
                "forwards": message.forwards if message.forwards else 0,
                "has_media": False,
                "image_path": None,
            }

            if message.photo:
                msg_data["has_media"] = True
                # Download image
                path = await client.download_media(message.photo, file=img_dir)
                msg_data["image_path"] = path

            messages_data.append(msg_data)

        # Save to JSON
        output_file = f"{json_dir}/{channel_name}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(messages_data, f, cls=DateEncoder, ensure_ascii=False, indent=4)

        logging.info(
            f"Successfully scraped {len(messages_data)} messages from {channel_name}"
        )
        print(f"Saved {len(messages_data)} messages to {output_file}")

    except Exception as e:
        logging.error(f"Error scraping {channel_url}: {e}")
        print(f"Error: {e}")


async def main():
    await client.start(phone=PHONE)

    tasks = [scrape_channel(url) for url in CHANNELS]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
