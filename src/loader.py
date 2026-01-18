import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# DB Connection
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_port"),
)
cur = conn.cursor()

# 1. Create Raw Schema and Table
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
cur.execute("""
    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        id SERIAL PRIMARY KEY,
        message_id BIGINT,
        channel_name TEXT,
        message_date TIMESTAMP,
        message_text TEXT,
        views INT,
        forwards INT,
        has_media BOOLEAN,
        image_path TEXT,
        ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
conn.commit()

# 2. Walk through data directory and load JSONs
base_path = "data/raw/telegram_messages"

for root, dirs, files in os.walk(base_path):
    for file in files:
        if file.endswith(".json"):
            file_path = os.path.join(root, file)
            print(f"Loading {file_path}...")

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

                for msg in data:
                    cur.execute(
                        """
                        INSERT INTO raw.telegram_messages 
                        (message_id, channel_name, message_date, message_text, views, forwards, has_media, image_path)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            msg["message_id"],
                            msg["channel_name"],
                            msg["date"],
                            msg["message_text"],
                            msg["views"],
                            msg["forwards"],
                            msg["has_media"],
                            msg["image_path"],
                        ),
                    )
            conn.commit()

print("Data loading complete.")
cur.close()
conn.close()
