import os
from dotenv import load_dotenv

load_dotenv()


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.startswith("REPLACE_ME"):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


BOT_TOKEN = required("BOT_TOKEN")
API_ID = int(required("API_ID"))
API_HASH = required("API_HASH")
MEMBER_GROUP_ID = int(required("MEMBER_GROUP_ID"))
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/bot.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
