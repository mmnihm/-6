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
SCANNER_SESSION_STRING = os.getenv("SCANNER_SESSION_STRING", "").strip()
ADMIN_ID = int(required("ADMIN_ID"))
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/bot.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
WEB_PORT = int(os.getenv("PORT") or os.getenv("WEB_PORT", "8080"))
