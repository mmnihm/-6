import asyncio
import os

from pyrogram import Client

import config


async def main():
    workdir = os.path.dirname(config.DATABASE_PATH) or "."
    os.makedirs(workdir, exist_ok=True)
    client = Client(
        "member_cloud_scanner",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        workdir=workdir,
    )
    await client.start()
    try:
        me = await client.get_me()
        if me.is_bot:
            raise RuntimeError("Please sign in with a normal Telegram user account, not a bot.")
        async for _dialog in client.get_dialogs():
            pass
        print(f"Scanner account ready: {me.id}")
        print("The login session is saved in the persistent data directory.")
    finally:
        await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
