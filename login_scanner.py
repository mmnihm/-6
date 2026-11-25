import asyncio
import base64
import os
import time

from pyrogram import Client, raw

import config


def _qr_deep_link(token: bytes) -> str:
    token_b64 = base64.urlsafe_b64encode(token).rstrip(b"=").decode()
    return f"tg://login?token={token_b64}"


async def main():
    workdir = os.path.dirname(config.DATABASE_PATH) or "."
    os.makedirs(workdir, exist_ok=True)
    client = Client(
        "member_cloud_scanner",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        workdir=workdir,
    )
    await client.connect()
    try:
        while True:
            result = await client.invoke(
                raw.functions.auth.ExportLoginToken(
                    api_id=config.API_ID, api_hash=config.API_HASH, except_ids=[]
                )
            )
            if isinstance(result, raw.types.auth.LoginTokenSuccess):
                break
            print("Open Telegram on a device already signed in to the scanner account,")
            print("then scan this login link (Settings → Devices → Link Desktop Device):")
            print(_qr_deep_link(result.token))
            deadline = time.time() + 60
            authorized = False
            while time.time() < deadline:
                await asyncio.sleep(3)
                result = await client.invoke(
                    raw.functions.auth.ExportLoginToken(
                        api_id=config.API_ID, api_hash=config.API_HASH, except_ids=[]
                    )
                )
                if isinstance(result, raw.types.auth.LoginTokenSuccess):
                    authorized = True
                    break
            if authorized:
                break
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
