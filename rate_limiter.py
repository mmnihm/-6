import asyncio
import random
import time
from collections import deque


class GlobalSendLimiter:
    """Token-bucket style limiter for all bot outgoing messages."""

    def __init__(self, rate_per_sec=0.5, burst=3, min_jitter=0.1, max_jitter=0.6):
        self.rate = rate_per_sec          # tokens added per second
        self.burst = burst                # max tokens in bucket
        self.tokens = burst
        self.last = time.monotonic()
        self.min_jitter = min_jitter
        self.max_jitter = max_jitter
        self._lock = asyncio.Lock()
        self.recent = deque(maxlen=50)   # timestamps of last sends, for diagnostics

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            # refill
            self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens < 1:
                need = 1 - self.tokens
                await asyncio.sleep(need / self.rate)
                self.tokens = 0
                self.last = time.monotonic()
            else:
                self.tokens -= 1
            # random jitter to avoid perfectly rhythmic traffic
            jitter = random.uniform(self.min_jitter, self.max_jitter)
            await asyncio.sleep(jitter)
            self.recent.append(time.monotonic())

    def wrap(self, client):
        """Monkey-patch client send methods through the limiter."""
        limiter = self
        _send_message = client.send_message
        _send_document = client.send_document
        _send_photo = client.send_photo
        _send_video = client.send_video
        _copy_message = client.copy_message
        _forward_messages = client.forward_messages

        async def limited(fn, *args, **kwargs):
            await limiter.acquire()
            for attempt in range(6):
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:
                    # FloodWait: sleep exactly what Telegram says, then retry
                    name = type(exc).__name__
                    if name == "FloodWait":
                        secs = max(1, int(getattr(exc, "value", 1) or 1)) + 1
                        await asyncio.sleep(secs)
                        continue
                    # pyrogram wraps 429 sometimes as RPCError with ID
                    s = str(exc)
                    if "429" in s or "FLOOD" in s.upper():
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise
            raise RuntimeError("send failed after 6 attempts")

        client.send_message = lambda *a, **k: limited(_send_message, *a, **k)
        client.send_document = lambda *a, **k: limited(_send_document, *a, **k)
        client.send_photo = lambda *a, **k: limited(_send_photo, *a, **k)
        client.send_video = lambda *a, **k: limited(_send_video, *a, **k)
        client.copy_message = lambda *a, **k: limited(_copy_message, *a, **k)
        client.forward_messages = lambda *a, **k: limited(_forward_messages, *a, **k)
        return client


# ---- usage in main.py, right after client = Client(...) ----
# limiter = GlobalSendLimiter(rate_per_sec=0.4, burst=2)
# app = limiter.wrap(app)
# await app.start()
