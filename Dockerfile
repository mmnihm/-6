FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
EXPOSE 8080
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# DejaVu keeps the CAPTCHA digits sharp; without it Pillow falls back to a
# bitmap font that gets blurry when scaled up.
RUN apt-get update \
    && apt-get install -y --no-install-recommends fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN mkdir -p /app/data \
    && useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c 'import os, urllib.request; port = os.getenv("PORT") or os.getenv("WEB_PORT", "8080"); urllib.request.urlopen("http://127.0.0.1:" + port + "/healthz", timeout=5)'

CMD ["python", "main.py"]
