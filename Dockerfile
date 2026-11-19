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
RUN mkdir -p /app/data

CMD ["python", "main.py"]
