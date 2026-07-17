FROM python:3.13-slim

LABEL org.opencontainers.image.title="CacheDeck" \
      org.opencontainers.image.description="Browser UI for SteamPrefill and LANCache" \
      org.opencontainers.image.source="https://github.com/DarmachD/CacheDeck" \
      org.opencontainers.image.licenses="MIT"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TARGET_CONTAINER=LANCache-Prefill \
    PREFILL_DIR=/lancacheprefill/SteamPrefill \
    PREFILL_USER=prefill \
    CACHEDECK_VERSION=0.3.0 \
    PORT=8080

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        docker-cli \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=3)"

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port \"${PORT}\""]
