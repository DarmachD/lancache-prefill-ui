FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io bash \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

ENV TARGET_CONTAINER=LANCache-Prefill \
    PREFILL_DIR=/lancacheprefill/SteamPrefill \
    PREFILL_USER=prefill \
    PORT=8080

EXPOSE 8080
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
