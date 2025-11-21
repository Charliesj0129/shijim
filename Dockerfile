FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SHIJIM_RAW_DIR=/data/raw \
    SHIJIM_FALLBACK_DIR=/data/fallback

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml README.md ./
COPY shijim ./shijim

# 安裝套件（含 clickhouse extra），保持非可寫層
RUN pip install --no-cache-dir ".[clickhouse]"

RUN mkdir -p ${SHIJIM_RAW_DIR} ${SHIJIM_FALLBACK_DIR}

CMD ["python", "-m", "shijim"]
