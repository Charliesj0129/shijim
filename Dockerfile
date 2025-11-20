FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SHIJIM_RAW_DIR=/data/raw \
    SHIJIM_FALLBACK_DIR=/data/fallback

WORKDIR /app

COPY pyproject.toml README.md ./
COPY shijim ./shijim

RUN pip install --upgrade pip \
    && pip install '.[clickhouse]' \
    && mkdir -p $SHIJIM_RAW_DIR $SHIJIM_FALLBACK_DIR

CMD ["python", "-m", "shijim"]
