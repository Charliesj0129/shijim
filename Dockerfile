FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY shijim ./shijim

RUN pip install --upgrade pip \
    && pip install .

# Shioaji credentials must be provided via environment variables at runtime:
#   SHIOAJI_API_KEY, SHIOAJI_SECRET_KEY, SHIOAJI_PERSON_ID, etc.
# Configure your ClickHouse DSN and fallback_dir via CLI args or custom config.

CMD ["python", "-m", "shijim"]
