# Завдання 1 — Dockerfile для ingestor. Специфікація: SPEC.md → «Завдання 1».
#
# Один образ, одна стадія. Що має бути (деталі й бали — у SPEC.md):
#
#   * базовий образ python:3.12-slim з фіксованим тегом (не latest);
FROM python:3.12-slim

#   * PYTHONUNBUFFERED=1 — інакше логів завантаження не видно;
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
#   * non-root користувач (напр. appuser, uid 1000), USER — перед ENTRYPOINT;
RUN useradd --create-home --uid 1000 appuser

WORKDIR /app

#   * каталог /cache має належати цьому користувачу — туди монтується volume;
COPY app/requirements.txt ./app/requirements.txt
RUN pip install --no-cache-dir -r app/requirements.txt
#   * залежності з app/requirements.txt ставляться ОКРЕМИМ шаром ПЕРЕД копіюванням
#     коду, з --no-cache-dir (правка коду не має перебудовувати pip install);
#   * ENTRYPOINT в exec-form запускає app/gh_ingest.py.
RUN mkdir -p /cache && chown appuser:appuser /cache

COPY app/gh_ingest.py ./app/gh_ingest.py

USER appuser


# Перевірте збірку:  docker build -t gh-ingest:latest .
# Скрипт app/gh_ingest.py — ДАНО, не редагуйте його.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD ["python", "-c", "import psycopg"]

ENTRYPOINT ["python", "app/gh_ingest.py"]
# TODO: реалізуйте Dockerfile тут.
