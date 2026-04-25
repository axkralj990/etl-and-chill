FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    VENV_PATH=/opt/venv

RUN python -m venv "$VENV_PATH"
ENV PATH="$VENV_PATH/bin:$PATH"

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip setuptools wheel && pip install .


FROM python:3.12-slim AS runtime

ENV VENV_PATH=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    TIMEZONE=Europe/Ljubljana

RUN apt-get update \
    && apt-get install -y --no-install-recommends cron tzdata tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY src ./src
COPY config ./config
COPY .env.example ./.env.example
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY docker/run-sync.sh /usr/local/bin/run-sync.sh
COPY docker/life-cron /etc/cron.d/life-cron

RUN chmod 0644 /etc/cron.d/life-cron \
    && chmod 0755 /usr/local/bin/entrypoint.sh /usr/local/bin/run-sync.sh \
    && touch /var/log/life-sync.log

EXPOSE 8501

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
