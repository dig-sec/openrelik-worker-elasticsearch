FROM ubuntu:24.04

RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

ARG OPENRELIK_PYDEBUG
ENV OPENRELIK_PYDEBUG=${OPENRELIK_PYDEBUG:-0}
ARG OPENRELIK_PYDEBUG_PORT
ENV OPENRELIK_PYDEBUG_PORT=${OPENRELIK_PYDEBUG_PORT:-5678}

WORKDIR /openrelik

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml ./
RUN uv sync --no-install-project --no-dev

COPY . ./
RUN uv sync --no-dev

ENV PATH="/openrelik/.venv/bin:$PATH"

CMD ["celery", "--app=src.app", "worker", "--task-events", "--concurrency=1", "--loglevel=INFO", "-Q", "openrelik-worker-elasticsearch"]
