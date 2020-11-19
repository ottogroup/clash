FROM python:3.8-slim-buster as base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1

WORKDIR /app

FROM base as builder

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.1.1

RUN apt-get update --fix-missing && apt-get install -y git build-essential gcc bzip2 curl ca-certificates && \
    pip install "poetry==$POETRY_VERSION" && \
    python -m venv /venv

COPY python/poetry.lock  python/poetry.lock
COPY python/pyproject.toml  python/pyproject.toml

RUN cd python && poetry export --without-hashes -f requirements.txt | /venv/bin/pip install -r /dev/stdin

COPY python/pyclash  python/pyclash
RUN cd python && poetry build && /venv/bin/pip install dist/*.whl

FROM base as final

COPY --from=builder /venv /venv

RUN useradd --create-home app
WORKDIR /home/app

RUN chown -R app:app /home/app

USER app

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH="/venv/bin:$PATH"

ENTRYPOINT []
