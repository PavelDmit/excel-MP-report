FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.7.21 /uv /uvx /bin/

WORKDIR /usr/src/app/

RUN useradd -r -s /bin/false -d /home/appuser/ -m appuser && \
    chown -R appuser:appuser /home/appuser/ && \
    chmod -R 755 /home/appuser/ && \
    chown -R appuser:appuser /usr/src/app/ && \
    chmod -R 755 /usr/src/app/

USER appuser

COPY ./pyproject.toml ./uv.lock /usr/src/app/

RUN uv sync --locked

COPY . /usr/src/app/
