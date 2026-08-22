FROM python:3.12-slim

RUN pip install --no-cache-dir uv==0.5.4

WORKDIR /app

COPY services/analyst-agent/pyproject.toml /app/pyproject.toml
COPY services/analyst-agent/src/ /app/src/

RUN uv pip install --system --no-cache -e .

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

EXPOSE 8100

# Serve mode by default; override the command to ask a one-off question:
#   docker compose run --rm analyst-agent python -m analyst_agent "what's trending?"
ENTRYPOINT ["python", "-m", "analyst_agent"]
CMD ["--serve"]
