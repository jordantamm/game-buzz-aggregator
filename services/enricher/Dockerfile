FROM python:3.12-slim

# Install uv
RUN pip install uv==0.5.4

WORKDIR /app

# Copy generated Python protos
COPY gen/python/ /app/gen/python/

# Install dependencies
COPY services/enricher/pyproject.toml /app/pyproject.toml
RUN uv pip install --system -e .

# Copy source
COPY services/enricher/src/ /app/src/

ENV PYTHONPATH=/app/src:/app/gen/python

ENTRYPOINT ["python", "-m", "enricher"]
