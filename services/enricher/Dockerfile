FROM python:3.12-slim

RUN pip install --no-cache-dir uv==0.5.4

WORKDIR /app

# Generated protobuf modules, imported via PYTHONPATH rather than packaged.
COPY gen/python/ /app/gen/python/

# pyproject declares packages = ["src/enricher"], so hatchling needs the source
# tree present at install time - copying it after the install fails the build.
COPY services/enricher/pyproject.toml /app/pyproject.toml
COPY services/enricher/src/ /app/src/

RUN uv pip install --system --no-cache -e .

# Bake the model checkpoints into the image. Without this every container start
# re-downloads ~500MB, and the first mention pays that cost inside an activity
# timeout, which looks like a flaky pipeline rather than a cold cache.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')" \
 && python -c "from transformers import pipeline; pipeline('text-classification', model='cardiffnlp/twitter-roberta-base-sentiment-latest')"

ENV PYTHONPATH=/app/src:/app/gen/python
ENV PYTHONUNBUFFERED=1

# /embed - query embeddings for the Go services' hybrid search
EXPOSE 8000

ENTRYPOINT ["python", "-m", "enricher"]
