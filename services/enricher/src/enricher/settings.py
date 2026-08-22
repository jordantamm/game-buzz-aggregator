from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Enricher configuration. Env vars win over .env, per 12-factor."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── Kafka / Redpanda ──────────────────────────────────────────────
    redpanda_brokers: str = "localhost:9092"
    input_topic: str = "mentions.raw"
    output_topic: str = "mentions.enriched"
    consumer_group: str = "enricher-v1"

    # ── Temporal ──────────────────────────────────────────────────────
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    task_queue: str = "enricher"
    max_concurrent_activities: int = 8

    # ── Postgres / Redis ──────────────────────────────────────────────
    postgres_dsn: str = "postgresql://gba:gba_dev_password@localhost:5432/gba"
    redis_addr: str = "localhost:6379"
    games_cache_refresh_seconds: int = 300

    # ── LLM disambiguation ────────────────────────────────────────────
    # Empty api key is a supported configuration: ambiguous mentions resolve to
    # no match rather than the service failing to start.
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-haiku-4-5-20251001"
    # Instructor re-asks the model this many times when its output fails schema
    # validation before giving up.
    llm_max_retries: int = 2

    # ── Embedding HTTP server ─────────────────────────────────────────
    embed_server_port: int = 8000

    # ── Observability ─────────────────────────────────────────────────
    otel_exporter_otlp_endpoint: str = "http://otel-collector:4317"
    otel_service_name: str = "enricher"
    log_level: str = "info"

    enricher_version: str = "0.1.0"


settings = Settings()
