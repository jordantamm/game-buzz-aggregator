from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redpanda_brokers: str = "localhost:9092"
    input_topic: str = "mentions.raw"
    output_topic: str = "mentions.enriched"
    consumer_group: str = "enricher-v1"

    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    task_queue: str = "enricher"

    postgres_dsn: str = "postgresql://gba:gba_dev_password@localhost:5432/gba"

    anthropic_api_key: str = ""
    anthropic_model: str = "claude-haiku-4-5-20251001"

    redis_addr: str = "localhost:6379"

    otel_exporter_otlp_endpoint: str = "http://otel-collector:4317"
    otel_service_name: str = "enricher"

    log_level: str = "info"

    enricher_version: str = "0.1.0"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
