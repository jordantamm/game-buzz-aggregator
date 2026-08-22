from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Analyst agent configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # MCP server's Streamable HTTP endpoint. Tools are discovered from here at
    # startup, never hand-declared, so adding a tool to mcp-server makes it
    # available to the agent with no code change.
    mcp_server_url: str = "http://mcp-server:8081/sse"

    anthropic_api_key: str = ""
    model: str = "claude-haiku-4-5-20251001"
    temperature: float = 0.0

    # Hard ceiling on tool calls per question. Without it, a reflect loop that
    # keeps deciding "try a broader window" runs until the context window fills.
    # On hitting the cap the agent answers from what it has, with confidence
    # lowered to reflect the shortfall.
    max_tool_calls: int = 6

    # Bounds a single tool call, so one slow query cannot hang the request.
    tool_timeout_seconds: int = 30

    server_port: int = 8100

    otel_exporter_otlp_endpoint: str = "http://otel-collector:4317"
    otel_service_name: str = "analyst-agent"
    log_level: str = "info"


settings = Settings()
