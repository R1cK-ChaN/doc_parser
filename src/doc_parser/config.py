"""Application configuration via pydantic-settings."""

from __future__ import annotations

from pathlib import Path

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # TextIn API (required)
    textin_app_id: str
    textin_secret_code: str

    # Local storage root
    data_dir: Path = Path("data")

    # TextIn parse settings
    textin_parse_mode: str = "auto"
    textin_max_concurrent: int = 3

    # LLM extraction settings (OpenRouter / OpenAI-compatible)
    llm_api_key: str = ""
    llm_base_url: str = "https://openrouter.ai/api/v1"
    llm_model: str = "openai/gpt-4o-mini"
    llm_max_tokens: int = 1024
    llm_temperature: float = 0.0
    llm_context_chars: int = 4000

    # VLM chart summarization (empty = disabled)
    vlm_model: str = ""
    vlm_max_tokens: int = 300

    @computed_field  # type: ignore[prop-decorator]
    @property
    def parsed_path(self) -> Path:
        return self.data_dir / "parsed"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def extraction_path(self) -> Path:
        return self.data_dir / "extraction"

    def ensure_dirs(self) -> None:
        """Create data directories if they don't exist."""
        self.parsed_path.mkdir(parents=True, exist_ok=True)
        self.extraction_path.mkdir(parents=True, exist_ok=True)


def get_settings(**overrides: object) -> Settings:
    """Create a Settings instance, allowing overrides for testing."""
    return Settings(**overrides)  # type: ignore[arg-type]
