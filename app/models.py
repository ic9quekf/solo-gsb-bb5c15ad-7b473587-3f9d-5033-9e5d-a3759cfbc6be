"""Pydantic request models."""
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class SettingsIn(BaseModel):
    target_url: str = Field(min_length=1, examples=["http://host.docker.internal:9000/hook"])
    hmac_secret: str = ""
    max_retries: int = Field(default=3, ge=0, le=10)
    timeout_seconds: float = Field(default=5.0, gt=0, le=60)

    @field_validator("target_url", "hmac_secret", mode="before")
    @classmethod
    def _as_str(cls, v):
        return v if isinstance(v, str) else str(v)

    @field_validator("max_retries", "timeout_seconds", mode="before")
    @classmethod
    def _coerce_number(cls, v):
        if isinstance(v, str):
            return float(v)
        return v


class ReplayIn(BaseModel):
    only_failed: bool = False
