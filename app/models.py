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


class TargetIn(BaseModel):
    """One named fanout target."""
    name: str = Field(min_length=1, max_length=64, examples=["billing"])
    url: str = Field(min_length=1, examples=["http://127.0.0.1:8000/recv"])
    hmac_secret: str = ""
    max_retries: int = Field(default=3, ge=0, le=10)
    timeout_seconds: float = Field(default=5.0, gt=0, le=60)

    @field_validator("name", "url", "hmac_secret", mode="before")
    @classmethod
    def _as_str(cls, v):
        return v if isinstance(v, str) else str(v)

    @field_validator("max_retries", "timeout_seconds", mode="before")
    @classmethod
    def _coerce_number(cls, v):
        if isinstance(v, str):
            return float(v)
        return v

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("目标名称不能为空")
        if any(c.isspace() for c in v):
            raise ValueError("目标名称不能包含空白字符")
        return v

    @field_validator("url")
    @classmethod
    def _check_url(cls, v: str) -> str:
        v = v.strip()
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("目标 URL 必须以 http:// 或 https:// 开头")
        return v


class TargetsIn(BaseModel):
    """Replacement set of 2-5 named fanout targets."""
    targets: list[TargetIn] = Field(min_length=2, max_length=5)


class ReplayIn(BaseModel):
    only_failed: bool = False


class EventReplayIn(BaseModel):
    # "all": fan out to every target in a fresh replay round;
    # "failed": retry only targets whose delivery is not yet successful.
    scope: str = Field(default="all", pattern="^(all|failed)$")
