"""Pydantic request models."""
from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator, model_validator

TARGET_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$")
MIN_TARGETS = 1   # single-target setups stay supported
MAX_TARGETS = 5


class TargetIn(BaseModel):
    name: str = Field(min_length=1, max_length=40, examples=["billing"])
    url: str = Field(min_length=1, examples=["http://host.docker.internal:9000/hook"])
    enabled: bool = True

    @field_validator("name", mode="before")
    @classmethod
    def _name_str(cls, v):
        return v if isinstance(v, str) else str(v)

    @field_validator("name")
    @classmethod
    def _name_shape(cls, v: str) -> str:
        v = v.strip()
        if not TARGET_NAME_RE.match(v):
            raise ValueError(
                "目标名称需为 1–40 位字母/数字/连字符/下划线，且以字母或数字开头")
        return v

    @field_validator("url", mode="before")
    @classmethod
    def _url_str(cls, v):
        return v if isinstance(v, str) else str(v)

    @field_validator("url")
    @classmethod
    def _url_shape(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith(("http://", "https://")):
            raise ValueError("目标 URL 必须以 http:// 或 https:// 开头")
        return v


def _validate_target_set(targets: list[TargetIn]) -> list[TargetIn]:
    if not MIN_TARGETS <= len(targets) <= MAX_TARGETS:
        raise ValueError(f"目标数量必须在 {MIN_TARGETS}–{MAX_TARGETS} 之间")
    names = [t.name for t in targets]
    if len(set(names)) != len(names):
        raise ValueError("目标名称不能重复")
    if not any(t.enabled for t in targets):
        raise ValueError("至少需要启用一个目标")
    return targets


class TargetsIn(BaseModel):
    targets: list[TargetIn]

    @model_validator(mode="after")
    def _check_set(self):
        _validate_target_set(self.targets)
        return self


class SettingsIn(BaseModel):
    target_url: str = Field(default="", examples=["http://host.docker.internal:9000/hook"])
    hmac_secret: str = ""
    max_retries: int = Field(default=3, ge=0, le=10)
    timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    # optional multi-target fan-out; when present it replaces the target set
    targets: list[TargetIn] | None = None

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

    @model_validator(mode="after")
    def _check(self):
        if self.targets is not None:
            _validate_target_set(self.targets)
        elif not self.target_url.strip():
            raise ValueError("target_url 不能为空（未提供 targets 时）")
        return self


class ReplayIn(BaseModel):
    only_failed: bool = False
    # restrict delivery to targets the event has not been delivered to yet
    only_failed_targets: bool = False


class EventReplayIn(BaseModel):
    # explicit target names take precedence over only_failed_targets
    targets: list[str] | None = None
    only_failed_targets: bool = False
