from __future__ import annotations

from pydantic import BaseModel, Field


class SubscribeItem(BaseModel):
    qid: str
    reason: str | None = None


class SubscribeRequest(BaseModel):
    qids: list[str] = Field(default_factory=list)
    items: list[SubscribeItem] = Field(default_factory=list)
    session_id: str | None = None


class CreatorHistoryRequest(BaseModel):
    window_start: str | None = None
    window_end: str | None = None
    force: bool = False


class PubSubCreateRequest(BaseModel):
    ttl_seconds: int = Field(gt=0)
    priority: int = Field(default=10, ge=0, le=1000)
    wants_creation: bool = False
    wants_content: bool = False
    wants_inlinks: bool = False
    qids: list[str] = Field(default_factory=list)


class PubSubAddRequest(BaseModel):
    qids: list[str] = Field(default_factory=list)
    priority: int = Field(default=10, ge=0, le=1000)
    wants_creation: bool | None = None
    wants_content: bool | None = None
    wants_inlinks: bool | None = None


class PubSubRefreshRequest(BaseModel):
    ttl_seconds: int = Field(gt=0)
