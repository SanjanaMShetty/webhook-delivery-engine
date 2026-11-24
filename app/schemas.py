from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, SecretStr, field_validator


class EndpointCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    url: AnyHttpUrl
    event_types: list[str] = Field(min_length=1, max_length=100)

    signing_secret: SecretStr = Field(min_length=16, max_length=256)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name must not be blank")
        return value

    @field_validator("event_types")
    @classmethod
    def validate_event_types(cls, values: list[str]) -> list[str]:
        cleaned = [value.strip() for value in values]
        if any(not value or len(value) > 160 for value in cleaned):
            raise ValueError("event types must be non-empty and at most 160 characters")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("event types must not contain duplicates")
        return cleaned


class EndpointUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    url: AnyHttpUrl | None = None
    event_types: list[str] | None = Field(default=None, min_length=1, max_length=100)
    status: str | None = None

    signing_secret: SecretStr | None = Field(default=None, min_length=16, max_length=256)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str | None) -> str | None:
        if value is None:
            return value
        value = value.strip()
        if not value:
            raise ValueError("name must not be blank")
        return value

    @field_validator("event_types")
    @classmethod
    def validate_event_types(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return values
        cleaned = [value.strip() for value in values]
        if any(not value or len(value) > 160 for value in cleaned):
            raise ValueError("event types must be non-empty and at most 160 characters")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("event types must not contain duplicates")
        return cleaned


class EndpointRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    url: str
    event_types: list[str]
    status: str
    created_at: datetime
    updated_at: datetime


class EventCreate(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=160)
    payload: dict = Field(default_factory=dict)

    @field_validator("event_id", "event_type")
    @classmethod
    def strip_identifiers(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value must not be blank")
        return value


class EventAccepted(BaseModel):
    event_id: UUID
    producer_event_id: str
    duplicate: bool
    matched_endpoints: int
    delivery_ids: list[UUID]
