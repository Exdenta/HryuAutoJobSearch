"""Lossless convenience parser for normalized LinkedIn Actor dataset rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

SCHEMA_VERSION = "nomad-agent-job-v1"
SOURCE = "linkedin"
TOP_LEVEL_KEYS = frozenset(
    {"schemaVersion", "identity", "data", "custom", "llm", "raw"}
)
LLM_STATUSES = frozenset({"not_requested", "completed", "failed"})


class OutputParseError(ValueError):
    """Raised when an Actor dataset row violates the expected envelope."""


def _mapping(value: object, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OutputParseError(f"{path} must be an object")
    return value


def _nullable_string(value: object, path: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise OutputParseError(f"{path} must be a non-empty string or null")
    return value


def _locations(value: object) -> tuple[Mapping[str, Any], ...] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise OutputParseError("data.locations must be an array or null")
    return tuple(
        _mapping(location, f"data.locations[{index}]")
        for index, location in enumerate(value)
    )


@dataclass(frozen=True, slots=True)
class LinkedInJob:
    """Convenient fields plus the complete normalized dataset record."""

    external_id: str | None
    posting_url: str | None
    title: str | None
    company_name: str | None
    locations: tuple[Mapping[str, Any], ...] | None
    posted_at: str | None
    application_url: str | None
    description: str | None
    llm_status: str
    normalized: Mapping[str, Any]


def parse_linkedin_output(item: Mapping[str, Any]) -> LinkedInJob:
    """Validate one LinkedIn row's stable envelope and return a lossless view."""

    record = _mapping(item, "record")
    if set(record) != TOP_LEVEL_KEYS:
        missing = sorted(TOP_LEVEL_KEYS - set(record))
        extra = sorted(set(record) - TOP_LEVEL_KEYS)
        raise OutputParseError(
            f"record has missing keys {missing!r} and extra keys {extra!r}"
        )
    if record["schemaVersion"] != SCHEMA_VERSION:
        raise OutputParseError(
            f"schemaVersion must be {SCHEMA_VERSION!r}, "
            f"got {record['schemaVersion']!r}"
        )

    identity = _mapping(record["identity"], "identity")
    if identity.get("source") != SOURCE:
        raise OutputParseError(
            f"identity.source must be {SOURCE!r}, "
            f"got {identity.get('source')!r}"
        )
    if record["custom"] is not None:
        raise OutputParseError("custom must be null for LinkedIn records")

    data = _mapping(record["data"], "data")
    company = _mapping(data.get("company"), "data.company")
    application = _mapping(data.get("application"), "data.application")
    llm = _mapping(record["llm"], "llm")
    raw = _mapping(record["raw"], "raw")

    llm_status = llm.get("status")
    if llm_status not in LLM_STATUSES:
        raise OutputParseError(
            f"llm.status must be one of {sorted(LLM_STATUSES)!r}"
        )

    return LinkedInJob(
        external_id=_nullable_string(
            identity.get("externalId"), "identity.externalId"
        ),
        posting_url=_nullable_string(identity.get("url"), "identity.url"),
        title=_nullable_string(data.get("title"), "data.title"),
        company_name=_nullable_string(
            company.get("name"), "data.company.name"
        ),
        locations=_locations(data.get("locations")),
        posted_at=_nullable_string(
            application.get("postedAt"), "data.application.postedAt"
        ),
        application_url=_nullable_string(
            application.get("url"), "data.application.url"
        ),
        description=_nullable_string(
            raw.get("description"), "raw.description"
        ),
        llm_status=llm_status,
        normalized=dict(record),
    )


def parse_linkedin_outputs(
    items: Iterable[Mapping[str, Any]],
) -> list[LinkedInJob]:
    """Parse all rows returned by an Apify dataset call."""

    return [parse_linkedin_output(item) for item in items]
