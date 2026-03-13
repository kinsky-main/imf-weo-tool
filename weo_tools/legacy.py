from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
import tomllib


def normalize_label(value: str) -> str:
    compact = re.sub(r"[^0-9a-z]+", " ", value.lower()).strip()
    return re.sub(r"\s+", " ", compact)


@dataclass(slots=True)
class AliasConfig:
    units: dict[str, str] = field(default_factory=dict)
    scales: dict[str, str] = field(default_factory=dict)
    unit_display: dict[str, str] = field(default_factory=dict)
    scale_display: dict[str, str] = field(default_factory=dict)
    countries: dict[str, list[str]] = field(default_factory=dict)
    subjects: dict[str, list[str]] = field(default_factory=dict)


def load_alias_config(path: str | Path) -> AliasConfig:
    alias_path = Path(path)
    if not alias_path.exists():
        return AliasConfig()

    with alias_path.open("rb") as handle:
        raw = tomllib.load(handle)

    def normalize_mapping(section: str) -> dict[str, str]:
        entries = raw.get(section, {})
        return {normalize_label(key): str(value) for key, value in entries.items()}

    def normalize_code_mapping(section: str) -> dict[str, list[str]]:
        entries = raw.get(section, {})
        normalized: dict[str, list[str]] = {}
        for key, value in entries.items():
            alias = normalize_label(key)
            if isinstance(value, list):
                codes = [str(item).strip() for item in value if str(item).strip()]
            else:
                code = str(value).strip()
                codes = [code] if code else []
            if codes:
                normalized[alias] = codes
        return normalized

    return AliasConfig(
        units=normalize_mapping("units"),
        scales=normalize_mapping("scales"),
        countries=normalize_code_mapping("countries"),
        subjects=normalize_code_mapping("subjects"),
        unit_display={str(key): str(value) for key, value in raw.get("unit_display", {}).items()},
        scale_display={str(key): str(value) for key, value in raw.get("scale_display", {}).items()},
    )
