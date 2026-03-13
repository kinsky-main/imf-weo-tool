from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


DEFAULT_REGION_MEMBERSHIP_PATH = Path("config/weo_regions.toml")


@dataclass(slots=True)
class RegionMembership:
    members_by_region: dict[str, list[str]]

    def available_region_codes(self, available_country_codes: list[str]) -> list[str]:
        available = set(available_country_codes)
        return [
            code
            for code, members in self.members_by_region.items()
            if any(member in available for member in members)
        ]

    def expand_region_codes(
        self,
        region_codes: list[str],
        *,
        allowed_country_codes: list[str] | None = None,
    ) -> list[str]:
        allowed = set(allowed_country_codes or [])
        expanded: list[str] = []
        seen: set[str] = set()
        for code in region_codes:
            for member in self.members_by_region.get(code, []):
                if allowed and member not in allowed:
                    continue
                if member in seen:
                    continue
                seen.add(member)
                expanded.append(member)
        return expanded

    def count_countries(self, region_code: str, *, allowed_country_codes: list[str] | None = None) -> int:
        return len(self.expand_region_codes([region_code], allowed_country_codes=allowed_country_codes))


def load_region_membership(
    path: str | Path = DEFAULT_REGION_MEMBERSHIP_PATH,
    *,
    valid_region_codes: set[str] | None = None,
    valid_country_codes: set[str] | None = None,
) -> RegionMembership:
    membership_path = Path(path)
    if not membership_path.exists():
        return RegionMembership(members_by_region={})

    with membership_path.open("rb") as handle:
        raw = tomllib.load(handle)

    raw_regions = raw.get("regions", {})
    members_by_region: dict[str, list[str]] = {}
    for region_code, members in raw_regions.items():
        code = str(region_code).strip()
        if not code:
            continue
        if valid_region_codes is not None and code not in valid_region_codes:
            continue
        if not isinstance(members, list):
            raise ValueError(f"Region membership for {code} must be a list of country codes.")

        region_members: list[str] = []
        seen: set[str] = set()
        for member in members:
            country_code = str(member).strip()
            if not country_code:
                continue
            if valid_country_codes is not None and country_code not in valid_country_codes:
                raise ValueError(
                    f"Unknown country code in region membership file for {code}: {country_code}"
                )
            if country_code in seen:
                continue
            seen.add(country_code)
            region_members.append(country_code)
        members_by_region[code] = region_members

    return RegionMembership(members_by_region=members_by_region)
