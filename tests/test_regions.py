from __future__ import annotations

from pathlib import Path

import pytest

from weo_tools.regions import load_region_membership


def test_load_region_membership_ignores_regions_outside_active_catalog(tmp_path: Path) -> None:
    path = tmp_path / "regions.toml"
    path.write_text(
        '[regions]\nU150 = ["AUT", "GBR"]\nU999 = ["ZZZ"]\n',
        encoding="utf-8",
    )

    membership = load_region_membership(
        path,
        valid_region_codes={"U150"},
        valid_country_codes={"AUT", "GBR"},
    )

    assert membership.members_by_region == {"U150": ["AUT", "GBR"]}


def test_load_region_membership_rejects_unknown_country_codes_for_active_region(tmp_path: Path) -> None:
    path = tmp_path / "regions.toml"
    path.write_text(
        '[regions]\nU150 = ["AUT", "ZZZ"]\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unknown country code"):
        load_region_membership(
            path,
            valid_region_codes={"U150"},
            valid_country_codes={"AUT", "GBR"},
        )
