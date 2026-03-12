from __future__ import annotations

from argparse import Namespace

from weo_tools.app import _resolve_codes, _resolve_optional_code_from_label
from weo_tools.configuration import RuntimeSettings, merge_settings


def test_merge_settings_cli_overrides_defaults() -> None:
    defaults = RuntimeSettings(
        countries=["Austria"],
        subject_descriptors=["Gross domestic product, current prices"],
        units=["U.S. dollars"],
        scales=["Billions"],
        frequency="A",
        start_year=2020,
        end_year=2030,
        interactive=False,
        output_path="old.csv",
    )
    args = Namespace(
        country=["United Kingdom"],
        subject_descriptor=["Gross domestic product, current prices"],
        unit=["U.S. dollars"],
        scale=["Billions"],
        frequency="A",
        start_year=2019,
        end_year=2028,
        output="new.csv",
        compatibility_workbook=None,
        alias_file=None,
        interactive=True,
        config="config/weo_defaults.toml",
    )

    merged = merge_settings(defaults, args)

    assert merged.countries == ["United Kingdom"]
    assert merged.start_year == 2019
    assert merged.end_year == 2028
    assert merged.output_path == "new.csv"
    assert merged.interactive is True


def test_subject_resolution_can_expand_to_multiple_indicator_codes() -> None:
    resolved = _resolve_codes(
        requested=["Gross domestic product, current prices"],
        current_labels={"NGDP": "GDP current national currency", "NGDPD": "GDP current US dollar"},
        preferred_labels={
            "NGDP": "Gross domestic product, current prices",
            "NGDPD": "Gross domestic product, current prices",
        },
        workbook_aliases={
            "NGDP": {"Gross domestic product, current prices"},
            "NGDPD": {"Gross domestic product, current prices"},
        },
        manual_aliases={},
        entity_name="subject descriptor",
        allow_multiple_matches=True,
    )

    assert resolved == ["NGDP", "NGDPD"]


def test_optional_unit_code_resolution_uses_aliases() -> None:
    resolved = _resolve_optional_code_from_label(
        "National currency",
        current_labels={"USD": "US dollar", "XDC": "Domestic currency"},
        manual_aliases={"national currency": "XDC"},
    )

    assert resolved == "XDC"
