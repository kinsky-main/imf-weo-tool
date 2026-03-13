from __future__ import annotations

from argparse import Namespace

from weo_tools.app import _resolve_codes
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
        alias_file=None,
        interactive=True,
        config="config/weo_defaults.toml",
    )

    merged = merge_settings(defaults, args)

    assert merged.countries == ["United Kingdom"]
    assert merged.start_year == 2019
    assert merged.end_year == 2028
    assert merged.frequency_explicit is True
    assert merged.start_year_explicit is True
    assert merged.end_year_explicit is True
    assert merged.output_path == "new.csv"
    assert merged.interactive is True


def test_subject_resolution_can_expand_to_multiple_indicator_codes() -> None:
    resolved = _resolve_codes(
        requested=["Gross domestic product, current prices"],
        current_labels={"NGDP": "GDP current national currency", "NGDPD": "GDP current US dollar"},
        preferred_labels={
            "NGDP": "GDP current national currency",
            "NGDPD": "GDP current US dollar",
        },
        manual_aliases={
            "gross domestic product current prices": ["NGDP", "NGDPD"],
        },
        entity_name="subject descriptor",
        allow_multiple_matches=True,
    )

    assert resolved == ["NGDP", "NGDPD"]
