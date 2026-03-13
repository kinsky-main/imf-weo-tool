from __future__ import annotations

import pytest

import weo_tools.app as app
from weo_tools.configuration import RuntimeSettings
from weo_tools.imf import AvailabilityAggregate, AvailabilityResult, Catalog, ReleaseInfo
from weo_tools.legacy import AliasConfig, LegacyCatalog
from weo_tools.regions import RegionMembership


class DummyClient:
    def __init__(
        self,
        *,
        indicator_availability_by_country: dict[str, list[str]] | None = None,
        country_availability_by_indicator: dict[str, list[str]] | None = None,
        available_location_codes: list[str] | None = None,
        available_indicator_catalog_codes: list[str] | None = None,
        available_frequency_codes: list[str] | None = None,
    ) -> None:
        self.indicator_availability_by_country = indicator_availability_by_country or {}
        self.country_availability_by_indicator = country_availability_by_indicator or {}
        self.available_location_codes = list(available_location_codes or [])
        self.available_indicator_catalog_codes = list(available_indicator_catalog_codes or [])
        self.available_frequency_codes = list(available_frequency_codes or ["A"])

    def fetch_indicator_availability(
        self,
        values: list[str],
        frequency: str,
        *,
        strict: bool = True,
    ) -> AvailabilityAggregate:
        del frequency, strict
        return _aggregate(values, self.indicator_availability_by_country)

    def fetch_country_availability(
        self,
        values: list[str],
        frequency: str,
        *,
        strict: bool = True,
    ) -> AvailabilityAggregate:
        del frequency, strict
        return _aggregate(values, self.country_availability_by_indicator)

    def fetch_available_location_codes(self, frequency: str) -> list[str]:
        del frequency
        return list(self.available_location_codes)

    def fetch_available_indicator_catalog_codes(self, frequency: str) -> list[str]:
        del frequency
        return list(self.available_indicator_catalog_codes)

    def fetch_available_frequency_codes(self) -> list[str]:
        return list(self.available_frequency_codes)


def _aggregate(values: list[str], mapping: dict[str, list[str]]) -> AvailabilityAggregate:
    results = [
        AvailabilityResult(
            requested_code=value,
            available_codes=list(mapping.get(value, [])),
            series_count=len(mapping.get(value, [])),
        )
        for value in values
    ]

    counts_by_code: dict[str, int] = {}
    common_codes: set[str] | None = None
    for result in results:
        current_codes = set(result.available_codes)
        for code in current_codes:
            counts_by_code[code] = counts_by_code.get(code, 0) + 1
        if common_codes is None:
            common_codes = current_codes
        else:
            common_codes &= current_codes

    return AvailabilityAggregate(
        results=results,
        available_codes=sorted(counts_by_code),
        common_codes=sorted(common_codes or set()),
        counts_by_code=dict(sorted(counts_by_code.items())),
    )


def _catalog() -> Catalog:
    locations = {
        "AUT": "Austria",
        "GBR": "United Kingdom",
        "U150": "Europe",
    }
    return Catalog(
        release=ReleaseInfo(version="9.0.0", updated_at="2026-03-12", name="WEO"),
        countries={"AUT": locations["AUT"], "GBR": locations["GBR"]},
        country_groups={"U150": locations["U150"]},
        locations=locations,
        indicators={
            "NGDP": "GDP current national currency",
            "NGDPD": "GDP current US dollar",
            "PCPIPCH": "Inflation",
            "LP": "Population",
        },
        frequencies={"A": "Annual", "Q": "Quarterly"},
        units={
            "USD": "US dollar",
            "XDC": "Domestic currency",
            "PCH": "Percent change",
            "MIL": "Millions",
        },
        scales={"9": "Billions", "0": "Units"},
    )


def _legacy() -> LegacyCatalog:
    return LegacyCatalog(
        country_aliases={"AUT": {"Austria"}, "GBR": {"United Kingdom"}},
        subject_aliases={
            "NGDP": {"Gross domestic product, current prices"},
            "NGDPD": {"Gross domestic product, current prices"},
            "PCPIPCH": {"Inflation, average consumer prices"},
            "LP": {"Population"},
        },
        preferred_country_labels={"AUT": "Austria", "GBR": "United Kingdom"},
        preferred_subject_labels={
            "NGDP": "Gross domestic product, current prices",
            "NGDPD": "Gross domestic product, current prices",
            "PCPIPCH": "Inflation, average consumer prices",
            "LP": "Population",
        },
        indicator_units={
            "NGDP": {"National currency"},
            "NGDPD": {"U.S. dollars"},
            "PCPIPCH": {"Percent change"},
            "LP": {"Millions"},
        },
        indicator_scales={
            "NGDP": {"Billions"},
            "NGDPD": {"Billions"},
            "PCPIPCH": {"Units"},
            "LP": {"Units"},
        },
        preferred_unit_labels={
            "NGDP": "National currency",
            "NGDPD": "U.S. dollars",
            "PCPIPCH": "Percent change",
            "LP": "Millions",
        },
        preferred_scale_labels={
            "NGDP": "Billions",
            "NGDPD": "Billions",
            "PCPIPCH": "Units",
            "LP": "Units",
        },
    )


def _aliases() -> AliasConfig:
    return AliasConfig(
        units={
            "national currency": "XDC",
            "u s dollars": "USD",
            "percent change": "PCH",
            "millions": "MIL",
        },
        scales={"billions": "9", "units": "0"},
        unit_display={
            "USD": "U.S. dollars",
            "XDC": "National currency",
            "PCH": "Percent change",
            "MIL": "Millions",
        },
        scale_display={"9": "Billions", "0": "Units"},
    )


def _regions() -> RegionMembership:
    return RegionMembership(members_by_region={"U150": ["AUT", "GBR"]})


def _resolve(settings: RuntimeSettings, client: DummyClient) -> app.ResolvedSelections:
    return app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), _regions(), client, "A")


def test_selection_order_for_settings_prefers_existing_inputs() -> None:
    assert app._selection_order_for_settings(RuntimeSettings(countries=["Austria"])) == app.COUNTRY_FIRST
    assert app._selection_order_for_settings(RuntimeSettings(subject_descriptors=["NGDPD"])) == app.INDICATOR_FIRST
    assert app._selection_order_for_settings(RuntimeSettings(interactive=True)) is None


def test_resolve_frequency_code_validates_unavailable_frequency(monkeypatch) -> None:
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    with pytest.raises(ValueError, match="Frequency 'Q' is not available"):
        app._resolve_frequency_code(RuntimeSettings(frequency="Q"), _catalog(), DummyClient(available_frequency_codes=["A"]))


def test_resolve_frequency_code_auto_skips_single_available_frequency(monkeypatch) -> None:
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))
    monkeypatch.setattr(
        app,
        "prompt_for_choice",
        lambda title, choices: (_ for _ in ()).throw(AssertionError(title)),
    )

    resolved = app._resolve_frequency_code(
        RuntimeSettings(frequency="", interactive=True),
        _catalog(),
        DummyClient(available_frequency_codes=["A"]),
    )

    assert resolved == "A"


def test_resolve_selections_country_first_prompts_regions_then_countries(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    client = DummyClient(
        indicator_availability_by_country={
            "AUT": ["NGDPD"],
            "GBR": ["NGDP", "NGDPD"],
        },
        available_location_codes=["AUT", "GBR"],
    )

    monkeypatch.setattr(app, "prompt_for_choice", lambda title, choices: app.COUNTRY_FIRST)

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del required
        if title == "Select regions":
            assert choices[0]["value"] == "U150"
            assert choices[0]["detail"] == "2 countries"
            return ["U150"]
        if title == "Select countries":
            assert [choice["value"] for choice in choices] == ["AUT", "GBR"]
            assert {choice["value"]: choice.get("checked", False) for choice in choices} == {"AUT": True, "GBR": True}
            return ["GBR"]
        if title == "Select subject descriptors":
            assert {choice["value"]: choice.get("detail", "") for choice in choices} == {
                "NGDP": "1/1 location",
                "NGDPD": "1/1 location",
            }
            return ["NGDPD"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.country_codes == ["GBR"]
    assert selections.indicator_codes == ["NGDPD"]


def test_resolve_selections_indicator_first_prompts_regions_then_countries(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    client = DummyClient(
        country_availability_by_indicator={"NGDPD": ["AUT", "GBR"]},
        available_indicator_catalog_codes=["NGDPD"],
    )

    monkeypatch.setattr(app, "prompt_for_choice", lambda title, choices: app.INDICATOR_FIRST)

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del required
        if title == "Select subject descriptors":
            assert [choice["value"] for choice in choices] == ["NGDPD"]
            assert {choice["value"]: choice.get("detail", "") for choice in choices} == {"NGDPD": "2 locations"}
            return ["NGDPD"]
        if title == "Select regions":
            assert choices[0]["value"] == "U150"
            return ["U150"]
        if title == "Select countries":
            assert [choice["value"] for choice in choices] == ["AUT", "GBR"]
            assert {choice["value"]: choice.get("checked", False) for choice in choices} == {"AUT": True, "GBR": True}
            return ["AUT"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.country_codes == ["AUT"]
    assert selections.indicator_codes == ["NGDPD"]


def test_resolve_selections_expands_explicit_region_codes_non_interactively(monkeypatch) -> None:
    settings = RuntimeSettings(countries=["U150"], subject_descriptors=["NGDPD"])
    client = DummyClient(
        indicator_availability_by_country={
            "AUT": ["NGDPD"],
            "GBR": ["NGDPD"],
        }
    )

    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.country_codes == ["AUT", "GBR"]
    assert selections.indicator_codes == ["NGDPD"]


def test_resolve_selections_auto_skips_single_unit_and_scale(monkeypatch) -> None:
    settings = RuntimeSettings(countries=["GBR"], subject_descriptors=["NGDPD"], interactive=True)
    client = DummyClient(indicator_availability_by_country={"GBR": ["NGDPD"]})

    monkeypatch.setattr(
        app,
        "prompt_for_choices",
        lambda title, choices, required=True: (_ for _ in ()).throw(AssertionError(title)),
    )
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.indicator_codes == ["NGDPD"]
    assert selections.unit_codes == ["USD"]
    assert selections.scale_codes == []


def test_resolve_selections_prompts_when_multiple_units_exist(monkeypatch) -> None:
    settings = RuntimeSettings(
        countries=["GBR"],
        subject_descriptors=["Gross domestic product, current prices"],
        interactive=True,
    )
    client = DummyClient(indicator_availability_by_country={"GBR": ["NGDP", "NGDPD"]})

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del choices, required
        if title == "Select units for Gross domestic product, current prices":
            return ["U.S. dollars"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.indicator_codes == ["NGDPD"]
    assert selections.unit_codes == ["USD"]


def test_resolve_selections_keeps_unrelated_single_option_series_when_prompting_units(monkeypatch) -> None:
    settings = RuntimeSettings(
        countries=["GBR"],
        subject_descriptors=["Gross domestic product, current prices", "Inflation, average consumer prices"],
        interactive=True,
    )
    client = DummyClient(indicator_availability_by_country={"GBR": ["NGDP", "NGDPD", "PCPIPCH"]})

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del choices, required
        if title == "Select units for Gross domestic product, current prices":
            return ["U.S. dollars"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.indicator_codes == ["NGDPD", "PCPIPCH"]
    assert selections.unit_codes == ["USD", "PCH"]


def test_resolve_selections_skips_prompt_for_non_ambiguous_mixed_units(monkeypatch) -> None:
    settings = RuntimeSettings(
        countries=["GBR"],
        subject_descriptors=["Inflation, average consumer prices", "Population"],
        interactive=True,
    )
    client = DummyClient(indicator_availability_by_country={"GBR": ["PCPIPCH", "LP"]})

    monkeypatch.setattr(
        app,
        "prompt_for_choices",
        lambda title, choices, required=True: (_ for _ in ()).throw(AssertionError(title)),
    )
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.indicator_codes == ["PCPIPCH", "LP"]
