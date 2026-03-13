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
        available_time_periods: list[int] | None = None,
    ) -> None:
        self.indicator_availability_by_country = indicator_availability_by_country or {}
        self.country_availability_by_indicator = country_availability_by_indicator or {}
        self.available_location_codes = list(available_location_codes or [])
        self.available_indicator_catalog_codes = list(available_indicator_catalog_codes or [])
        self.available_frequency_codes = list(available_frequency_codes or ["A"])
        self.available_time_periods = list(available_time_periods or [])

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

    def fetch_available_time_periods(
        self,
        country_codes: list[str],
        indicator_codes: list[str],
        frequency: str,
    ) -> list[int]:
        del country_codes, indicator_codes, frequency
        return list(self.available_time_periods)


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


def test_resolve_date_range_prompts_for_missing_start_and_end_years(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    prompts: list[tuple[str, list[str]]] = []

    def fake_prompt_for_choice(title: str, choices: list[dict[str, str]]) -> str:
        prompts.append((title, [choice["value"] for choice in choices]))
        return choices[0]["value"] if title == "Select start year" else choices[-1]["value"]

    monkeypatch.setattr(app, "prompt_for_choice", fake_prompt_for_choice)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    start_year, end_year = app._resolve_date_range(
        settings,
        DummyClient(available_time_periods=[2022, 2023, 2024]),
        ["GBR"],
        ["NGDPD"],
        "A",
    )

    assert (start_year, end_year) == (2022, 2024)
    assert prompts == [
        ("Select start year", ["2022", "2023", "2024"]),
        ("Select end year", ["2022", "2023", "2024"]),
    ]


def test_resolve_date_range_constrains_end_year_choices_by_selected_start(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True, start_year=2023)
    captured_choices: dict[str, list[str]] = {}

    def fake_prompt_for_choice(title: str, choices: list[dict[str, str]]) -> str:
        captured_choices[title] = [choice["value"] for choice in choices]
        return choices[-1]["value"]

    monkeypatch.setattr(app, "prompt_for_choice", fake_prompt_for_choice)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    start_year, end_year = app._resolve_date_range(
        settings,
        DummyClient(available_time_periods=[2021, 2022, 2023, 2024]),
        ["GBR"],
        ["NGDPD"],
        "A",
    )

    assert (start_year, end_year) == (2023, 2024)
    assert captured_choices["Select end year"] == ["2023", "2024"]


def test_resolve_date_range_rejects_reversed_years() -> None:
    with pytest.raises(ValueError, match="Start year 2025 cannot be later than end year 2024"):
        app._resolve_date_range(
            RuntimeSettings(start_year=2025, end_year=2024),
            DummyClient(),
            ["GBR"],
            ["NGDPD"],
            "A",
        )


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


def test_resolve_selections_indicator_first_filters_zero_availability_subject_rows(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    client = DummyClient(
        country_availability_by_indicator={
            "NGDPD": ["AUT", "GBR"],
            "NGDP": [],
        },
        available_indicator_catalog_codes=["NGDPD", "NGDP"],
    )

    monkeypatch.setattr(app, "prompt_for_choice", lambda title, choices: app.INDICATOR_FIRST)

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del required
        if title == "Select subject descriptors":
            assert [choice["value"] for choice in choices] == ["NGDPD"]
            return ["NGDPD"]
        if title == "Select regions":
            return []
        if title == "Select countries":
            assert [choice["value"] for choice in choices] == ["AUT", "GBR"]
            return ["GBR"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.country_codes == ["GBR"]
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


def test_resolve_selections_supports_all_country_wildcard_non_interactively(monkeypatch) -> None:
    settings = RuntimeSettings(countries=["*"], subject_descriptors=["NGDPD"])
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


def test_resolve_selections_skips_prompt_for_non_ambiguous_mixed_units_within_subject(monkeypatch) -> None:
    settings = RuntimeSettings(
        countries=["GBR"],
        subject_descriptors=["Gross domestic product, current prices"],
        interactive=True,
    )
    client = DummyClient(indicator_availability_by_country={"GBR": ["NGDP", "NGDPD"]})

    monkeypatch.setattr(
        app,
        "prompt_for_choices",
        lambda title, choices, required=True: (_ for _ in ()).throw(AssertionError(title)),
    )
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.indicator_codes == ["NGDP", "NGDPD"]
    assert selections.unit_codes == ["XDC", "USD"]


def test_resolve_selections_keeps_unrelated_single_option_series_without_prompting_units(monkeypatch) -> None:
    settings = RuntimeSettings(
        countries=["GBR"],
        subject_descriptors=["Gross domestic product, current prices", "Inflation, average consumer prices"],
        interactive=True,
    )
    client = DummyClient(indicator_availability_by_country={"GBR": ["NGDP", "NGDPD", "PCPIPCH"]})

    monkeypatch.setattr(
        app,
        "prompt_for_choices",
        lambda title, choices, required=True: (_ for _ in ()).throw(AssertionError(title)),
    )
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.indicator_codes == ["NGDP", "NGDPD", "PCPIPCH"]
    assert selections.unit_codes == ["XDC", "USD", "PCH"]


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


def test_resolve_unit_scale_filters_prompts_only_for_true_scale_ambiguity(monkeypatch) -> None:
    legacy = _legacy()
    legacy.indicator_scales["LP"] = {"Units", "Billions"}
    legacy.preferred_scale_labels["LP"] = "Units"
    prompts: list[str] = []

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del choices, required
        prompts.append(title)
        return ["Units"]

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)

    filters = app._resolve_unit_scale_filters(
        RuntimeSettings(interactive=True),
        ["LP"],
        legacy,
    )

    assert prompts == ["Select scales for Population"]
    assert filters.indicator_codes == ["LP"]
    assert filters.selected_scales == ["Units"]


def test_resolve_unit_scale_filters_prompts_only_for_true_unit_ambiguity(monkeypatch) -> None:
    legacy = _legacy()
    legacy.indicator_units["LP"] = {"Millions", "Persons"}
    legacy.preferred_unit_labels["LP"] = "Millions"
    prompts: list[str] = []

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del choices, required
        prompts.append(title)
        return ["Millions"]

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)

    filters = app._resolve_unit_scale_filters(
        RuntimeSettings(interactive=True),
        ["LP"],
        legacy,
    )

    assert prompts == ["Select units for Population"]
    assert filters.indicator_codes == ["LP"]
    assert filters.selected_units == ["Millions"]
