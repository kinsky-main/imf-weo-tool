from __future__ import annotations

import pytest

import weo_tools.app as app
from weo_tools.configuration import RuntimeSettings
from weo_tools.imf import (
    AvailabilityAggregate,
    AvailabilityResult,
    Catalog,
    ReleaseInfo,
    SeriesVariant,
    TimePeriod,
    parse_time_period,
)
from weo_tools.legacy import AliasConfig, normalize_label
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
        available_scoped_frequency_codes: list[str] | None = None,
        available_time_periods: list[TimePeriod | int | str] | None = None,
        series_variants_by_indicator: dict[str, list[SeriesVariant]] | None = None,
    ) -> None:
        self.indicator_availability_by_country = indicator_availability_by_country or {}
        self.country_availability_by_indicator = country_availability_by_indicator or {}
        self.available_location_codes = list(available_location_codes or [])
        self.available_indicator_catalog_codes = list(available_indicator_catalog_codes or [])
        self.available_frequency_codes = list(available_frequency_codes or ["A"])
        self.available_scoped_frequency_codes = list(available_scoped_frequency_codes or self.available_frequency_codes)
        self.available_time_periods = list(available_time_periods or [])
        self.series_variants_by_indicator = {
            key: list(value)
            for key, value in (
                series_variants_by_indicator
                or {
                    "NGDP": [SeriesVariant(unit_code="XDC", scale_code="9")],
                    "NGDPD": [SeriesVariant(unit_code="USD", scale_code="9")],
                    "PCPIPCH": [SeriesVariant(unit_code="PCH", scale_code="0")],
                    "LP": [SeriesVariant(unit_code="MIL", scale_code="0")],
                }
            ).items()
        }

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

    def fetch_available_frequencies(
        self,
        country_codes: list[str],
        indicator_codes: list[str],
    ) -> list[str]:
        del country_codes, indicator_codes
        return list(self.available_scoped_frequency_codes)

    def fetch_indicator_frequency_availability(
        self,
        indicator_codes: list[str],
        country_codes: list[str] | None = None,
    ) -> dict[str, list[str]]:
        del country_codes
        return {indicator_code: list(self.available_scoped_frequency_codes) for indicator_code in indicator_codes}

    def fetch_available_time_periods(
        self,
        country_codes: list[str],
        indicator_codes: list[str],
        frequency: str,
    ) -> list[TimePeriod | int | str]:
        del country_codes, indicator_codes, frequency
        return list(self.available_time_periods)

    def fetch_indicator_series_variants(
        self,
        country_codes: list[str],
        indicator_codes: list[str],
        frequency: str,
    ) -> dict[str, list[SeriesVariant]]:
        del country_codes, frequency
        return {
            indicator_code: list(self.series_variants_by_indicator.get(indicator_code, []))
            for indicator_code in indicator_codes
        }


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
        "G001": "World",
        "U150": "Europe",
    }
    return Catalog(
        release=ReleaseInfo(version="9.0.0", updated_at="2026-03-12", name="WEO"),
        countries={"AUT": locations["AUT"], "GBR": locations["GBR"]},
        country_groups={"G001": locations["G001"], "U150": locations["U150"]},
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
        countries={
            normalize_label("Austria"): ["AUT"],
            normalize_label("United Kingdom"): ["GBR"],
            normalize_label("World"): ["G001"],
            normalize_label("Europe"): ["U150"],
        },
        subjects={
            normalize_label("Gross domestic product, current prices"): ["NGDP", "NGDPD"],
            normalize_label("Inflation, average consumer prices"): ["PCPIPCH"],
            normalize_label("Population"): ["LP"],
        },
    )


def _regions() -> RegionMembership:
    return RegionMembership(members_by_region={"U150": ["AUT", "GBR"]})


def _resolve(settings: RuntimeSettings, client: DummyClient) -> app.ResolvedSelections:
    return app._resolve_selections(settings, _catalog(), _aliases(), _regions(), client, "A")


def _series_variants() -> dict[str, list[SeriesVariant]]:
    return {
        "NGDP": [SeriesVariant(unit_code="XDC", scale_code="9")],
        "NGDPD": [SeriesVariant(unit_code="USD", scale_code="9")],
        "PCPIPCH": [SeriesVariant(unit_code="PCH", scale_code="0")],
        "LP": [SeriesVariant(unit_code="MIL", scale_code="0")],
    }


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


def test_resolve_frequency_code_uses_scoped_availability_when_context_is_known(monkeypatch) -> None:
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))
    monkeypatch.setattr(
        app,
        "prompt_for_choice",
        lambda title, choices: (_ for _ in ()).throw(AssertionError(title)),
    )

    resolved = app._resolve_frequency_code(
        RuntimeSettings(frequency="", interactive=True),
        _catalog(),
        DummyClient(available_frequency_codes=["A", "Q"], available_scoped_frequency_codes=["Q"]),
        country_codes=["GBR"],
        indicator_codes=["NGDPD"],
    )

    assert resolved == "Q"


def test_resolve_frequency_code_rejects_globally_valid_but_scope_invalid_frequency(monkeypatch) -> None:
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    with pytest.raises(ValueError, match="not available for the selected countries and subject descriptors"):
        app._resolve_frequency_code(
            RuntimeSettings(frequency="Q", frequency_explicit=True),
            _catalog(),
            DummyClient(available_frequency_codes=["A", "Q"], available_scoped_frequency_codes=["A"]),
            country_codes=["GBR"],
            indicator_codes=["NGDPD"],
        )


def test_resolve_date_range_prompts_for_missing_start_and_end_years(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    prompts: list[tuple[str, str, str]] = []

    def fake_prompt_for_time_range(**kwargs) -> tuple[str, str]:
        prompts.append((kwargs["title"], kwargs["start_value"], kwargs["end_value"]))
        return "2022", "2024"

    monkeypatch.setattr(app, "prompt_for_time_range", fake_prompt_for_time_range)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    start_year, end_year = app._resolve_date_range(
        settings,
        DummyClient(available_time_periods=[2022, 2023, 2024]),
        ["GBR"],
        ["NGDPD"],
        "A",
    )

    assert (start_year, end_year) == (2022, 2024)
    assert prompts == [("Select time range", "2022", "2024")]


def test_resolve_date_range_prompts_when_years_are_default_derived(monkeypatch) -> None:
    settings = RuntimeSettings(
        interactive=True,
        start_year=2020,
        end_year=2030,
        start_year_explicit=False,
        end_year_explicit=False,
    )
    prompts: list[tuple[str, str, str]] = []

    def fake_prompt_for_time_range(**kwargs) -> tuple[str, str]:
        prompts.append((kwargs["title"], kwargs["start_value"], kwargs["end_value"]))
        return "2022", "2024"

    monkeypatch.setattr(app, "prompt_for_time_range", fake_prompt_for_time_range)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    start_year, end_year = app._resolve_date_range(
        settings,
        DummyClient(available_time_periods=[2022, 2023, 2024]),
        ["GBR"],
        ["NGDPD"],
        "A",
    )

    assert (start_year, end_year) == (2022, 2024)
    assert prompts == [("Select time range", "2022", "2024")]


def test_resolve_date_range_constrains_end_year_choices_by_selected_start(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True, start_year=2023, start_year_explicit=True)
    prompts: list[tuple[str, str, str]] = []

    def fake_prompt_for_time_range(**kwargs) -> tuple[str, str]:
        prompts.append((kwargs["title"], kwargs["start_value"], kwargs["end_value"]))
        return "2023", "2024"

    monkeypatch.setattr(app, "prompt_for_time_range", fake_prompt_for_time_range)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    start_year, end_year = app._resolve_date_range(
        settings,
        DummyClient(available_time_periods=[2021, 2022, 2023, 2024]),
        ["GBR"],
        ["NGDPD"],
        "A",
    )

    assert (start_year, end_year) == (2023, 2024)
    assert prompts == [("Select time range", "2023", "2024")]


def test_resolve_date_range_rejects_reversed_years() -> None:
    with pytest.raises(ValueError, match="Start year 2025 cannot be later than end year 2024"):
        app._resolve_date_range(
            RuntimeSettings(start_year=2025, end_year=2024),
            DummyClient(),
            ["GBR"],
            ["NGDPD"],
            "A",
        )


def test_run_dataframe_interactive_clears_default_frequency_and_years(monkeypatch) -> None:
    settings = RuntimeSettings(
        interactive=True,
        frequency="A",
        start_year=2020,
        end_year=2030,
        frequency_explicit=False,
        start_year_explicit=False,
        end_year_explicit=False,
    )
    client = DummyClient()

    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))
    monkeypatch.setattr(app, "load_alias_config", lambda path: _aliases())
    monkeypatch.setattr(app, "_load_region_membership", lambda catalog: _regions())
    monkeypatch.setattr(client, "fetch_catalog", _catalog, raising=False)

    captured_frequency_inputs: list[str] = []
    captured_date_inputs: list[tuple[int | None, int | None]] = []

    monkeypatch.setattr(
        app,
        "_resolve_primary_selection_state",
        lambda *args, **kwargs: app.CoreSelectionState(country_codes=["GBR"], indicator_codes=["NGDPD"]),
    )

    def fake_resolve_frequency_code(
        runtime_settings: RuntimeSettings,
        catalog: Catalog,
        active_client: DummyClient,
        country_codes: list[str] | None = None,
        indicator_codes: list[str] | None = None,
    ) -> str:
        del catalog, active_client, country_codes, indicator_codes
        captured_frequency_inputs.append(runtime_settings.frequency)
        return "Q"

    monkeypatch.setattr(app, "_resolve_frequency_code", fake_resolve_frequency_code)
    monkeypatch.setattr(
        app,
        "_build_resolved_selections_for_core_state",
        lambda *args, **kwargs: app.ResolvedSelections(
            country_codes=["GBR"],
            indicator_codes=["NGDPD"],
            unit_codes=["USD"],
            scale_codes=["9"],
            country_labels={"GBR": "United Kingdom"},
            subject_labels={"NGDPD": "GDP current US dollar"},
            unit_labels={"USD": "US dollar"},
            scale_labels={"9": "Billions"},
        ),
    )

    def fake_resolve_time_range(
        runtime_settings: RuntimeSettings,
        active_client: DummyClient,
        country_codes: list[str],
        indicator_codes: list[str],
        frequency: str,
    ) -> app.ResolvedTimeRange:
        del active_client, country_codes, indicator_codes, frequency
        captured_date_inputs.append((runtime_settings.start_year, runtime_settings.end_year))
        return app.ResolvedTimeRange(
            start_year=2022,
            end_year=2024,
            start_period=parse_time_period("2022", "A"),
            end_period=parse_time_period("2024", "A"),
        )

    monkeypatch.setattr(app, "_resolve_time_range", fake_resolve_time_range)
    monkeypatch.setattr(
        client,
        "fetch_dataframe",
        lambda **kwargs: app.pd.DataFrame(
            [{"country": "GBR", "subject_descriptor": "GDP", "units": "USD", "scale": "Billions", "time_period": 2022, "obs_value": 1.0}]
        ),
        raising=False,
    )

    dataframe = app._run_dataframe(settings, client)

    assert captured_frequency_inputs == [""]
    assert captured_date_inputs == [(None, None)]
    assert settings.frequency == "Q"
    assert settings.start_year == 2022
    assert settings.end_year == 2024
    assert not dataframe.empty


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


def test_resolve_selections_indicator_first_allows_aggregate_location_only_series(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    client = DummyClient(
        country_availability_by_indicator={"NGDPD": ["G001"]},
        available_indicator_catalog_codes=["NGDPD"],
    )

    monkeypatch.setattr(app, "prompt_for_choice", lambda title, choices: app.INDICATOR_FIRST)

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del required
        if title == "Select subject descriptors":
            assert [choice["value"] for choice in choices] == ["NGDPD"]
            return ["NGDPD"]
        if title == "Select locations":
            assert [choice["value"] for choice in choices] == ["G001"]
            return ["G001"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = _resolve(settings, client)

    assert selections.country_codes == ["G001"]
    assert selections.country_labels == {"G001": "World"}
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
    assert selections.unit_codes == []
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
    assert selections.unit_codes == []


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
    assert selections.unit_codes == []


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
    prompts: list[str] = []

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del choices, required
        prompts.append(title)
        return ["0"]

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)

    filters = app._resolve_unit_scale_filters(
        RuntimeSettings(interactive=True),
        country_codes=["GBR"],
        indicator_codes=["LP"],
        catalog=_catalog(),
        aliases=_aliases(),
        client=DummyClient(series_variants_by_indicator={"LP": [SeriesVariant("MIL", "0"), SeriesVariant("MIL", "9")]}),
        frequency="A",
    )

    assert prompts == ["Select scales for Population"]
    assert filters.indicator_codes == ["LP"]
    assert filters.selected_scale_codes == ["0"]


def test_resolve_unit_scale_filters_prompts_only_for_true_unit_ambiguity(monkeypatch) -> None:
    prompts: list[str] = []

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del choices, required
        prompts.append(title)
        return ["MIL"]

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)

    filters = app._resolve_unit_scale_filters(
        RuntimeSettings(interactive=True),
        country_codes=["GBR"],
        indicator_codes=["LP"],
        catalog=_catalog(),
        aliases=_aliases(),
        client=DummyClient(series_variants_by_indicator={"LP": [SeriesVariant("MIL", "0"), SeriesVariant("PE", "0")]}),
        frequency="A",
    )

    assert prompts == ["Select units for Population"]
    assert filters.indicator_codes == ["LP"]
    assert filters.selected_unit_codes == ["MIL"]
