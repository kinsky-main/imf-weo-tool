from __future__ import annotations

import weo_tools.app as app
from weo_tools.configuration import RuntimeSettings
from weo_tools.imf import AvailabilityAggregate, AvailabilityResult, Catalog, ReleaseInfo
from weo_tools.legacy import AliasConfig, LegacyCatalog


class DummyClient:
    def __init__(
        self,
        *,
        indicator_availability_by_country: dict[str, list[str]] | None = None,
        country_availability_by_indicator: dict[str, list[str]] | None = None,
        available_location_codes: list[str] | None = None,
        available_indicator_catalog_codes: list[str] | None = None,
    ) -> None:
        self.indicator_availability_by_country = indicator_availability_by_country or {}
        self.country_availability_by_indicator = country_availability_by_indicator or {}
        self.available_location_codes = list(available_location_codes or [])
        self.available_indicator_catalog_codes = list(available_indicator_catalog_codes or [])

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
        "G120": "G20",
    }
    return Catalog(
        release=ReleaseInfo(version="9.0.0", updated_at="2026-03-12", name="WEO"),
        countries={"AUT": locations["AUT"], "GBR": locations["GBR"]},
        country_groups={"G001": locations["G001"], "G120": locations["G120"]},
        locations=locations,
        indicators={"NGDP": "GDP current national currency", "NGDPD": "GDP current US dollar"},
        units={"USD": "US dollar", "XDC": "Domestic currency"},
        scales={"9": "Billions"},
    )


def _legacy() -> LegacyCatalog:
    return LegacyCatalog(
        country_aliases={"AUT": {"Austria"}, "GBR": {"United Kingdom"}},
        subject_aliases={
            "NGDP": {"Gross domestic product, current prices"},
            "NGDPD": {"Gross domestic product, current prices"},
        },
        preferred_country_labels={"AUT": "Austria", "GBR": "United Kingdom"},
        preferred_subject_labels={
            "NGDP": "Gross domestic product, current prices",
            "NGDPD": "Gross domestic product, current prices",
        },
        indicator_units={"NGDP": {"National currency"}, "NGDPD": {"U.S. dollars"}},
        indicator_scales={"NGDP": {"Billions"}, "NGDPD": {"Billions"}},
        preferred_unit_labels={"NGDP": "National currency", "NGDPD": "U.S. dollars"},
        preferred_scale_labels={"NGDP": "Billions", "NGDPD": "Billions"},
    )


def _aliases() -> AliasConfig:
    return AliasConfig(
        units={"national currency": "XDC", "u s dollars": "USD"},
        scales={"billions": "9"},
        unit_display={"USD": "U.S. dollars", "XDC": "National currency"},
        scale_display={"9": "Billions"},
    )


def test_selection_order_for_settings_prefers_existing_inputs() -> None:
    assert app._selection_order_for_settings(RuntimeSettings(countries=["Austria"])) == app.COUNTRY_FIRST
    assert app._selection_order_for_settings(RuntimeSettings(subject_descriptors=["NGDPD"])) == app.INDICATOR_FIRST
    assert app._selection_order_for_settings(RuntimeSettings(interactive=True)) is None


def test_resolve_selections_country_first_filters_zero_series_and_prompts_groups(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    client = DummyClient(
        indicator_availability_by_country={
            "GBR": ["NGDP", "NGDPD"],
            "G001": ["NGDPD"],
        },
        available_location_codes=["GBR", "G001"],
    )

    monkeypatch.setattr(app, "prompt_for_choice", lambda title, choices: app.COUNTRY_FIRST)

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del required
        if title == "Select countries":
            assert [choice["value"] for choice in choices] == ["GBR"]
            assert {choice["value"]: choice.get("detail", "") for choice in choices} == {"GBR": "2 indicators"}
            return ["GBR"]
        if title == "Select country groups":
            assert [choice["value"] for choice in choices] == ["G001"]
            assert {choice["value"]: choice.get("detail", "") for choice in choices} == {"G001": "1 indicator"}
            return ["G001"]
        if title == "Select subject descriptors":
            assert {choice["value"]: choice.get("detail", "") for choice in choices} == {
                "NGDP": "1/2 locations",
                "NGDPD": "2/2 locations",
            }
            return ["NGDPD"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), client)

    assert selections.country_codes == ["GBR", "G001"]
    assert selections.indicator_codes == ["NGDPD"]


def test_resolve_selections_indicator_first_filters_zero_series_and_prompts_groups(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    client = DummyClient(
        country_availability_by_indicator={
            "NGDPD": ["AUT", "G001"],
        },
        available_indicator_catalog_codes=["NGDPD"],
    )

    monkeypatch.setattr(app, "prompt_for_choice", lambda title, choices: app.INDICATOR_FIRST)

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del required
        if title == "Select subject descriptors":
            assert [choice["value"] for choice in choices] == ["NGDPD"]
            assert {choice["value"]: choice.get("detail", "") for choice in choices} == {"NGDPD": "2 locations"}
            return ["NGDPD"]
        if title == "Select countries":
            assert [choice["value"] for choice in choices] == ["AUT"]
            assert {choice["value"]: choice.get("detail", "") for choice in choices} == {"AUT": "1/1 subject"}
            return ["AUT"]
        if title == "Select country groups":
            assert [choice["value"] for choice in choices] == ["G001"]
            assert {choice["value"]: choice.get("detail", "") for choice in choices} == {"G001": "1/1 subject"}
            return ["G001"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), client)

    assert selections.country_codes == ["AUT", "G001"]
    assert selections.indicator_codes == ["NGDPD"]


def test_resolve_selections_accepts_explicit_group_codes_non_interactively(monkeypatch) -> None:
    settings = RuntimeSettings(countries=["G001"], subject_descriptors=["NGDPD"])
    client = DummyClient(indicator_availability_by_country={"G001": ["NGDPD"]})

    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), client)

    assert selections.country_codes == ["G001"]
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

    selections = app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), client)

    assert selections.indicator_codes == ["NGDPD"]
    assert selections.unit_codes == ["USD"]
    assert selections.scale_codes == ["9"]


def test_resolve_selections_prompts_when_multiple_units_exist(monkeypatch) -> None:
    settings = RuntimeSettings(countries=["GBR"], subject_descriptors=["NGDP", "NGDPD"], interactive=True)
    client = DummyClient(indicator_availability_by_country={"GBR": ["NGDP", "NGDPD"]})

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        del choices, required
        if title == "Select units":
            return ["U.S. dollars"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), client)

    assert selections.indicator_codes == ["NGDPD"]
    assert selections.unit_codes == ["USD"]
