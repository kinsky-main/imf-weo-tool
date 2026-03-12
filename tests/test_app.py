from __future__ import annotations

import weo_tools.app as app
from weo_tools.configuration import RuntimeSettings
from weo_tools.imf import Catalog, ReleaseInfo
from weo_tools.legacy import AliasConfig, LegacyCatalog


class DummyClient:
    def __init__(self, *, available_indicator_codes: list[str], available_country_codes: list[str]) -> None:
        self.available_indicator_codes = available_indicator_codes
        self.available_country_codes = available_country_codes

    def fetch_available_indicator_codes(self, country_codes: list[str], frequency: str) -> list[str]:
        return list(self.available_indicator_codes)

    def fetch_available_country_codes(self, indicator_codes: list[str], frequency: str) -> list[str]:
        return list(self.available_country_codes)


def _catalog() -> Catalog:
    return Catalog(
        release=ReleaseInfo(version="9.0.0", updated_at="2026-03-12", name="WEO"),
        countries={"AUT": "Austria", "GBR": "United Kingdom"},
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


def test_resolve_selections_country_first_prompts_subjects(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    client = DummyClient(available_indicator_codes=["NGDPD"], available_country_codes=["GBR"])

    monkeypatch.setattr(app, "prompt_for_choice", lambda title, choices: app.COUNTRY_FIRST)

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        if title == "Select countries":
            return ["GBR"]
        if title == "Select subject descriptors":
            return ["NGDPD"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), client)

    assert selections.country_codes == ["GBR"]
    assert selections.indicator_codes == ["NGDPD"]


def test_resolve_selections_indicator_first_narrows_countries(monkeypatch) -> None:
    settings = RuntimeSettings(interactive=True)
    client = DummyClient(available_indicator_codes=["NGDPD"], available_country_codes=["GBR"])

    monkeypatch.setattr(app, "prompt_for_choice", lambda title, choices: app.INDICATOR_FIRST)

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        if title == "Select subject descriptors":
            return ["NGDPD"]
        if title == "Select countries":
            assert [choice["value"] for choice in choices] == ["GBR"]
            return ["GBR"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), client)

    assert selections.country_codes == ["GBR"]
    assert selections.indicator_codes == ["NGDPD"]


def test_resolve_selections_auto_skips_single_unit_and_scale(monkeypatch) -> None:
    settings = RuntimeSettings(countries=["GBR"], subject_descriptors=["NGDPD"], interactive=True)
    client = DummyClient(available_indicator_codes=["NGDPD"], available_country_codes=["GBR"])

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
    client = DummyClient(available_indicator_codes=["NGDP", "NGDPD"], available_country_codes=["GBR"])

    def fake_prompt_for_choices(title: str, choices: list[dict[str, str]], required: bool = True) -> list[str]:
        if title == "Select units":
            return ["U.S. dollars"]
        raise AssertionError(title)

    monkeypatch.setattr(app, "prompt_for_choices", fake_prompt_for_choices)
    monkeypatch.setattr(app, "run_with_status", lambda message, func, /, *args, **kwargs: func(*args, **kwargs))

    selections = app._resolve_selections(settings, _catalog(), _legacy(), _aliases(), client)

    assert selections.indicator_codes == ["NGDPD"]
    assert selections.unit_codes == ["USD"]
