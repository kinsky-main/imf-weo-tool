from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .configuration import RuntimeSettings
from .imf import Catalog, ImfWeoClient
from .legacy import AliasConfig, LegacyCatalog, load_alias_config, load_legacy_catalog, normalize_label


try:
    import questionary
except ImportError:  # pragma: no cover
    questionary = None


@dataclass(slots=True)
class ResolvedSelections:
    country_codes: list[str]
    indicator_codes: list[str]
    unit_codes: list[str]
    scale_codes: list[str]
    country_labels: dict[str, str]
    subject_labels: dict[str, str]
    unit_labels: dict[str, str]
    scale_labels: dict[str, str]
    indicator_unit_labels: dict[str, str]
    indicator_unit_codes: dict[str, str]
    indicator_scale_labels: dict[str, str]


def load_weo_dataframe(
    *,
    countries: list[str],
    subject_descriptors: list[str],
    units: list[str] | None = None,
    scales: list[str] | None = None,
    frequency: str = "A",
    start_year: int | None = None,
    end_year: int | None = None,
    compatibility_workbook: str = "data/weoapr2025all.xlsx",
    alias_file: str = "config/weo_aliases.toml",
    interactive: bool = False,
) -> pd.DataFrame:
    settings = RuntimeSettings(
        countries=countries,
        subject_descriptors=subject_descriptors,
        units=list(units or []),
        scales=list(scales or []),
        frequency=frequency,
        start_year=start_year,
        end_year=end_year,
        compatibility_workbook=compatibility_workbook,
        alias_file=alias_file,
        interactive=interactive,
    )
    client = ImfWeoClient()
    return run_dataframe(settings, client)


def run_dataframe(settings: RuntimeSettings, client: ImfWeoClient | None = None) -> pd.DataFrame:
    active_client = client or ImfWeoClient()
    catalog = active_client.fetch_catalog()
    aliases = load_alias_config(settings.alias_file)
    legacy = load_legacy_catalog(settings.compatibility_workbook)
    selections = _resolve_selections(settings, catalog, legacy, aliases, active_client)

    # A single WEO series splice is COUNTRY.INDICATOR.FREQUENCY, for example
    # GBR.NGDPD.A for UK annual current-price GDP. UNIT and SCALE are filters.
    dataframe = active_client.fetch_dataframe(
        country_codes=selections.country_codes,
        indicator_codes=selections.indicator_codes,
        unit_codes=selections.unit_codes,
        scale_codes=selections.scale_codes,
        frequency=settings.frequency,
        start_year=settings.start_year,
        end_year=settings.end_year,
        subject_labels=selections.subject_labels,
        country_labels=selections.country_labels,
        unit_labels=selections.unit_labels,
        scale_labels=selections.scale_labels,
        indicator_unit_labels=selections.indicator_unit_labels,
        indicator_unit_codes=selections.indicator_unit_codes,
        indicator_scale_labels=selections.indicator_scale_labels,
    )
    return enrich_for_legacy_columns(dataframe)


def run_excel_export(settings: RuntimeSettings, client: ImfWeoClient | None = None) -> Path:
    dataframe = run_dataframe(settings, client)
    output_path = Path(settings.output_path or "output/weo_export.xlsx")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wide = pivot_for_excel(dataframe)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        wide.to_excel(writer, sheet_name="WEO Data", index=False)
    return output_path


def save_dataframe(dataframe: pd.DataFrame, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        dataframe.to_csv(path, index=False)
    elif suffix == ".parquet":
        dataframe.to_parquet(path, index=False)
    elif suffix in {".pkl", ".pickle"}:
        dataframe.to_pickle(path)
    elif suffix == ".xlsx":
        dataframe.to_excel(path, index=False)
    else:
        raise ValueError(f"Unsupported dataframe output format: {path.suffix}")
    return path


def enrich_for_legacy_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    return dataframe.rename(
        columns={
            "country": "Country",
            "subject_descriptor": "Subject Descriptor",
            "units": "Units",
            "scale": "Scale",
        }
    )


def pivot_for_excel(dataframe: pd.DataFrame) -> pd.DataFrame:
    wide = (
        dataframe.pivot_table(
            index=["Country", "Subject Descriptor", "Units", "Scale"],
            columns="time_period",
            values="obs_value",
            aggfunc="first",
        )
        .reset_index()
        .sort_values(["Country", "Subject Descriptor", "Units", "Scale"], ignore_index=True)
    )
    wide.columns = [str(column) if isinstance(column, int) else column for column in wide.columns]
    return wide


def _resolve_selections(
    settings: RuntimeSettings,
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
    client: ImfWeoClient,
) -> ResolvedSelections:
    countries = list(settings.countries)
    subjects = list(settings.subject_descriptors)

    if settings.interactive:
        countries = countries or _prompt_for_codes(
            "Select countries",
            _build_choice_map(catalog.countries, legacy.preferred_country_labels),
        )
        subjects = subjects or _prompt_for_codes(
            "Select subject descriptors",
            _build_choice_map(catalog.indicators, legacy.preferred_subject_labels),
        )

    if not countries:
        raise ValueError("At least one country selector is required.")
    if not subjects:
        raise ValueError("At least one subject descriptor selector is required.")

    country_codes = _resolve_codes(
        requested=countries,
        current_labels=catalog.countries,
        preferred_labels=legacy.preferred_country_labels,
        workbook_aliases=legacy.country_aliases,
        manual_aliases=aliases.countries,
        entity_name="country",
    )
    indicator_codes = _resolve_codes(
        requested=subjects,
        current_labels=catalog.indicators,
        preferred_labels=legacy.preferred_subject_labels,
        workbook_aliases=legacy.subject_aliases,
        manual_aliases=aliases.subjects,
        entity_name="subject descriptor",
        allow_multiple_matches=True,
    )

    selected_units = list(settings.units)
    selected_scales = list(settings.scales)
    if settings.interactive and not selected_units:
        selected_units = _prompt_for_labels(
            "Select units",
            _build_label_choices(_available_legacy_labels(indicator_codes, legacy.indicator_units), preselect_all=True),
            required=False,
        )
    if settings.interactive and not selected_scales:
        selected_scales = _prompt_for_labels(
            "Select scales",
            _build_label_choices(_available_legacy_labels(indicator_codes, legacy.indicator_scales), preselect_all=True),
            required=False,
        )

    indicator_codes = _filter_indicator_codes(indicator_codes, selected_units, selected_scales, legacy)
    if not indicator_codes:
        raise ValueError("No WEO series match the selected Subject Descriptor, Units, and Scale combination.")

    scale_codes = _resolve_contextual_codes(
        requested=selected_scales,
        available_codes=list(catalog.scales.keys()),
        current_labels=catalog.scales,
        display_overrides=aliases.scale_display,
        manual_aliases=aliases.scales,
        entity_name="scale",
    )
    indicator_unit_codes = client.fetch_indicator_unit_codes(country_codes[0], indicator_codes, settings.frequency)
    unit_codes: list[str] = []

    country_labels = {
        code: legacy.preferred_country_labels.get(code, catalog.countries[code]) for code in country_codes
    }
    subject_labels = {
        code: legacy.preferred_subject_labels.get(code, catalog.indicators[code]) for code in indicator_codes
    }
    unit_labels = {
        code: aliases.unit_display.get(code, catalog.units.get(code, code))
        for code in indicator_unit_codes.values()
        if code
    }
    scale_labels = {code: aliases.scale_display.get(code, catalog.scales.get(code, code)) for code in catalog.scales}
    indicator_unit_labels = {
        code: legacy.preferred_unit_labels.get(code, next(iter(legacy.indicator_units.get(code, {""})), ""))
        for code in indicator_codes
    }
    indicator_scale_labels = {
        code: legacy.preferred_scale_labels.get(code, next(iter(legacy.indicator_scales.get(code, {""})), ""))
        for code in indicator_codes
    }

    return ResolvedSelections(
        country_codes=country_codes,
        indicator_codes=indicator_codes,
        unit_codes=unit_codes,
        scale_codes=scale_codes,
        country_labels=country_labels,
        subject_labels=subject_labels,
        unit_labels=unit_labels,
        scale_labels=scale_labels,
        indicator_unit_labels=indicator_unit_labels,
        indicator_unit_codes=indicator_unit_codes,
        indicator_scale_labels=indicator_scale_labels,
    )


def _resolve_codes(
    requested: list[str],
    current_labels: dict[str, str],
    preferred_labels: dict[str, str],
    workbook_aliases: dict[str, set[str]],
    manual_aliases: dict[str, str],
    entity_name: str,
    allow_multiple_matches: bool = False,
) -> list[str]:
    if not requested:
        return []

    normalized_map: dict[str, set[str]] = {}
    for code, label in current_labels.items():
        normalized_map.setdefault(normalize_label(code), set()).add(code)
        normalized_map.setdefault(normalize_label(label), set()).add(code)
    for code, label in preferred_labels.items():
        normalized_map.setdefault(normalize_label(label), set()).add(code)
    for code, aliases in workbook_aliases.items():
        for alias in aliases:
            normalized_map.setdefault(normalize_label(alias), set()).add(code)
    for alias, code in manual_aliases.items():
        normalized_map.setdefault(alias, set()).add(code)

    resolved: list[str] = []
    for item in requested:
        matches = normalized_map.get(normalize_label(item), set())
        if not matches:
            raise ValueError(f"Unknown {entity_name}: {item}")
        if len(matches) > 1 and not allow_multiple_matches:
            raise ValueError(f"Ambiguous {entity_name}: {item} -> {sorted(matches)}")
        match_codes = sorted(matches) if allow_multiple_matches else [next(iter(matches))]
        for code in match_codes:
            if code not in resolved:
                resolved.append(code)
    return resolved


def _resolve_contextual_codes(
    requested: list[str],
    available_codes: list[str],
    current_labels: dict[str, str],
    display_overrides: dict[str, str],
    manual_aliases: dict[str, str],
    entity_name: str,
) -> list[str]:
    if not requested:
        return []

    normalized_map: dict[str, set[str]] = {}
    for code in available_codes:
        normalized_map.setdefault(normalize_label(code), set()).add(code)
        normalized_map.setdefault(normalize_label(current_labels.get(code, code)), set()).add(code)
        normalized_map.setdefault(normalize_label(display_overrides.get(code, current_labels.get(code, code))), set()).add(code)
    for alias, code in manual_aliases.items():
        if code in available_codes:
            normalized_map.setdefault(alias, set()).add(code)

    resolved: list[str] = []
    for item in requested:
        matches = normalized_map.get(normalize_label(item), set())
        if not matches:
            raise ValueError(f"Unknown {entity_name}: {item}")
        if len(matches) > 1:
            raise ValueError(f"Ambiguous {entity_name}: {item} -> {sorted(matches)}")
        code = next(iter(matches))
        if code not in resolved:
            resolved.append(code)
    return resolved


def _build_choice_map(
    current_labels: dict[str, str],
    preferred_labels: dict[str, str],
    preselected: list[str] | None = None,
) -> list[dict[str, Any]]:
    choices: list[dict[str, Any]] = []
    for code, current_label in sorted(current_labels.items(), key=lambda item: preferred_labels.get(item[0], item[1])):
        preferred = preferred_labels.get(code, current_label)
        title = f"{preferred} [{code}]"
        if preferred != current_label:
            title = f"{title} ({current_label})"
        choice: dict[str, Any] = {"name": title, "value": code}
        if preselected and code in preselected:
            choice["checked"] = True
        choices.append(choice)
    return choices


def _build_label_choices(labels: list[str], preselect_all: bool = False) -> list[dict[str, Any]]:
    choices: list[dict[str, Any]] = []
    for label in labels:
        choice: dict[str, Any] = {"name": label, "value": label}
        if preselect_all:
            choice["checked"] = True
        choices.append(choice)
    return choices


def _available_legacy_labels(indicator_codes: list[str], label_map: dict[str, set[str]]) -> list[str]:
    labels = {label for code in indicator_codes for label in label_map.get(code, set())}
    return sorted(labels)


def _filter_indicator_codes(
    indicator_codes: list[str],
    selected_units: list[str],
    selected_scales: list[str],
    legacy: LegacyCatalog,
) -> list[str]:
    if not selected_units and not selected_scales:
        return indicator_codes

    normalized_units = {normalize_label(value) for value in selected_units}
    normalized_scales = {normalize_label(value) for value in selected_scales}
    filtered: list[str] = []
    for code in indicator_codes:
        legacy_units = {normalize_label(value) for value in legacy.indicator_units.get(code, set())}
        legacy_scales = {normalize_label(value) for value in legacy.indicator_scales.get(code, set())}
        unit_match = not normalized_units or bool(normalized_units & legacy_units)
        scale_match = not normalized_scales or bool(normalized_scales & legacy_scales)
        if unit_match and scale_match:
            filtered.append(code)
    return filtered


def _prompt_for_codes(prompt: str, choices: list[dict[str, Any]], required: bool = True) -> list[str]:
    if questionary is None:
        raise RuntimeError("Interactive mode requires questionary to be installed.")

    result = questionary.checkbox(prompt, choices=choices).ask()
    if required and not result:
        raise ValueError(f"{prompt} requires at least one selection.")
    return list(result or [])


def _prompt_for_labels(prompt: str, choices: list[dict[str, Any]], required: bool = True) -> list[str]:
    return _prompt_for_codes(prompt, choices, required=required)
