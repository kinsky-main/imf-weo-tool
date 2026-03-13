from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .configuration import RuntimeSettings
from .imf import AvailabilityAggregate, AvailabilityResult, Catalog, ImfWeoClient
from .legacy import AliasConfig, LegacyCatalog, load_alias_config, load_legacy_catalog, normalize_label
from .tui import prompt_for_choice, prompt_for_choices, run_with_status


COUNTRY_FIRST = "country-first"
INDICATOR_FIRST = "indicator-first"


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


@dataclass(slots=True)
class CoreSelectionState:
    country_codes: list[str]
    indicator_codes: list[str]


@dataclass(slots=True)
class FilterSelectionState:
    indicator_codes: list[str]
    selected_units: list[str]
    selected_scales: list[str]


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
    catalog = run_with_status("Loading IMF WEO catalog...", active_client.fetch_catalog)
    aliases = load_alias_config(settings.alias_file)
    legacy = load_legacy_catalog(settings.compatibility_workbook)
    selections = _resolve_selections(settings, catalog, legacy, aliases, active_client)

    dataframe = run_with_status(
        "Fetching WEO data...",
        active_client.fetch_dataframe,
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
    wide = _normalize_excel_numeric_frame(pivot_for_excel(dataframe))
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
        _normalize_excel_numeric_frame(dataframe).to_excel(path, index=False)
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
    core_state = _resolve_primary_selection_state(settings, catalog, legacy, aliases, client)
    filter_state = _resolve_unit_scale_filters(settings, core_state.indicator_codes, legacy)
    return _build_resolved_selections(
        country_codes=core_state.country_codes,
        indicator_codes=filter_state.indicator_codes,
        selected_scales=filter_state.selected_scales,
        catalog=catalog,
        legacy=legacy,
        aliases=aliases,
    )


def _resolve_primary_selection_state(
    settings: RuntimeSettings,
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
    client: ImfWeoClient,
) -> CoreSelectionState:
    selection_order = _selection_order_for_settings(settings)
    if selection_order is None:
        selection_order = _prompt_for_selection_order()

    selected_countries = list(settings.countries)
    selected_subjects = list(settings.subject_descriptors)
    if selection_order == INDICATOR_FIRST:
        return _resolve_indicator_first_selection(
            settings,
            catalog,
            legacy,
            aliases,
            client,
            selected_countries=selected_countries,
            selected_subjects=selected_subjects,
        )
    return _resolve_country_first_selection(
        settings,
        catalog,
        legacy,
        aliases,
        client,
        selected_countries=selected_countries,
        selected_subjects=selected_subjects,
    )


def _selection_order_for_settings(settings: RuntimeSettings) -> str | None:
    if settings.countries:
        return COUNTRY_FIRST
    if settings.subject_descriptors:
        return INDICATOR_FIRST
    if settings.interactive:
        return None
    return COUNTRY_FIRST


def _prompt_for_selection_order() -> str:
    return prompt_for_choice(
        "Choose selection order",
        [
            {"name": "Country first", "value": COUNTRY_FIRST},
            {"name": "Indicator first", "value": INDICATOR_FIRST},
        ],
    )


def _resolve_country_first_selection(
    settings: RuntimeSettings,
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
    client: ImfWeoClient,
    *,
    selected_countries: list[str],
    selected_subjects: list[str],
) -> CoreSelectionState:
    if settings.interactive and not selected_countries:
        available_location_codes = _fetch_available_location_codes(client, settings.frequency)
        available_country_codes, available_group_codes = _split_available_location_codes(
            catalog,
            available_location_codes,
        )
        country_totals = _fetch_indicator_availability(
            client,
            available_country_codes,
            settings.frequency,
            strict=False,
            status_message="Checking country availability...",
        )
        selected_country_codes = _prompt_for_country_codes(
            "Select countries",
            catalog,
            legacy,
            available_country_codes=available_country_codes,
            detail_by_code=_build_total_count_details(country_totals.results, "indicator"),
            required=False,
        )
        group_totals = _fetch_indicator_availability(
            client,
            available_group_codes,
            settings.frequency,
            strict=False,
            status_message="Checking country group availability...",
        )
        selected_group_codes = _prompt_for_country_group_codes(
            "Select country groups",
            catalog,
            legacy,
            available_group_codes=available_group_codes,
            detail_by_code=_build_total_count_details(group_totals.results, "indicator"),
            required=False,
        )
        selected_countries = selected_country_codes + selected_group_codes
    if not selected_countries:
        raise ValueError("At least one country or country group selector is required.")

    country_codes = _resolve_country_codes(selected_countries, catalog, legacy, aliases)
    indicator_availability = _fetch_indicator_availability(client, country_codes, settings.frequency)
    if not indicator_availability.available_codes:
        raise ValueError("No indicators are available for the selected countries or country groups and frequency.")

    if settings.interactive and not selected_subjects:
        selected_subjects = _prompt_for_indicator_codes(
            "Select subject descriptors",
            indicator_availability.available_codes,
            catalog,
            legacy,
            detail_by_code=_build_ratio_count_details(
                indicator_availability.counts_by_code,
                len(country_codes),
                "location",
            ),
        )
    if not selected_subjects:
        raise ValueError("At least one subject descriptor selector is required.")

    indicator_codes = _resolve_indicator_codes(selected_subjects, catalog, legacy, aliases)
    indicator_codes = _restrict_codes(indicator_codes, indicator_availability.available_codes)
    if not indicator_codes:
        raise ValueError("No matching indicators are available for the selected countries or country groups.")

    return CoreSelectionState(country_codes=country_codes, indicator_codes=indicator_codes)


def _resolve_indicator_first_selection(
    settings: RuntimeSettings,
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
    client: ImfWeoClient,
    *,
    selected_countries: list[str],
    selected_subjects: list[str],
) -> CoreSelectionState:
    if settings.interactive and not selected_subjects:
        available_indicator_codes = _fetch_available_indicator_catalog_codes(client, settings.frequency)
        indicator_totals = _fetch_country_availability(
            client,
            available_indicator_codes,
            settings.frequency,
            strict=False,
            status_message="Checking subject availability...",
        )
        selected_subjects = _prompt_for_indicator_codes(
            "Select subject descriptors",
            available_indicator_codes,
            catalog,
            legacy,
            detail_by_code=_build_total_count_details(indicator_totals.results, "location"),
        )
    if not selected_subjects:
        raise ValueError("At least one subject descriptor selector is required.")

    indicator_codes = _resolve_indicator_codes(selected_subjects, catalog, legacy, aliases)
    country_availability = _fetch_country_availability(client, indicator_codes, settings.frequency)
    if not country_availability.available_codes:
        raise ValueError("No countries or country groups are available for the selected subject descriptors and frequency.")

    if settings.interactive and not selected_countries:
        available_country_codes, available_group_codes = _split_available_location_codes(
            catalog,
            country_availability.available_codes,
        )
        selected_country_codes = _prompt_for_country_codes(
            "Select countries",
            catalog,
            legacy,
            available_country_codes=available_country_codes,
            detail_by_code=_build_ratio_count_details(
                _filter_count_map(country_availability.counts_by_code, available_country_codes),
                len(indicator_codes),
                "subject",
            ),
            required=False,
        )
        selected_group_codes = _prompt_for_country_group_codes(
            "Select country groups",
            catalog,
            legacy,
            available_group_codes=available_group_codes,
            detail_by_code=_build_ratio_count_details(
                _filter_count_map(country_availability.counts_by_code, available_group_codes),
                len(indicator_codes),
                "subject",
            ),
            required=False,
        )
        selected_countries = selected_country_codes + selected_group_codes
    if not selected_countries:
        raise ValueError("At least one country or country group selector is required.")

    country_codes = _resolve_country_codes(selected_countries, catalog, legacy, aliases)
    country_codes = _restrict_codes(country_codes, country_availability.available_codes)
    if not country_codes:
        raise ValueError("No matching countries or country groups are available for the selected subject descriptors.")

    return CoreSelectionState(country_codes=country_codes, indicator_codes=indicator_codes)


def _resolve_unit_scale_filters(
    settings: RuntimeSettings,
    indicator_codes: list[str],
    legacy: LegacyCatalog,
) -> FilterSelectionState:
    selected_units = list(settings.units)
    selected_scales = list(settings.scales)

    if settings.interactive and not selected_units:
        available_units = _available_legacy_labels(
            _filter_indicator_codes(indicator_codes, [], selected_scales, legacy),
            legacy.indicator_units,
        )
        selected_units = _resolve_optional_labels("Select units", available_units)

    filtered_indicator_codes = _filter_indicator_codes(indicator_codes, selected_units, selected_scales, legacy)
    if not filtered_indicator_codes:
        raise ValueError("No WEO series match the selected Subject Descriptor, Units, and Scale combination.")

    if settings.interactive and not selected_scales:
        available_scales = _available_legacy_labels(
            _filter_indicator_codes(indicator_codes, selected_units, [], legacy),
            legacy.indicator_scales,
        )
        selected_scales = _resolve_optional_labels("Select scales", available_scales)

    filtered_indicator_codes = _filter_indicator_codes(indicator_codes, selected_units, selected_scales, legacy)
    if not filtered_indicator_codes:
        raise ValueError("No WEO series match the selected Subject Descriptor, Units, and Scale combination.")

    return FilterSelectionState(
        indicator_codes=filtered_indicator_codes,
        selected_units=selected_units,
        selected_scales=selected_scales,
    )


def _build_resolved_selections(
    *,
    country_codes: list[str],
    indicator_codes: list[str],
    selected_scales: list[str],
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
) -> ResolvedSelections:
    indicator_unit_labels = {
        code: legacy.preferred_unit_labels.get(code, next(iter(legacy.indicator_units.get(code, {""})), ""))
        for code in indicator_codes
    }
    indicator_scale_labels = {
        code: legacy.preferred_scale_labels.get(code, next(iter(legacy.indicator_scales.get(code, {""})), ""))
        for code in indicator_codes
    }
    indicator_unit_codes = {
        code: _resolve_optional_code_from_label(indicator_unit_labels[code], catalog.units, aliases.units)
        for code in indicator_codes
    }
    scale_codes = _resolve_contextual_codes(
        requested=selected_scales,
        available_codes=list(catalog.scales.keys()),
        current_labels=catalog.scales,
        display_overrides=aliases.scale_display,
        manual_aliases=aliases.scales,
        entity_name="scale",
    )
    country_labels = {code: legacy.preferred_country_labels.get(code, catalog.locations[code]) for code in country_codes}
    subject_labels = {
        code: legacy.preferred_subject_labels.get(code, catalog.indicators[code]) for code in indicator_codes
    }
    unit_labels = {
        code: aliases.unit_display.get(code, catalog.units.get(code, code))
        for code in indicator_unit_codes.values()
        if code
    }
    scale_labels = {
        code: aliases.scale_display.get(code, catalog.scales.get(code, code)) for code in catalog.scales
    }
    return ResolvedSelections(
        country_codes=country_codes,
        indicator_codes=indicator_codes,
        unit_codes=[code for code in indicator_unit_codes.values() if code],
        scale_codes=scale_codes,
        country_labels=country_labels,
        subject_labels=subject_labels,
        unit_labels=unit_labels,
        scale_labels=scale_labels,
        indicator_unit_labels=indicator_unit_labels,
        indicator_unit_codes=indicator_unit_codes,
        indicator_scale_labels=indicator_scale_labels,
    )


def _fetch_indicator_availability(
    client: ImfWeoClient,
    country_codes: list[str],
    frequency: str,
    *,
    strict: bool = True,
    status_message: str = "Checking available indicators...",
) -> AvailabilityAggregate:
    return run_with_status(
        status_message,
        client.fetch_indicator_availability,
        values=country_codes,
        frequency=frequency,
        strict=strict,
    )


def _fetch_country_availability(
    client: ImfWeoClient,
    indicator_codes: list[str],
    frequency: str,
    *,
    strict: bool = True,
    status_message: str = "Checking available countries...",
) -> AvailabilityAggregate:
    return run_with_status(
        status_message,
        client.fetch_country_availability,
        values=indicator_codes,
        frequency=frequency,
        strict=strict,
    )


def _fetch_available_location_codes(client: ImfWeoClient, frequency: str) -> list[str]:
    return run_with_status(
        "Checking available locations...",
        client.fetch_available_location_codes,
        frequency=frequency,
    )


def _fetch_available_indicator_catalog_codes(client: ImfWeoClient, frequency: str) -> list[str]:
    return run_with_status(
        "Checking available subjects...",
        client.fetch_available_indicator_catalog_codes,
        frequency=frequency,
    )


def _resolve_country_codes(
    requested: list[str],
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
) -> list[str]:
    return _resolve_codes(
        requested=requested,
        current_labels=catalog.locations,
        preferred_labels=legacy.preferred_country_labels,
        workbook_aliases=legacy.country_aliases,
        manual_aliases=aliases.countries,
        entity_name="country",
    )


def _resolve_indicator_codes(
    requested: list[str],
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
) -> list[str]:
    return _resolve_codes(
        requested=requested,
        current_labels=catalog.indicators,
        preferred_labels=legacy.preferred_subject_labels,
        workbook_aliases=legacy.subject_aliases,
        manual_aliases=aliases.subjects,
        entity_name="subject descriptor",
        allow_multiple_matches=True,
    )


def _prompt_for_country_codes(
    prompt: str,
    catalog: Catalog,
    legacy: LegacyCatalog,
    available_country_codes: list[str] | None = None,
    detail_by_code: dict[str, str] | None = None,
    required: bool = True,
) -> list[str]:
    codes = available_country_codes or list(catalog.countries.keys())
    return _prompt_for_codes(
        prompt,
        _build_choice_map_for_codes(
            codes,
            catalog.countries,
            legacy.preferred_country_labels,
            detail_by_code=detail_by_code,
        ),
        required=required,
    )


def _prompt_for_country_group_codes(
    prompt: str,
    catalog: Catalog,
    legacy: LegacyCatalog,
    available_group_codes: list[str] | None = None,
    detail_by_code: dict[str, str] | None = None,
    required: bool = True,
) -> list[str]:
    codes = available_group_codes or list(catalog.country_groups.keys())
    return _prompt_for_codes(
        prompt,
        _build_choice_map_for_codes(
            codes,
            catalog.country_groups,
            legacy.preferred_country_labels,
            detail_by_code=detail_by_code,
        ),
        required=required,
    )


def _split_available_location_codes(catalog: Catalog, available_location_codes: list[str]) -> tuple[list[str], list[str]]:
    available = set(available_location_codes)
    return (
        [code for code in catalog.countries if code in available],
        [code for code in catalog.country_groups if code in available],
    )


def _prompt_for_indicator_codes(
    prompt: str,
    available_indicator_codes: list[str],
    catalog: Catalog,
    legacy: LegacyCatalog,
    detail_by_code: dict[str, str] | None = None,
) -> list[str]:
    preferred_labels = {
        code: legacy.preferred_subject_labels.get(code, catalog.indicators[code]) for code in available_indicator_codes
    }
    return _prompt_for_codes(
        prompt,
        _build_choice_map_for_codes(
            available_indicator_codes,
            catalog.indicators,
            preferred_labels,
            detail_by_code=detail_by_code,
        ),
    )


def _restrict_codes(resolved_codes: list[str], available_codes: list[str]) -> list[str]:
    available = set(available_codes)
    return [code for code in resolved_codes if code in available]


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
        normalized_map.setdefault(
            normalize_label(display_overrides.get(code, current_labels.get(code, code))),
            set(),
        ).add(code)
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


def _resolve_optional_code_from_label(
    label: str,
    current_labels: dict[str, str],
    manual_aliases: dict[str, str],
) -> str:
    if not label:
        return ""
    normalized = normalize_label(label)
    for code, current_label in current_labels.items():
        if normalize_label(code) == normalized or normalize_label(current_label) == normalized:
            return code
    return manual_aliases.get(normalized, "")


def _build_choice_map_for_codes(
    codes: list[str],
    current_labels: dict[str, str],
    preferred_labels: dict[str, str],
    preselected: list[str] | None = None,
    detail_by_code: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    subset = {code: current_labels[code] for code in codes}
    return _build_choice_map(
        subset,
        preferred_labels,
        preselected=preselected,
        detail_by_code=detail_by_code,
    )


def _build_choice_map(
    current_labels: dict[str, str],
    preferred_labels: dict[str, str],
    preselected: list[str] | None = None,
    detail_by_code: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    choices: list[dict[str, Any]] = []
    for code, current_label in sorted(current_labels.items(), key=lambda item: preferred_labels.get(item[0], item[1])):
        preferred = preferred_labels.get(code, current_label)
        title = f"{preferred} [{code}]"
        if preferred != current_label:
            title = f"{title} ({current_label})"
        choice: dict[str, Any] = {"name": title, "value": code}
        if detail_by_code and code in detail_by_code:
            choice["detail"] = detail_by_code[code]
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


def _resolve_optional_labels(prompt: str, labels: list[str]) -> list[str]:
    if not labels:
        return []
    if len(labels) == 1:
        return list(labels)
    return _prompt_for_labels(
        prompt,
        _build_label_choices(labels, preselect_all=True),
        required=False,
    )


def _available_legacy_labels(indicator_codes: list[str], label_map: dict[str, set[str]]) -> list[str]:
    labels = {
        label
        for code in indicator_codes
        for label in label_map.get(code, set())
        if label
    }
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
    if not choices:
        if required:
            raise ValueError(f"No choices are available for {prompt.lower()}.")
        return []
    result = prompt_for_choices(prompt, choices, required=required)
    if required and not result:
        raise ValueError(f"{prompt} requires at least one selection.")
    return result


def _prompt_for_labels(prompt: str, choices: list[dict[str, Any]], required: bool = True) -> list[str]:
    return _prompt_for_codes(prompt, choices, required=required)


def _build_total_count_details(results: list[AvailabilityResult], counterpart_name: str) -> dict[str, str]:
    return {
        result.requested_code: f"{result.series_count} {_pluralize(counterpart_name, result.series_count)}"
        for result in results
    }


def _build_ratio_count_details(
    counts_by_code: dict[str, int],
    denominator: int,
    counterpart_name: str,
) -> dict[str, str]:
    plural = _pluralize(counterpart_name, denominator)
    return {
        code: f"{count}/{denominator} {plural}"
        for code, count in counts_by_code.items()
    }


def _filter_count_map(counts_by_code: dict[str, int], codes: list[str]) -> dict[str, int]:
    allowed = set(codes)
    return {code: count for code, count in counts_by_code.items() if code in allowed}


def _pluralize(noun: str, count: int) -> str:
    if noun.endswith("y") and not noun.endswith(("ay", "ey", "iy", "oy", "uy")):
        return noun if count == 1 else f"{noun[:-1]}ies"
    return noun if count == 1 else f"{noun}s"


def _normalize_excel_numeric_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    for column in normalized.columns:
        if _should_normalize_excel_column(column, normalized[column]):
            normalized[column] = _coerce_excel_numeric_series(normalized[column])
    return normalized


def _should_normalize_excel_column(column: Any, series: pd.Series) -> bool:
    column_name = str(column)
    if column_name in {"time_period", "obs_value"} or column_name.isdigit():
        return True
    if not pd.api.types.is_object_dtype(series.dtype) and not pd.api.types.is_string_dtype(series.dtype):
        return False
    non_empty = series.replace("", pd.NA).dropna()
    if non_empty.empty:
        return False
    converted = pd.to_numeric(non_empty, errors="coerce")
    return bool(converted.notna().all())


def _coerce_excel_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = series.replace("", pd.NA)
    converted = pd.to_numeric(cleaned, errors="coerce")
    non_null = converted.dropna()
    if non_null.empty:
        return converted
    if ((non_null % 1) == 0).all():
        return converted.astype("Int64")
    return converted.astype("Float64")
