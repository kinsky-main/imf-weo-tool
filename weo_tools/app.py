from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .configuration import RuntimeSettings
from .imf import AvailabilityAggregate, AvailabilityResult, Catalog, ImfWeoClient
from .legacy import AliasConfig, LegacyCatalog, load_alias_config, load_legacy_catalog, normalize_label
from .regions import DEFAULT_REGION_MEMBERSHIP_PATH, RegionMembership, load_region_membership
from .tui import interactive_tui_session, prompt_for_choice, prompt_for_choices, run_with_status, set_interactive_summary


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
    if settings.interactive:
        with interactive_tui_session():
            return _run_dataframe(settings, client)
    return _run_dataframe(settings, client)


def _run_dataframe(settings: RuntimeSettings, client: ImfWeoClient | None = None) -> pd.DataFrame:
    active_client = client or ImfWeoClient()
    catalog = run_with_status("Loading IMF WEO catalog...", active_client.fetch_catalog)
    region_membership = _load_region_membership(catalog)
    frequency = _resolve_frequency_code(settings, catalog, active_client)
    settings.frequency = frequency
    _update_interactive_summary(settings)
    aliases = load_alias_config(settings.alias_file)
    legacy = load_legacy_catalog(settings.compatibility_workbook)
    selections = _resolve_selections(
        settings,
        catalog,
        legacy,
        aliases,
        region_membership,
        active_client,
        frequency,
    )
    _update_interactive_summary(settings, selections=selections)
    start_year, end_year = _resolve_date_range(
        settings,
        active_client,
        selections.country_codes,
        selections.indicator_codes,
        frequency,
    )
    settings.start_year = start_year
    settings.end_year = end_year
    _update_interactive_summary(settings, selections=selections)

    dataframe = run_with_status(
        "Fetching WEO data...",
        active_client.fetch_dataframe,
        country_codes=selections.country_codes,
        indicator_codes=selections.indicator_codes,
        unit_codes=selections.unit_codes,
        scale_codes=selections.scale_codes,
        frequency=frequency,
        start_year=start_year,
        end_year=end_year,
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
    output_path = _resolve_output_path(settings, dataframe, suffix=".xlsx")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wide = _normalize_excel_frame(pivot_for_excel(dataframe))
    _write_excel_frame(
        output_path,
        wide,
        sheet_name="WEO Data",
        header_frequency=settings.frequency,
        fixed_header_columns=4,
    )
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
        _write_excel_frame(path, _normalize_excel_frame(dataframe), sheet_name="Sheet1")
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
    region_membership: RegionMembership,
    client: ImfWeoClient,
    frequency: str,
) -> ResolvedSelections:
    core_state = _resolve_primary_selection_state(
        settings,
        catalog,
        legacy,
        aliases,
        region_membership,
        client,
        frequency,
    )
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
    region_membership: RegionMembership,
    client: ImfWeoClient,
    frequency: str,
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
            region_membership,
            client,
            frequency,
            selected_countries=selected_countries,
            selected_subjects=selected_subjects,
        )
    return _resolve_country_first_selection(
        settings,
        catalog,
        legacy,
        aliases,
        region_membership,
        client,
        frequency,
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


def _load_region_membership(catalog: Catalog) -> RegionMembership:
    return load_region_membership(
        DEFAULT_REGION_MEMBERSHIP_PATH,
        valid_region_codes=set(catalog.country_groups),
        valid_country_codes=set(catalog.countries),
    )


def _resolve_frequency_code(settings: RuntimeSettings, catalog: Catalog, client: ImfWeoClient) -> str:
    available_frequency_codes = _fetch_available_frequency_codes(client)
    if not available_frequency_codes:
        raise ValueError("No frequencies are available for IMF WEO.")

    requested_frequency = str(settings.frequency or "").strip().upper()
    if requested_frequency:
        if requested_frequency not in available_frequency_codes:
            available = ", ".join(available_frequency_codes)
            raise ValueError(
                f"Frequency '{requested_frequency}' is not available for IMF WEO. "
                f"Available frequencies: {available}."
            )
        return requested_frequency

    if settings.interactive:
        return _prompt_for_frequency(catalog, available_frequency_codes)
    return available_frequency_codes[0]


def _prompt_for_frequency(catalog: Catalog, available_frequency_codes: list[str]) -> str:
    if len(available_frequency_codes) == 1:
        return available_frequency_codes[0]
    return prompt_for_choice(
        "Select frequency",
        [
            {
                "name": f"{catalog.frequencies.get(code, code)} [{code}]",
                "value": code,
            }
            for code in available_frequency_codes
        ],
    )


def _update_interactive_summary(
    settings: RuntimeSettings,
    *,
    selections: ResolvedSelections | None = None,
) -> None:
    if not settings.interactive:
        return

    lines: list[str] = []
    if settings.frequency:
        lines.append(f"Frequency: {settings.frequency}")
    if selections is not None:
        lines.append(f"Countries: {len(selections.country_codes)} selected")
        lines.append(f"Subjects: {len(selections.indicator_codes)} selected")
        if settings.start_year is not None or settings.end_year is not None:
            start = settings.start_year if settings.start_year is not None else "min"
            end = settings.end_year if settings.end_year is not None else "max"
            lines.append(f"Date range: {start} to {end}")
    set_interactive_summary(lines)


def _resolve_date_range(
    settings: RuntimeSettings,
    client: ImfWeoClient,
    country_codes: list[str],
    indicator_codes: list[str],
    frequency: str,
) -> tuple[int | None, int | None]:
    start_year = settings.start_year
    end_year = settings.end_year

    if start_year is not None and end_year is not None:
        _validate_date_range(start_year, end_year)
        return start_year, end_year

    if not settings.interactive:
        if start_year is not None and end_year is not None:
            _validate_date_range(start_year, end_year)
        return start_year, end_year

    available_years = _fetch_available_time_periods(client, country_codes, indicator_codes, frequency)
    if not available_years:
        if start_year is not None and end_year is not None:
            _validate_date_range(start_year, end_year)
        return start_year, end_year

    if start_year is None and end_year is None and len(available_years) == 1:
        year = available_years[0]
        return year, year

    if start_year is None:
        start_choices = [year for year in available_years if end_year is None or year <= end_year]
        start_year = _prompt_for_year("Select start year", start_choices)

    if end_year is None:
        end_choices = [year for year in available_years if year >= start_year]
        end_year = _prompt_for_year("Select end year", end_choices)

    if start_year is not None and end_year is not None:
        _validate_date_range(start_year, end_year)
    return start_year, end_year


def _validate_date_range(start_year: int, end_year: int) -> None:
    if start_year > end_year:
        raise ValueError(f"Start year {start_year} cannot be later than end year {end_year}.")


def _prompt_for_year(title: str, years: list[int]) -> int:
    if not years:
        raise ValueError(f"No years are available for {title.lower()}.")
    if len(years) == 1:
        return years[0]
    return int(
        prompt_for_choice(
            title,
            [{"name": str(year), "value": str(year)} for year in years],
        )
    )


def _resolve_country_first_selection(
    settings: RuntimeSettings,
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
    region_membership: RegionMembership,
    client: ImfWeoClient,
    frequency: str,
    *,
    selected_countries: list[str],
    selected_subjects: list[str],
) -> CoreSelectionState:
    prompted_indicator_codes: list[str] = []
    if settings.interactive and not selected_countries:
        available_country_codes = _available_country_codes(catalog, _fetch_available_location_codes(client, frequency))
        country_totals = _fetch_indicator_availability(
            client,
            available_country_codes,
            frequency,
            strict=False,
            status_message="Checking country availability...",
        )
        available_country_codes = [
            result.requested_code for result in country_totals.results if result.series_count > 0
        ]
        selected_region_codes = _prompt_for_region_codes_for_countries(
            catalog,
            region_membership,
            available_country_codes=available_country_codes,
        )
        preselected_country_codes = region_membership.expand_region_codes(
            selected_region_codes,
            allowed_country_codes=available_country_codes,
        )
        selected_country_codes = _prompt_for_country_codes(
            "Select countries",
            catalog,
            legacy,
            available_country_codes=available_country_codes,
            detail_by_code=_build_total_count_details(country_totals.results, "indicator"),
            preselected=preselected_country_codes,
            required=True,
        )
        selected_countries = selected_country_codes
    if not selected_countries:
        raise ValueError("At least one country or region selector is required.")

    country_codes = _resolve_country_codes(selected_countries, catalog, legacy, aliases, region_membership)
    indicator_availability = _fetch_indicator_availability(client, country_codes, frequency)
    if not indicator_availability.available_codes:
        raise ValueError("No indicators are available for the selected countries and frequency.")

    if settings.interactive and not selected_subjects:
        prompted_indicator_codes = _prompt_for_indicator_codes(
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
        selected_subjects = list(prompted_indicator_codes)
    if not selected_subjects:
        raise ValueError("At least one subject descriptor selector is required.")

    indicator_codes = list(prompted_indicator_codes) if prompted_indicator_codes else _resolve_indicator_codes(
        selected_subjects,
        catalog,
        legacy,
        aliases,
    )
    indicator_codes = _restrict_codes(indicator_codes, indicator_availability.available_codes)
    if not indicator_codes:
        raise ValueError("No matching indicators are available for the selected countries.")

    return CoreSelectionState(country_codes=country_codes, indicator_codes=indicator_codes)


def _resolve_indicator_first_selection(
    settings: RuntimeSettings,
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
    region_membership: RegionMembership,
    client: ImfWeoClient,
    frequency: str,
    *,
    selected_countries: list[str],
    selected_subjects: list[str],
) -> CoreSelectionState:
    prompted_indicator_codes: list[str] = []
    country_availability: AvailabilityAggregate | None = None
    if settings.interactive and not selected_subjects:
        available_indicator_codes = _fetch_available_indicator_catalog_codes(client, frequency)
        indicator_totals = _fetch_country_availability(
            client,
            available_indicator_codes,
            frequency,
            strict=False,
            status_message="Checking subject availability...",
        )
        positive_indicator_results = [result for result in indicator_totals.results if result.series_count > 0]
        prompted_indicator_codes = _prompt_for_indicator_codes(
            "Select subject descriptors",
            [result.requested_code for result in positive_indicator_results],
            catalog,
            legacy,
            detail_by_code=_build_total_count_details(positive_indicator_results, "location"),
        )
        selected_subjects = list(prompted_indicator_codes)
        country_availability = _aggregate_availability_results(
            [
                result
                for result in positive_indicator_results
                if result.requested_code in prompted_indicator_codes
            ]
        )
    if not selected_subjects:
        raise ValueError("At least one subject descriptor selector is required.")

    indicator_codes = list(prompted_indicator_codes) if prompted_indicator_codes else _resolve_indicator_codes(
        selected_subjects,
        catalog,
        legacy,
        aliases,
    )
    if country_availability is None:
        country_availability = _fetch_country_availability(client, indicator_codes, frequency)
    available_country_codes = _available_country_codes(catalog, country_availability.available_codes)
    if not available_country_codes:
        raise ValueError("No countries are available for the selected subject descriptors and frequency.")

    if settings.interactive and not selected_countries:
        selected_region_codes = _prompt_for_region_codes_for_countries(
            catalog,
            region_membership,
            available_country_codes=available_country_codes,
        )
        preselected_country_codes = region_membership.expand_region_codes(
            selected_region_codes,
            allowed_country_codes=available_country_codes,
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
            preselected=preselected_country_codes,
            required=True,
        )
        selected_countries = selected_country_codes
    if not selected_countries:
        raise ValueError("At least one country or region selector is required.")

    country_codes = _resolve_country_codes(selected_countries, catalog, legacy, aliases, region_membership)
    country_codes = _restrict_codes(country_codes, available_country_codes)
    if not country_codes:
        raise ValueError("No matching countries are available for the selected subject descriptors.")

    return CoreSelectionState(country_codes=country_codes, indicator_codes=indicator_codes)


def _resolve_unit_scale_filters(
    settings: RuntimeSettings,
    indicator_codes: list[str],
    legacy: LegacyCatalog,
) -> FilterSelectionState:
    selected_units = list(settings.units)
    selected_scales = list(settings.scales)

    filtered_indicator_codes = _filter_indicator_codes(indicator_codes, selected_units, selected_scales, legacy)
    if not filtered_indicator_codes:
        raise ValueError("No WEO series match the selected Subject Descriptor, Units, and Scale combination.")

    if settings.interactive and not selected_units:
        filtered_indicator_codes, prompted_units = _resolve_contextual_legacy_filters(
            filtered_indicator_codes,
            legacy,
            dimension_name="units",
            label_map=legacy.indicator_units,
        )
        selected_units.extend(prompted_units)

    if settings.interactive and not selected_scales:
        filtered_indicator_codes, prompted_scales = _resolve_contextual_legacy_filters(
            filtered_indicator_codes,
            legacy,
            dimension_name="scales",
            label_map=legacy.indicator_scales,
        )
        selected_scales.extend(prompted_scales)

    return FilterSelectionState(
        indicator_codes=filtered_indicator_codes,
        selected_units=selected_units,
        selected_scales=selected_scales,
    )


def _resolve_contextual_legacy_filters(
    indicator_codes: list[str],
    legacy: LegacyCatalog,
    *,
    dimension_name: str,
    label_map: dict[str, set[str]],
) -> tuple[list[str], list[str]]:
    filtered_codes: list[str] = []
    prompted_labels: list[str] = []
    for subject_label, subject_codes in _group_indicator_codes_by_subject(indicator_codes, legacy):
        choices = _available_legacy_labels(subject_codes, label_map)
        selected_labels: list[str] = []
        if _subject_requires_dimension_prompt(subject_codes, label_map) and choices:
            selected_labels = _resolve_optional_labels(
                f"Select {dimension_name} for {subject_label}",
                choices,
            )
            prompted_labels.extend(selected_labels)
        matching_codes = _filter_indicator_codes(
            subject_codes,
            selected_labels if dimension_name == "units" else [],
            selected_labels if dimension_name == "scales" else [],
            legacy,
        )
        if not matching_codes:
            raise ValueError("No WEO series match the selected Subject Descriptor, Units, and Scale combination.")
        for code in matching_codes:
            if code not in filtered_codes:
                filtered_codes.append(code)
    return filtered_codes, prompted_labels


def _group_indicator_codes_by_subject(
    indicator_codes: list[str],
    legacy: LegacyCatalog,
) -> list[tuple[str, list[str]]]:
    grouped: dict[str, list[str]] = {}
    for code in indicator_codes:
        subject_label = legacy.preferred_subject_labels.get(code, code)
        grouped.setdefault(subject_label, []).append(code)
    return list(grouped.items())


def _subject_requires_dimension_prompt(indicator_codes: list[str], label_map: dict[str, set[str]]) -> bool:
    return any(
        _indicator_has_multiple_dimension_values(code, label_map)
        for code in indicator_codes
    )


def _indicator_has_multiple_dimension_values(indicator_code: str, label_map: dict[str, set[str]]) -> bool:
    return len(_non_empty_legacy_labels(label_map.get(indicator_code, set()))) > 1


def _non_empty_legacy_labels(labels: set[str]) -> set[str]:
    return {label for label in labels if label}


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


def _fetch_available_frequency_codes(client: ImfWeoClient) -> list[str]:
    return run_with_status(
        "Checking available frequencies...",
        client.fetch_available_frequency_codes,
    )


def _fetch_available_indicator_catalog_codes(client: ImfWeoClient, frequency: str) -> list[str]:
    return run_with_status(
        "Checking available subjects...",
        client.fetch_available_indicator_catalog_codes,
        frequency=frequency,
    )


def _fetch_available_time_periods(
    client: ImfWeoClient,
    country_codes: list[str],
    indicator_codes: list[str],
    frequency: str,
    ) -> list[int]:
    return run_with_status(
        "Checking available years...",
        client.fetch_available_time_periods,
        country_codes=country_codes,
        indicator_codes=indicator_codes,
        frequency=frequency,
    )


def _aggregate_availability_results(results: list[AvailabilityResult]) -> AvailabilityAggregate:
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


def _available_country_codes(catalog: Catalog, available_location_codes: list[str]) -> list[str]:
    available = set(available_location_codes)
    return [code for code in catalog.countries if code in available]


def _resolve_country_codes(
    requested: list[str],
    catalog: Catalog,
    legacy: LegacyCatalog,
    aliases: AliasConfig,
    region_membership: RegionMembership,
) -> list[str]:
    if any(str(item).strip() == "*" or normalize_label(item) in {"all", "all countries"} for item in requested):
        return list(catalog.countries.keys())
    location_codes = _resolve_codes(
        requested=requested,
        current_labels=catalog.locations,
        preferred_labels=legacy.preferred_country_labels,
        workbook_aliases=legacy.country_aliases,
        manual_aliases=aliases.countries,
        entity_name="country or region",
    )
    return _expand_location_codes(location_codes, catalog, region_membership)


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
    preselected: list[str] | None = None,
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
            preselected=preselected,
            detail_by_code=detail_by_code,
        ),
        required=required,
    )


def _prompt_for_region_codes(
    prompt: str,
    catalog: Catalog,
    available_region_codes: list[str],
    detail_by_code: dict[str, str] | None = None,
    required: bool = True,
) -> list[str]:
    return _prompt_for_codes(
        prompt,
        _build_choice_map_for_codes(
            available_region_codes,
            catalog.country_groups,
            catalog.country_groups,
            detail_by_code=detail_by_code,
        ),
        required=required,
    )


def _prompt_for_region_codes_for_countries(
    catalog: Catalog,
    region_membership: RegionMembership,
    *,
    available_country_codes: list[str],
) -> list[str]:
    available_region_codes = region_membership.available_region_codes(available_country_codes)
    if not available_region_codes:
        return []
    return _prompt_for_region_codes(
        "Select regions",
        catalog,
        available_region_codes,
        detail_by_code=_build_region_country_count_details(
            region_membership,
            available_region_codes,
            available_country_codes,
        ),
        required=False,
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


def _expand_location_codes(
    location_codes: list[str],
    catalog: Catalog,
    region_membership: RegionMembership,
) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()
    for code in location_codes:
        if code in catalog.countries:
            if code not in seen:
                seen.add(code)
                expanded.append(code)
            continue
        region_members = region_membership.members_by_region.get(code)
        if region_members is None:
            raise ValueError(f"Region membership is not configured for {catalog.locations.get(code, code)} [{code}].")
        for member in region_members:
            if member in seen:
                continue
            seen.add(member)
            expanded.append(member)
    return expanded


def _restrict_codes(resolved_codes: list[str], available_codes: list[str]) -> list[str]:
    available = set(available_codes)
    return [code for code in resolved_codes if code in available]


def _build_region_country_count_details(
    region_membership: RegionMembership,
    region_codes: list[str],
    available_country_codes: list[str],
) -> dict[str, str]:
    return {
        code: f"{region_membership.count_countries(code, allowed_country_codes=available_country_codes)} countries"
        for code in region_codes
    }


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


def _resolve_output_path(settings: RuntimeSettings, dataframe: pd.DataFrame, *, suffix: str) -> Path:
    if settings.output_path:
        return Path(settings.output_path)
    output_dir = Path("output")
    stem = _build_output_stem(settings, dataframe)
    return _ensure_unique_output_path(output_dir / f"{stem}{suffix}")


def _build_output_stem(settings: RuntimeSettings, dataframe: pd.DataFrame) -> str:
    country_fragment = _output_fragment(
        _frame_values(dataframe, "Country"),
        fallback_values=settings.countries,
        fallback="countries",
    )
    indicator_fragment = _output_fragment(
        _frame_values(dataframe, "Subject Descriptor"),
        fallback_values=settings.subject_descriptors,
        fallback="indicators",
    )
    frequency_fragment = _output_fragment(
        _frame_values(dataframe, "frequency"),
        fallback_values=[settings.frequency] if settings.frequency else [],
        fallback="data",
    )
    stem = f"weo_{country_fragment}_{indicator_fragment}_{frequency_fragment}"
    return stem[:120].rstrip("-_") or "weo_export"


def _frame_values(dataframe: pd.DataFrame, column_name: str) -> list[str]:
    if column_name not in dataframe.columns:
        return []
    values = dataframe[column_name].dropna().astype(str).tolist()
    unique_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_values.append(normalized)
    return unique_values


def _output_fragment(values: list[str], *, fallback_values: list[str], fallback: str) -> str:
    candidates = values or [value for value in fallback_values if str(value).strip()]
    if not candidates:
        return fallback
    primary = _slugify_output_value(candidates[0], fallback=fallback)
    if len(candidates) == 1:
        return primary
    return f"{primary}-plus-{len(candidates) - 1}"


def _slugify_output_value(value: str, *, fallback: str) -> str:
    compact = normalize_label(value).replace(" ", "-")
    return compact[:48].strip("-") or fallback


def _ensure_unique_output_path(path: Path) -> Path:
    if not path.exists():
        return path
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    candidate = path.with_stem(f"{path.stem}_{timestamp}")
    counter = 1
    while candidate.exists():
        counter += 1
        candidate = path.with_stem(f"{path.stem}_{timestamp}_{counter}")
    return candidate


def _write_excel_frame(
    path: Path,
    dataframe: pd.DataFrame,
    *,
    sheet_name: str,
    header_frequency: str | None = None,
    fixed_header_columns: int = 0,
) -> None:
    with pd.ExcelWriter(
        path,
        engine="openpyxl",
        date_format="YYYY-MM-DD",
        datetime_format="YYYY-MM-DD",
    ) as writer:
        dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        if header_frequency:
            worksheet = writer.book[sheet_name]
            _convert_period_header_cells(
                worksheet,
                frequency=header_frequency,
                fixed_header_columns=fixed_header_columns,
            )


def _normalize_excel_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    for column in normalized.columns:
        if _should_normalize_excel_date_column(column, normalized[column]):
            normalized[column] = _coerce_excel_date_series(normalized[column])
        elif _should_normalize_excel_column(column, normalized[column]):
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


def _should_normalize_excel_date_column(column: Any, series: pd.Series) -> bool:
    column_name = str(column)
    if "date" not in normalize_label(column_name):
        return False
    if not pd.api.types.is_object_dtype(series.dtype) and not pd.api.types.is_string_dtype(series.dtype):
        return False
    non_empty = series.replace("", pd.NA).dropna()
    if non_empty.empty:
        return False
    converted = pd.to_datetime(non_empty, errors="coerce")
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


def _coerce_excel_date_series(series: pd.Series) -> pd.Series:
    cleaned = series.replace("", pd.NA)
    return pd.to_datetime(cleaned, errors="coerce")


def _convert_period_header_cells(worksheet: Any, *, frequency: str, fixed_header_columns: int) -> None:
    for column_index in range(fixed_header_columns + 1, worksheet.max_column + 1):
        cell = worksheet.cell(row=1, column=column_index)
        parsed = _parse_period_header_to_datetime(cell.value, frequency)
        if parsed is None:
            continue
        cell.value = parsed
        cell.number_format = "yyyy-mm-dd"


def _parse_period_header_to_datetime(value: Any, frequency: str) -> datetime | None:
    if value in {None, ""}:
        return None

    normalized_frequency = str(frequency or "").strip().upper()
    text = str(value).strip()
    if not text:
        return None

    if normalized_frequency == "A":
        if re.fullmatch(r"\d{4}", text):
            return datetime(int(text), 12, 31)
        return None

    if normalized_frequency == "Q":
        match = re.fullmatch(r"(\d{4})[- ]?Q([1-4])", text)
        if match is None:
            return None
        year = int(match.group(1))
        quarter = int(match.group(2))
        month = quarter * 3
        return _end_of_month(year, month)

    if normalized_frequency == "M":
        match = re.fullmatch(r"(\d{4})(?:[-/ ]|M)(\d{1,2})", text)
        if match is None:
            return None
        year = int(match.group(1))
        month = int(match.group(2))
        if month < 1 or month > 12:
            return None
        return _end_of_month(year, month)

    if normalized_frequency == "D":
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()

    return None


def _end_of_month(year: int, month: int) -> datetime:
    return datetime(year, month, monthrange(year, month)[1])
