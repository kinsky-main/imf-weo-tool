from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .configuration import RuntimeSettings
from .imf import (
    AvailabilityAggregate,
    AvailabilityResult,
    Catalog,
    ImfWeoClient,
    SeriesVariant,
    TimePeriod,
    parse_time_period,
)
from .legacy import AliasConfig, load_alias_config, normalize_label
from .regions import DEFAULT_REGION_MEMBERSHIP_PATH, RegionMembership, load_region_membership
from .tui import (
    interactive_tui_session,
    prompt_for_choice,
    prompt_for_choices,
    prompt_for_time_range,
    run_with_status,
    set_interactive_summary,
)


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


@dataclass(slots=True)
class CoreSelectionState:
    country_codes: list[str]
    indicator_codes: list[str]


@dataclass(slots=True)
class FilterSelectionState:
    indicator_codes: list[str]
    selected_unit_codes: list[str]
    selected_scale_codes: list[str]


@dataclass(slots=True)
class ResolvedTimeRange:
    start_year: int | None
    end_year: int | None
    start_period: TimePeriod | None = None
    end_period: TimePeriod | None = None


def load_weo_dataframe(
    *,
    countries: list[str],
    subject_descriptors: list[str],
    units: list[str] | None = None,
    scales: list[str] | None = None,
    frequency: str = "A",
    start_year: int | None = None,
    end_year: int | None = None,
    alias_file: str = "config/weo_aliases.toml",
    interactive: bool = False,
) -> pd.DataFrame:
    settings = RuntimeSettings(
        countries=countries,
        subject_descriptors=subject_descriptors,
        units=list(units or []),
        scales=list(scales or []),
        frequency=frequency,
        frequency_explicit=False if interactive and str(frequency).strip().upper() == "A" else bool(str(frequency).strip()),
        start_year=start_year,
        end_year=end_year,
        start_year_explicit=start_year is not None,
        end_year_explicit=end_year is not None,
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
    aliases = load_alias_config(settings.alias_file)
    if settings.interactive and not settings.frequency_explicit:
        settings.frequency = ""
    if settings.interactive and not settings.start_year_explicit:
        settings.start_year = None
    if settings.interactive and not settings.end_year_explicit:
        settings.end_year = None
    settings.start_period_display = ""
    settings.end_period_display = ""

    if settings.interactive:
        selection_frequency = _primary_selection_frequency(settings)
        core_state = _resolve_primary_selection_state(
            settings,
            catalog,
            aliases,
            region_membership,
            active_client,
            selection_frequency,
        )
        _update_interactive_summary(
            settings,
            country_codes=core_state.country_codes,
            indicator_codes=core_state.indicator_codes,
        )
        frequency = _resolve_frequency_code(
            settings,
            catalog,
            active_client,
            country_codes=core_state.country_codes,
            indicator_codes=core_state.indicator_codes,
        )
        settings.frequency = frequency
        selections = _build_resolved_selections_for_core_state(
            settings,
            core_state,
            catalog,
            aliases,
            active_client,
            frequency,
        )
    else:
        scoped_country_codes, scoped_indicator_codes = _resolve_requested_scope_for_frequency_validation(
            settings,
            catalog,
            aliases,
            region_membership,
        )
        frequency = _resolve_frequency_code(
            settings,
            catalog,
            active_client,
            country_codes=scoped_country_codes,
            indicator_codes=scoped_indicator_codes,
        )
        settings.frequency = frequency
        selections = _resolve_selections(
            settings,
            catalog,
            aliases,
            region_membership,
            active_client,
            frequency,
        )
    _update_interactive_summary(settings, selections=selections)
    time_range = _resolve_time_range(
        settings,
        active_client,
        selections.country_codes,
        selections.indicator_codes,
        frequency,
    )
    settings.start_year = time_range.start_year
    settings.end_year = time_range.end_year
    settings.start_period_display = _display_time_period_text(time_range.start_period)
    settings.end_period_display = _display_time_period_text(time_range.end_period)
    _update_interactive_summary(settings, selections=selections)

    dataframe = run_with_status(
        "Fetching WEO data...",
        active_client.fetch_dataframe,
        country_codes=selections.country_codes,
        indicator_codes=selections.indicator_codes,
        unit_codes=selections.unit_codes,
        scale_codes=selections.scale_codes,
        frequency=frequency,
        start_year=time_range.start_year,
        end_year=time_range.end_year,
        subject_labels=selections.subject_labels,
        country_labels=selections.country_labels,
        unit_labels=selections.unit_labels,
        scale_labels=selections.scale_labels,
        start_period=time_range.start_period,
        end_period=time_range.end_period,
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
    aliases: AliasConfig,
    region_membership: RegionMembership,
    client: ImfWeoClient,
    frequency: str,
) -> ResolvedSelections:
    core_state = _resolve_primary_selection_state(
        settings,
        catalog,
        aliases,
        region_membership,
        client,
        frequency,
    )
    filter_state = _resolve_unit_scale_filters(
        settings,
        country_codes=core_state.country_codes,
        indicator_codes=core_state.indicator_codes,
        catalog=catalog,
        aliases=aliases,
        client=client,
        frequency=frequency,
    )
    return _build_resolved_selections(
        country_codes=core_state.country_codes,
        indicator_codes=filter_state.indicator_codes,
        selected_unit_codes=filter_state.selected_unit_codes,
        selected_scale_codes=filter_state.selected_scale_codes,
        catalog=catalog,
        aliases=aliases,
    )


def _build_resolved_selections_for_core_state(
    settings: RuntimeSettings,
    core_state: CoreSelectionState,
    catalog: Catalog,
    aliases: AliasConfig,
    client: ImfWeoClient,
    frequency: str,
) -> ResolvedSelections:
    filter_state = _resolve_unit_scale_filters(
        settings,
        country_codes=core_state.country_codes,
        indicator_codes=core_state.indicator_codes,
        catalog=catalog,
        aliases=aliases,
        client=client,
        frequency=frequency,
    )
    return _build_resolved_selections(
        country_codes=core_state.country_codes,
        indicator_codes=filter_state.indicator_codes,
        selected_unit_codes=filter_state.selected_unit_codes,
        selected_scale_codes=filter_state.selected_scale_codes,
        catalog=catalog,
        aliases=aliases,
    )


def _resolve_primary_selection_state(
    settings: RuntimeSettings,
    catalog: Catalog,
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


def _resolve_frequency_code(
    settings: RuntimeSettings,
    catalog: Catalog,
    client: ImfWeoClient,
    country_codes: list[str] | None = None,
    indicator_codes: list[str] | None = None,
) -> str:
    scoped_lookup = bool(country_codes and indicator_codes)
    available_frequency_codes = (
        _fetch_available_frequency_codes_for_scope(client, country_codes or [], indicator_codes or [])
        if scoped_lookup
        else _fetch_available_frequency_codes(client)
    )
    if not available_frequency_codes:
        if scoped_lookup:
            raise ValueError("No frequencies are available for the selected countries and subject descriptors.")
        raise ValueError("No frequencies are available for IMF WEO.")

    requested_frequency = str(settings.frequency or "").strip().upper()
    if requested_frequency:
        if requested_frequency not in available_frequency_codes:
            global_available_frequency_codes = _fetch_available_frequency_codes(client)
            if requested_frequency not in global_available_frequency_codes:
                available = ", ".join(global_available_frequency_codes)
                raise ValueError(
                    f"Frequency '{requested_frequency}' is not available for IMF WEO. "
                    f"Available frequencies: {available}."
                )
            available = ", ".join(available_frequency_codes)
            raise ValueError(
                f"Frequency '{requested_frequency}' is not available for the selected countries and subject descriptors. "
                f"Available frequencies: {available}."
            )
        return requested_frequency

    if settings.interactive:
        return _prompt_for_frequency(catalog, available_frequency_codes)
    return available_frequency_codes[0]


def _primary_selection_frequency(settings: RuntimeSettings) -> str:
    requested_frequency = str(settings.frequency or "").strip().upper()
    if settings.frequency_explicit and requested_frequency:
        return requested_frequency
    return "*"


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


def _resolve_requested_scope_for_frequency_validation(
    settings: RuntimeSettings,
    catalog: Catalog,
    aliases: AliasConfig,
    region_membership: RegionMembership,
) -> tuple[list[str] | None, list[str] | None]:
    if not settings.countries or not settings.subject_descriptors:
        return None, None
    country_codes = _resolve_country_codes(settings.countries, catalog, aliases, region_membership)
    indicator_codes = _resolve_indicator_codes(settings.subject_descriptors, catalog, aliases)
    return country_codes, indicator_codes


def _update_interactive_summary(
    settings: RuntimeSettings,
    *,
    selections: ResolvedSelections | None = None,
    country_codes: list[str] | None = None,
    indicator_codes: list[str] | None = None,
) -> None:
    if not settings.interactive:
        return

    lines: list[str] = []
    if settings.frequency:
        lines.append(f"Frequency: {settings.frequency}")
    selected_country_codes = selections.country_codes if selections is not None else list(country_codes or [])
    selected_indicator_codes = selections.indicator_codes if selections is not None else list(indicator_codes or [])
    if selected_country_codes:
        lines.append(f"Countries: {len(selected_country_codes)} selected")
    if selected_indicator_codes:
        lines.append(f"Subjects: {len(selected_indicator_codes)} selected")
    if selections is not None:
        if settings.start_period_display or settings.end_period_display:
            start = settings.start_period_display or "min"
            end = settings.end_period_display or "max"
            lines.append(f"Date range: {start} to {end}")
        elif settings.start_year is not None or settings.end_year is not None:
            start = settings.start_year if settings.start_year is not None else "min"
            end = settings.end_year if settings.end_year is not None else "max"
            lines.append(f"Date range: {start} to {end}")
    set_interactive_summary(lines)


def _resolve_time_range(
    settings: RuntimeSettings,
    client: ImfWeoClient,
    country_codes: list[str],
    indicator_codes: list[str],
    frequency: str,
) -> ResolvedTimeRange:
    start_year = settings.start_year if settings.start_year_explicit or not settings.interactive else None
    end_year = settings.end_year if settings.end_year_explicit or not settings.interactive else None

    if settings.start_year_explicit and settings.end_year_explicit and start_year is not None and end_year is not None:
        _validate_date_range(start_year, end_year)
        return ResolvedTimeRange(start_year=start_year, end_year=end_year)

    if not settings.interactive:
        if start_year is not None and end_year is not None:
            _validate_date_range(start_year, end_year)
        return ResolvedTimeRange(start_year=start_year, end_year=end_year)

    available_periods = _fetch_available_time_periods(client, country_codes, indicator_codes, frequency)
    if not available_periods:
        if start_year is not None and end_year is not None:
            _validate_date_range(start_year, end_year)
        return ResolvedTimeRange(start_year=start_year, end_year=end_year)

    constrained_periods = _constrain_time_periods_by_years(available_periods, start_year, end_year)
    if constrained_periods:
        available_periods = constrained_periods

    if len(available_periods) == 1 and start_year is None and end_year is None:
        only_period = available_periods[0]
        return ResolvedTimeRange(
            start_year=only_period.start_date.year if only_period.start_date is not None else None,
            end_year=only_period.end_date.year if only_period.end_date is not None else None,
            start_period=only_period,
            end_period=only_period,
        )

    if settings.start_year_explicit and settings.end_year_explicit and start_year is not None and end_year is not None:
        _validate_date_range(start_year, end_year)
        return ResolvedTimeRange(start_year=start_year, end_year=end_year)

    default_start_period = available_periods[0]
    default_end_period = available_periods[-1]
    start_period, end_period = _prompt_for_time_period_range(
        frequency,
        available_periods,
        default_start=default_start_period.display_text,
        default_end=default_end_period.display_text,
    )
    if start_period.start_date is not None and end_period.end_date is not None:
        _validate_date_range(start_period.start_date.year, end_period.end_date.year)
    return ResolvedTimeRange(
        start_year=start_period.start_date.year if start_period.start_date is not None else None,
        end_year=end_period.end_date.year if end_period.end_date is not None else None,
        start_period=start_period,
        end_period=end_period,
    )


def _resolve_date_range(
    settings: RuntimeSettings,
    client: ImfWeoClient,
    country_codes: list[str],
    indicator_codes: list[str],
    frequency: str,
) -> tuple[int | None, int | None]:
    resolved = _resolve_time_range(settings, client, country_codes, indicator_codes, frequency)
    return resolved.start_year, resolved.end_year


def _validate_date_range(start_year: int, end_year: int) -> None:
    if start_year > end_year:
        raise ValueError(f"Start year {start_year} cannot be later than end year {end_year}.")


def _display_time_period_text(period: TimePeriod | None) -> str:
    if period is None:
        return ""
    return period.display_text


def _time_period_placeholder(frequency: str) -> str:
    normalized_frequency = str(frequency or "").strip().upper()
    if normalized_frequency == "Q":
        return "Q1 2024"
    if normalized_frequency == "M":
        return "03.2024"
    if normalized_frequency == "D":
        return "31.12.2024"
    return "2024"


def _time_period_caption(frequency: str) -> str:
    normalized_frequency = str(frequency or "").strip().upper()
    if normalized_frequency == "Q":
        return "Enter a quarter like Q1 2024."
    if normalized_frequency == "M":
        return "Enter a month like 03.2024."
    if normalized_frequency == "D":
        return "Enter a day like 31.12.2024."
    return "Enter a year like 2024."


def _prompt_for_time_period_range(
    frequency: str,
    available_periods: list[TimePeriod],
    *,
    default_start: str,
    default_end: str,
) -> tuple[TimePeriod, TimePeriod]:
    if not available_periods:
        raise ValueError("No time periods are available for the selected series.")
    if len(available_periods) == 1:
        return available_periods[0], available_periods[0]

    available_by_sort_key = {period.sort_key: period for period in available_periods}

    def validate(start_text: str, end_text: str) -> tuple[str, str]:
        start_period = _resolve_prompt_time_period(start_text, frequency, available_by_sort_key)
        end_period = _resolve_prompt_time_period(end_text, frequency, available_by_sort_key)
        if start_period.sort_key > end_period.sort_key:
            raise ValueError(
                f"Start {start_period.display_text} cannot be later than end {end_period.display_text}."
            )
        return start_period.display_text, end_period.display_text

    start_text, end_text = prompt_for_time_range(
        title="Select time range",
        start_value=default_start,
        end_value=default_end,
        start_placeholder=_time_period_placeholder(frequency),
        end_placeholder=_time_period_placeholder(frequency),
        caption=_time_period_caption(frequency),
        validate=validate,
    )
    return (
        _resolve_prompt_time_period(start_text, frequency, available_by_sort_key),
        _resolve_prompt_time_period(end_text, frequency, available_by_sort_key),
    )


def _resolve_prompt_time_period(
    text: str,
    frequency: str,
    available_by_sort_key: dict[str, TimePeriod],
) -> TimePeriod:
    parsed = parse_time_period(text, frequency)
    if parsed is None:
        raise ValueError(f"Invalid {str(frequency or '').strip().upper() or 'time'} period: {text}")
    matched = available_by_sort_key.get(parsed.sort_key)
    if matched is None:
        raise ValueError(f"{parsed.display_text} is not available for the selected series.")
    return matched


def _constrain_time_periods_by_years(
    available_periods: list[TimePeriod],
    start_year: int | None,
    end_year: int | None,
) -> list[TimePeriod]:
    constrained: list[TimePeriod] = []
    for period in available_periods:
        if start_year is not None and period.end_date is not None and period.end_date.year < start_year:
            continue
        if end_year is not None and period.start_date is not None and period.start_date.year > end_year:
            continue
        constrained.append(period)
    return constrained


def _resolve_country_first_selection(
    settings: RuntimeSettings,
    catalog: Catalog,
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
            available_country_codes=available_country_codes,
            detail_by_code=_build_total_count_details(country_totals.results, "indicator"),
            preselected=preselected_country_codes,
            required=True,
        )
        selected_countries = selected_country_codes
    if not selected_countries:
        raise ValueError("At least one country or region selector is required.")

    country_codes = _resolve_country_codes(selected_countries, catalog, aliases, region_membership)
    indicator_availability = _fetch_indicator_availability(client, country_codes, frequency)
    if not indicator_availability.available_codes:
        raise ValueError("No indicators are available for the selected countries and frequency.")

    if settings.interactive and not selected_subjects:
        indicator_frequency_codes = _fetch_indicator_frequency_details(
            client,
            indicator_availability.available_codes,
            country_codes=country_codes,
        )
        prompted_indicator_codes = _prompt_for_indicator_codes(
            "Select subject descriptors",
            indicator_availability.available_codes,
            catalog,
            frequency_by_code=_format_indicator_frequency_meta(catalog, indicator_frequency_codes),
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
        aliases,
    )
    indicator_codes = _restrict_codes(indicator_codes, indicator_availability.available_codes)
    if not indicator_codes:
        raise ValueError("No matching indicators are available for the selected countries.")

    return CoreSelectionState(country_codes=country_codes, indicator_codes=indicator_codes)


def _resolve_indicator_first_selection(
    settings: RuntimeSettings,
    catalog: Catalog,
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
    prompted_location_codes: list[str] = []
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
        indicator_frequency_codes = _fetch_indicator_frequency_details(
            client,
            [result.requested_code for result in positive_indicator_results],
        )
        prompted_indicator_codes = _prompt_for_indicator_codes(
            "Select subject descriptors",
            [result.requested_code for result in positive_indicator_results],
            catalog,
            frequency_by_code=_format_indicator_frequency_meta(catalog, indicator_frequency_codes),
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
        aliases,
    )
    if country_availability is None:
        country_availability = _fetch_country_availability(client, indicator_codes, frequency)
    available_location_codes = _available_location_codes(catalog, country_availability.available_codes)
    if not available_location_codes:
        raise ValueError("No locations are available for the selected subject descriptors and frequency.")

    if settings.interactive and not selected_countries:
        available_country_codes = [code for code in available_location_codes if code in catalog.countries]
        selected_region_codes = _prompt_for_region_codes_for_countries(
            catalog,
            region_membership,
            available_country_codes=available_country_codes,
        ) if available_country_codes else []
        preselected_country_codes = region_membership.expand_region_codes(
            selected_region_codes,
            allowed_country_codes=available_country_codes,
        )
        prompt_title = "Select countries" if len(available_country_codes) == len(available_location_codes) else "Select locations"
        prompted_location_codes = _prompt_for_location_codes(
            prompt_title,
            catalog,
            available_location_codes=available_location_codes,
            detail_by_code=_build_ratio_count_details(
                _filter_count_map(country_availability.counts_by_code, available_location_codes),
                len(indicator_codes),
                "subject",
            ),
            preselected=preselected_country_codes,
            required=True,
        )
        selected_countries = prompted_location_codes
    if not selected_countries:
        raise ValueError("At least one country or region selector is required.")

    country_codes = (
        list(prompted_location_codes)
        if prompted_location_codes
        else _resolve_country_codes(selected_countries, catalog, aliases, region_membership)
    )
    country_codes = _restrict_codes(country_codes, available_location_codes)
    if not country_codes:
        raise ValueError("No matching locations are available for the selected subject descriptors.")

    return CoreSelectionState(country_codes=country_codes, indicator_codes=indicator_codes)


def _resolve_unit_scale_filters(
    settings: RuntimeSettings,
    *,
    country_codes: list[str],
    indicator_codes: list[str],
    catalog: Catalog,
    aliases: AliasConfig,
    client: ImfWeoClient,
    frequency: str,
) -> FilterSelectionState:
    variants_by_indicator = _fetch_indicator_series_variants(
        client,
        country_codes=country_codes,
        indicator_codes=indicator_codes,
        frequency=frequency,
    )
    selected_unit_codes = _resolve_requested_attribute_codes(
        settings.units,
        available_codes=_available_variant_attribute_codes(variants_by_indicator, "unit"),
        current_labels=catalog.units,
        display_overrides=aliases.unit_display,
        manual_aliases=aliases.units,
        entity_name="unit",
    )
    selected_scale_codes = _resolve_requested_attribute_codes(
        settings.scales,
        available_codes=_available_variant_attribute_codes(variants_by_indicator, "scale"),
        current_labels=catalog.scales,
        display_overrides=aliases.scale_display,
        manual_aliases=aliases.scales,
        entity_name="scale",
    )

    filtered_indicator_codes = _filter_indicator_codes(
        indicator_codes,
        variants_by_indicator,
        selected_unit_codes,
        selected_scale_codes,
    )
    if not filtered_indicator_codes:
        raise ValueError("No WEO series match the selected Subject Descriptor, Units, and Scale combination.")

    if settings.interactive and not selected_unit_codes:
        filtered_indicator_codes, prompted_units = _resolve_contextual_attribute_filters(
            filtered_indicator_codes,
            variants_by_indicator,
            subject_labels=catalog.indicators,
            dimension_name="units",
            current_labels=catalog.units,
            display_overrides=aliases.unit_display,
            manual_aliases=aliases.units,
            selected_unit_codes=selected_unit_codes,
            selected_scale_codes=selected_scale_codes,
        )
        selected_unit_codes = _unique_codes(selected_unit_codes + prompted_units)

    if settings.interactive and not selected_scale_codes:
        filtered_indicator_codes, prompted_scales = _resolve_contextual_attribute_filters(
            filtered_indicator_codes,
            variants_by_indicator,
            subject_labels=catalog.indicators,
            dimension_name="scales",
            current_labels=catalog.scales,
            display_overrides=aliases.scale_display,
            manual_aliases=aliases.scales,
            selected_unit_codes=selected_unit_codes,
            selected_scale_codes=selected_scale_codes,
        )
        selected_scale_codes = _unique_codes(selected_scale_codes + prompted_scales)

    return FilterSelectionState(
        indicator_codes=filtered_indicator_codes,
        selected_unit_codes=selected_unit_codes,
        selected_scale_codes=selected_scale_codes,
    )


def _resolve_contextual_attribute_filters(
    indicator_codes: list[str],
    variants_by_indicator: dict[str, list[SeriesVariant]],
    *,
    subject_labels: dict[str, str],
    dimension_name: str,
    current_labels: dict[str, str],
    display_overrides: dict[str, str],
    manual_aliases: dict[str, str],
    selected_unit_codes: list[str],
    selected_scale_codes: list[str],
) -> tuple[list[str], list[str]]:
    filtered_codes: list[str] = []
    prompted_codes: list[str] = []
    for subject_label, subject_codes in _group_indicator_codes_by_subject(indicator_codes, subject_labels):
        choices = _available_attribute_codes(
            subject_codes,
            variants_by_indicator,
            dimension_name=dimension_name,
            selected_unit_codes=selected_unit_codes,
            selected_scale_codes=selected_scale_codes,
        )
        selected_codes: list[str] = []
        if _subject_requires_dimension_prompt(
            subject_codes,
            variants_by_indicator,
            dimension_name=dimension_name,
            selected_unit_codes=selected_unit_codes,
            selected_scale_codes=selected_scale_codes,
        ) and choices:
            selected_codes = _resolve_optional_attribute_codes(
                f"Select {dimension_name} for {subject_label}",
                choices,
                current_labels=current_labels,
                display_overrides=display_overrides,
                manual_aliases=manual_aliases,
            )
            prompted_codes.extend(selected_codes)
        matching_codes = _filter_indicator_codes(
            subject_codes,
            variants_by_indicator,
            selected_codes if dimension_name == "units" else selected_unit_codes,
            selected_codes if dimension_name == "scales" else selected_scale_codes,
        )
        if not matching_codes:
            raise ValueError("No WEO series match the selected Subject Descriptor, Units, and Scale combination.")
        for code in matching_codes:
            if code not in filtered_codes:
                filtered_codes.append(code)
    return filtered_codes, _unique_codes(prompted_codes)


def _group_indicator_codes_by_subject(
    indicator_codes: list[str],
    subject_labels: dict[str, str],
) -> list[tuple[str, list[str]]]:
    grouped: dict[str, list[str]] = {}
    for code in indicator_codes:
        subject_label = subject_labels.get(code, code)
        grouped.setdefault(subject_label, []).append(code)
    return list(grouped.items())


def _subject_requires_dimension_prompt(
    indicator_codes: list[str],
    variants_by_indicator: dict[str, list[SeriesVariant]],
    *,
    dimension_name: str,
    selected_unit_codes: list[str],
    selected_scale_codes: list[str],
) -> bool:
    return any(
        _indicator_has_multiple_dimension_values(
            code,
            variants_by_indicator,
            dimension_name=dimension_name,
            selected_unit_codes=selected_unit_codes,
            selected_scale_codes=selected_scale_codes,
        )
        for code in indicator_codes
    )


def _indicator_has_multiple_dimension_values(
    indicator_code: str,
    variants_by_indicator: dict[str, list[SeriesVariant]],
    *,
    dimension_name: str,
    selected_unit_codes: list[str],
    selected_scale_codes: list[str],
) -> bool:
    return len(
        _attribute_codes_for_indicator(
            indicator_code,
            variants_by_indicator,
            dimension_name=dimension_name,
            selected_unit_codes=selected_unit_codes,
            selected_scale_codes=selected_scale_codes,
        )
    ) > 1


def _build_resolved_selections(
    *,
    country_codes: list[str],
    indicator_codes: list[str],
    selected_unit_codes: list[str],
    selected_scale_codes: list[str],
    catalog: Catalog,
    aliases: AliasConfig,
) -> ResolvedSelections:
    country_labels = {code: catalog.locations[code] for code in country_codes}
    subject_labels = {code: catalog.indicators[code] for code in indicator_codes}
    unit_labels = {
        code: aliases.unit_display.get(code, catalog.units.get(code, code))
        for code in selected_unit_codes or catalog.units
        if code
    }
    scale_labels = {
        code: aliases.scale_display.get(code, catalog.scales.get(code, code))
        for code in selected_scale_codes or catalog.scales
    }
    return ResolvedSelections(
        country_codes=country_codes,
        indicator_codes=indicator_codes,
        unit_codes=list(selected_unit_codes),
        scale_codes=list(selected_scale_codes),
        country_labels=country_labels,
        subject_labels=subject_labels,
        unit_labels=unit_labels,
        scale_labels=scale_labels,
    )


def _fetch_indicator_series_variants(
    client: ImfWeoClient,
    *,
    country_codes: list[str],
    indicator_codes: list[str],
    frequency: str,
) -> dict[str, list[SeriesVariant]]:
    return run_with_status(
        "Checking available units and scales...",
        client.fetch_indicator_series_variants,
        country_codes=country_codes,
        indicator_codes=indicator_codes,
        frequency=frequency,
    )


def _resolve_requested_attribute_codes(
    requested: list[str],
    *,
    available_codes: list[str],
    current_labels: dict[str, str],
    display_overrides: dict[str, str],
    manual_aliases: dict[str, str],
    entity_name: str,
) -> list[str]:
    if not requested:
        return []
    codes = available_codes or list(current_labels.keys())
    return _resolve_contextual_codes(
        requested=requested,
        available_codes=codes,
        current_labels=current_labels,
        display_overrides=display_overrides,
        manual_aliases=manual_aliases,
        entity_name=entity_name,
    )


def _available_variant_attribute_codes(
    variants_by_indicator: dict[str, list[SeriesVariant]],
    dimension_name: str,
) -> list[str]:
    codes = {
        _variant_dimension_value(variant, dimension_name)
        for variants in variants_by_indicator.values()
        for variant in variants
        if _variant_dimension_value(variant, dimension_name)
    }
    return sorted(codes)


def _available_attribute_codes(
    indicator_codes: list[str],
    variants_by_indicator: dict[str, list[SeriesVariant]],
    *,
    dimension_name: str,
    selected_unit_codes: list[str],
    selected_scale_codes: list[str],
) -> list[str]:
    codes = {
        code
        for indicator_code in indicator_codes
        for code in _attribute_codes_for_indicator(
            indicator_code,
            variants_by_indicator,
            dimension_name=dimension_name,
            selected_unit_codes=selected_unit_codes,
            selected_scale_codes=selected_scale_codes,
        )
    }
    return sorted(codes)


def _attribute_codes_for_indicator(
    indicator_code: str,
    variants_by_indicator: dict[str, list[SeriesVariant]],
    *,
    dimension_name: str,
    selected_unit_codes: list[str],
    selected_scale_codes: list[str],
) -> list[str]:
    values = {
        _variant_dimension_value(variant, dimension_name)
        for variant in variants_by_indicator.get(indicator_code, [])
        if _variant_matches_filters(
            variant,
            selected_unit_codes=selected_unit_codes,
            selected_scale_codes=selected_scale_codes,
        )
        and _variant_dimension_value(variant, dimension_name)
    }
    return sorted(values)


def _variant_dimension_value(variant: SeriesVariant, dimension_name: str) -> str:
    if dimension_name == "units":
        return variant.unit_code
    return variant.scale_code


def _variant_matches_filters(
    variant: SeriesVariant,
    *,
    selected_unit_codes: list[str],
    selected_scale_codes: list[str],
) -> bool:
    unit_match = not selected_unit_codes or variant.unit_code in selected_unit_codes
    scale_match = not selected_scale_codes or variant.scale_code in selected_scale_codes
    return unit_match and scale_match


def _resolve_optional_attribute_codes(
    prompt: str,
    codes: list[str],
    *,
    current_labels: dict[str, str],
    display_overrides: dict[str, str],
    manual_aliases: dict[str, str],
) -> list[str]:
    if not codes:
        return []
    if len(codes) == 1:
        return list(codes)
    choices = _build_choice_map(
        {code: current_labels.get(code, code) for code in codes},
        {code: display_overrides.get(code, current_labels.get(code, code)) for code in codes},
        preselected=codes,
    )
    return _prompt_for_codes(prompt, choices, required=False)


def _unique_codes(values: list[str]) -> list[str]:
    unique: list[str] = []
    for value in values:
        if value and value not in unique:
            unique.append(value)
    return unique


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


def _fetch_available_frequency_codes_for_scope(
    client: ImfWeoClient,
    country_codes: list[str],
    indicator_codes: list[str],
) -> list[str]:
    if not hasattr(client, "fetch_available_frequencies"):
        return _fetch_available_frequency_codes(client)
    return run_with_status(
        "Checking available frequencies...",
        client.fetch_available_frequencies,
        country_codes=country_codes,
        indicator_codes=indicator_codes,
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
    ) -> list[TimePeriod]:
    raw_periods = run_with_status(
        "Checking available time periods...",
        client.fetch_available_time_periods,
        country_codes=country_codes,
        indicator_codes=indicator_codes,
        frequency=frequency,
    )
    periods: list[TimePeriod] = []
    for raw_period in raw_periods:
        if isinstance(raw_period, TimePeriod):
            periods.append(raw_period)
            continue
        parsed = parse_time_period(raw_period, frequency)
        if parsed is not None:
            periods.append(parsed)
    return periods


def _fetch_indicator_frequency_details(
    client: ImfWeoClient,
    indicator_codes: list[str],
    *,
    country_codes: list[str] | None = None,
) -> dict[str, list[str]]:
    if not indicator_codes:
        return {}
    if hasattr(client, "fetch_indicator_frequency_availability"):
        return run_with_status(
            "Checking available frequencies...",
            client.fetch_indicator_frequency_availability,
            indicator_codes=indicator_codes,
            country_codes=country_codes,
        )
    return run_with_status(
        "Checking available frequencies...",
        lambda: {
            indicator_code: client.fetch_available_frequencies(list(country_codes or []), [indicator_code])
            for indicator_code in indicator_codes
        },
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


def _available_location_codes(catalog: Catalog, available_location_codes: list[str]) -> list[str]:
    available = set(available_location_codes)
    return [code for code in catalog.locations if code in available]


def _resolve_country_codes(
    requested: list[str],
    catalog: Catalog,
    aliases: AliasConfig,
    region_membership: RegionMembership,
) -> list[str]:
    if any(str(item).strip() == "*" or normalize_label(item) in {"all", "all countries"} for item in requested):
        return list(catalog.countries.keys())
    location_codes = _resolve_codes(
        requested=requested,
        current_labels=catalog.locations,
        preferred_labels=catalog.locations,
        manual_aliases=aliases.countries,
        entity_name="country or region",
    )
    return _expand_location_codes(location_codes, catalog, region_membership)


def _resolve_indicator_codes(
    requested: list[str],
    catalog: Catalog,
    aliases: AliasConfig,
) -> list[str]:
    return _resolve_codes(
        requested=requested,
        current_labels=catalog.indicators,
        preferred_labels=catalog.indicators,
        manual_aliases=aliases.subjects,
        entity_name="subject descriptor",
        allow_multiple_matches=True,
    )


def _prompt_for_country_codes(
    prompt: str,
    catalog: Catalog,
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
            catalog.countries,
            preselected=preselected,
            detail_by_code=detail_by_code,
        ),
        required=required,
    )


def _prompt_for_location_codes(
    prompt: str,
    catalog: Catalog,
    available_location_codes: list[str] | None = None,
    preselected: list[str] | None = None,
    detail_by_code: dict[str, str] | None = None,
    required: bool = True,
) -> list[str]:
    codes = available_location_codes or list(catalog.locations.keys())
    return _prompt_for_codes(
        prompt,
        _build_choice_map_for_codes(
            codes,
            catalog.locations,
            catalog.locations,
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
    frequency_by_code: dict[str, str] | None = None,
    detail_by_code: dict[str, str] | None = None,
) -> list[str]:
    return _prompt_for_codes(
        prompt,
        _build_choice_map_for_codes(
            available_indicator_codes,
            catalog.indicators,
            catalog.indicators,
            meta_by_code=frequency_by_code,
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
    manual_aliases: dict[str, list[str]],
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
    for alias, codes in manual_aliases.items():
        for code in codes:
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


def _build_choice_map_for_codes(
    codes: list[str],
    current_labels: dict[str, str],
    preferred_labels: dict[str, str],
    preselected: list[str] | None = None,
    meta_by_code: dict[str, str] | None = None,
    detail_by_code: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    subset = {code: current_labels[code] for code in codes}
    return _build_choice_map(
        subset,
        preferred_labels,
        preselected=preselected,
        meta_by_code=meta_by_code,
        detail_by_code=detail_by_code,
    )


def _build_choice_map(
    current_labels: dict[str, str],
    preferred_labels: dict[str, str],
    preselected: list[str] | None = None,
    meta_by_code: dict[str, str] | None = None,
    detail_by_code: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    choices: list[dict[str, Any]] = []
    for code, current_label in sorted(current_labels.items(), key=lambda item: preferred_labels.get(item[0], item[1])):
        preferred = preferred_labels.get(code, current_label)
        title = f"{preferred} [{code}]"
        if preferred != current_label:
            title = f"{title} ({current_label})"
        choice: dict[str, Any] = {"name": title, "value": code}
        if meta_by_code and code in meta_by_code:
            choice["meta"] = meta_by_code[code]
        if detail_by_code and code in detail_by_code:
            choice["detail"] = detail_by_code[code]
        if preselected and code in preselected:
            choice["checked"] = True
        choices.append(choice)
    return choices


def _filter_indicator_codes(
    indicator_codes: list[str],
    variants_by_indicator: dict[str, list[SeriesVariant]],
    selected_unit_codes: list[str],
    selected_scale_codes: list[str],
) -> list[str]:
    if not selected_unit_codes and not selected_scale_codes:
        return indicator_codes

    filtered: list[str] = []
    for code in indicator_codes:
        if any(
            _variant_matches_filters(
                variant,
                selected_unit_codes=selected_unit_codes,
                selected_scale_codes=selected_scale_codes,
            )
            for variant in variants_by_indicator.get(code, [])
        ):
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


def _format_indicator_frequency_meta(
    catalog: Catalog,
    frequency_codes_by_indicator: dict[str, list[str]],
) -> dict[str, str]:
    ordering = {code: index for index, code in enumerate(catalog.frequencies)}

    def sort_key(code: str) -> tuple[int, str]:
        return ordering.get(code, len(ordering)), code

    formatted: dict[str, str] = {}
    for indicator_code, frequency_codes in frequency_codes_by_indicator.items():
        unique_codes = sorted({code for code in frequency_codes if code}, key=sort_key)
        if unique_codes:
            formatted[indicator_code] = ",".join(unique_codes)
    return formatted


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
    period = parse_time_period(value, frequency)
    if period is None or period.end_date is None:
        return None
    return datetime.combine(period.end_date, datetime.min.time())


def _end_of_month(year: int, month: int) -> datetime:
    return datetime(year, month, monthrange(year, month)[1])
