from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import json
import re
from typing import Any, Callable

import pandas as pd
from pysdmx.api.qb import (
    ApiVersion,
    AvailabilityMode,
    AvailabilityQuery,
    DataContext,
    DataFormat,
    DataQuery,
    RestService,
    StructureDetail,
    StructureFormat,
    StructureQuery,
    StructureType,
)
from pysdmx.errors import Invalid
from pysdmx.io.reader import read_sdmx


API_BASE = "https://api.imf.org/external/sdmx/3.0"
MAX_AVAILABILITY_WORKERS = 8
MAX_DATA_QUERY_KEY_LENGTH = 180
COUNTRY_CODE_PATTERN = re.compile(r"^[A-Z]{3}$")


@dataclass(slots=True)
class ReleaseInfo:
    version: str
    updated_at: str
    name: str


@dataclass(slots=True)
class Catalog:
    release: ReleaseInfo
    countries: dict[str, str]
    country_groups: dict[str, str]
    locations: dict[str, str]
    indicators: dict[str, str]
    units: dict[str, str]
    scales: dict[str, str]


@dataclass(slots=True)
class AvailabilityResult:
    requested_code: str
    available_codes: list[str]
    series_count: int
    error_message: str | None = None


@dataclass(slots=True)
class AvailabilityAggregate:
    results: list[AvailabilityResult]
    available_codes: list[str]
    common_codes: list[str]
    counts_by_code: dict[str, int]


class AvailabilityLookupError(ValueError):
    pass


class ImfWeoClient:
    def __init__(self) -> None:
        self._catalog: Catalog | None = None
        self._available_location_codes_by_frequency: dict[str, list[str]] = {}
        self._available_indicator_catalog_codes_by_frequency: dict[str, list[str]] = {}
        self._service = RestService(
            API_BASE,
            ApiVersion.V2_2_2,
            data_format=DataFormat.SDMX_CSV_2_1_0,
            structure_format=StructureFormat.SDMX_JSON_2_0_0,
            timeout=60,
        )

    def fetch_catalog(self) -> Catalog:
        if self._catalog is not None:
            return self._catalog

        dataflow = self._fetch_structure_json(
            StructureQuery(
                artefact_type=StructureType.DATAFLOW,
                agency_id="IMF.RES",
                resource_id="WEO",
                version="+",
                detail=StructureDetail.FULL,
            )
        )
        flow = dataflow["data"]["dataflows"][0]
        updated_at = ""
        for annotation in flow.get("annotations", []):
            if annotation.get("id") == "lastUpdatedAt":
                updated_at = annotation.get("value", "")
                break

        release = ReleaseInfo(version=flow["version"], updated_at=updated_at, name=flow["name"])
        locations = self._fetch_codelist("IMF.RES", "CL_WEO_COUNTRY")
        countries, country_groups = _split_weo_locations(locations)
        indicators = self._fetch_codelist("IMF.RES", "CL_WEO_INDICATOR")
        units = self._fetch_codelist("IMF", "CL_UNIT")
        scales = self._fetch_codelist("IMF", "CL_UNIT_MULT")

        self._catalog = Catalog(
            release=release,
            countries=countries,
            country_groups=country_groups,
            locations=locations,
            indicators=indicators,
            units=units,
            scales=scales,
        )
        return self._catalog

    def fetch_available_location_codes(self, frequency: str) -> list[str]:
        cached = self._available_location_codes_by_frequency.get(frequency)
        if cached is not None:
            return list(cached)
        available_codes = self._fetch_batched_available_codes(
            component_id="COUNTRY",
            key=f"*.*.{frequency}",
        )
        self._available_location_codes_by_frequency[frequency] = available_codes
        return list(available_codes)

    def fetch_available_indicator_catalog_codes(self, frequency: str) -> list[str]:
        cached = self._available_indicator_catalog_codes_by_frequency.get(frequency)
        if cached is not None:
            return list(cached)
        available_codes = self._fetch_batched_available_codes(
            component_id="INDICATOR",
            key=f"*.*.{frequency}",
        )
        self._available_indicator_catalog_codes_by_frequency[frequency] = available_codes
        return list(available_codes)

    def fetch_available_indicator_codes(
        self,
        country_codes: list[str],
        frequency: str,
    ) -> list[str]:
        return self.fetch_indicator_availability(
            values=country_codes,
            frequency=frequency,
        ).available_codes

    def fetch_available_country_codes(
        self,
        indicator_codes: list[str],
        frequency: str,
    ) -> list[str]:
        return self.fetch_country_availability(
            values=indicator_codes,
            frequency=frequency,
        ).available_codes

    def fetch_indicator_availability(
        self,
        values: list[str],
        frequency: str,
        *,
        strict: bool = True,
    ) -> AvailabilityAggregate:
        return self._aggregate_availability(
            values=values,
            component_id="INDICATOR",
            key_builder=lambda country_code: f"{country_code}.*.{frequency}",
            strict=strict,
        )

    def fetch_country_availability(
        self,
        values: list[str],
        frequency: str,
        *,
        strict: bool = True,
    ) -> AvailabilityAggregate:
        return self._aggregate_availability(
            values=values,
            component_id="COUNTRY",
            key_builder=lambda indicator_code: f"*.{indicator_code}.{frequency}",
            strict=strict,
        )

    def fetch_dataframe(
        self,
        country_codes: list[str],
        indicator_codes: list[str],
        unit_codes: list[str],
        scale_codes: list[str],
        frequency: str,
        start_year: int | None,
        end_year: int | None,
        subject_labels: dict[str, str],
        country_labels: dict[str, str],
        unit_labels: dict[str, str],
        scale_labels: dict[str, str],
        indicator_unit_labels: dict[str, str],
        indicator_unit_codes: dict[str, str],
        indicator_scale_labels: dict[str, str],
    ) -> pd.DataFrame:
        csv_frame = self._fetch_batched_dataframe(
            country_codes=country_codes,
            indicator_codes=indicator_codes,
            frequency=frequency,
        )
        release = self.fetch_catalog().release
        dataframe = _build_dataframe(
            csv_frame=csv_frame,
            dataset_version=release.version,
            country_labels=country_labels,
            subject_labels=subject_labels,
            unit_labels=unit_labels,
            scale_labels=scale_labels,
            indicator_unit_labels=indicator_unit_labels,
            indicator_unit_codes=indicator_unit_codes,
            indicator_scale_labels=indicator_scale_labels,
        )
        if start_year is not None:
            dataframe = dataframe[dataframe["time_period"] >= start_year]
        if end_year is not None:
            dataframe = dataframe[dataframe["time_period"] <= end_year]
        return dataframe.reset_index(drop=True)

    def _fetch_batched_dataframe(
        self,
        *,
        country_codes: list[str],
        indicator_codes: list[str],
        frequency: str,
    ) -> pd.DataFrame:
        pending: list[tuple[list[str], list[str]]] = [(list(country_codes), list(indicator_codes))]
        frames: list[pd.DataFrame] = []

        while pending:
            batch_countries, batch_indicators = pending.pop(0)
            key = self._build_key(batch_countries, batch_indicators, frequency)
            if len(key) > MAX_DATA_QUERY_KEY_LENGTH and (len(batch_countries) > 1 or len(batch_indicators) > 1):
                pending = self._split_data_request(batch_countries, batch_indicators) + pending
                continue

            query = DataQuery(
                context=DataContext.DATAFLOW,
                agency_id="IMF.RES",
                resource_id="WEO",
                version="+",
                key=key,
                components=None,
                obs_dimension="TIME_PERIOD",
                attributes="dsd",
            )
            try:
                frame = _read_sdmx_dataframe(self._service.data(query))
            except Invalid as exc:
                if _is_invalid_url_error(exc) and (len(batch_countries) > 1 or len(batch_indicators) > 1):
                    pending = self._split_data_request(batch_countries, batch_indicators) + pending
                    continue
                raise
            if not frame.empty:
                frames.append(frame)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    def _aggregate_availability(
        self,
        *,
        values: list[str],
        component_id: str,
        key_builder: Callable[[str], str],
        strict: bool,
    ) -> AvailabilityAggregate:
        if not values:
            return AvailabilityAggregate(results=[], available_codes=[], common_codes=[], counts_by_code={})

        results_by_value: dict[str, AvailabilityResult] = {}
        max_workers = min(MAX_AVAILABILITY_WORKERS, len(values))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_availability_result,
                    requested_code=value,
                    component_id=component_id,
                    key=key_builder(value),
                    strict=strict,
                ): value
                for value in values
            }
            for future in as_completed(futures):
                result = future.result()
                results_by_value[result.requested_code] = result

        ordered_results = [results_by_value[value] for value in values]
        successful_results = [result for result in ordered_results if result.error_message is None]

        counts_by_code: dict[str, int] = {}
        common_codes: set[str] | None = None
        for result in successful_results:
            current_codes = set(result.available_codes)
            for code in current_codes:
                counts_by_code[code] = counts_by_code.get(code, 0) + 1
            if common_codes is None:
                common_codes = current_codes
            else:
                common_codes &= current_codes

        available_codes = sorted(counts_by_code)
        return AvailabilityAggregate(
            results=ordered_results,
            available_codes=available_codes,
            common_codes=sorted(common_codes or set()),
            counts_by_code=dict(sorted(counts_by_code.items())),
        )

    def _fetch_availability_result(
        self,
        *,
        requested_code: str,
        component_id: str,
        key: str,
        strict: bool,
    ) -> AvailabilityResult:
        query = AvailabilityQuery(
            context=DataContext.DATAFLOW,
            agency_id="IMF.RES",
            resource_id="WEO",
            version="+",
            key=key,
            component_id=component_id,
            mode=AvailabilityMode.AVAILABLE,
        )
        try:
            payload = self._fetch_availability_json(query)
        except Invalid as exc:
            message = _short_invalid_message(exc)
            if strict:
                raise AvailabilityLookupError(f"Availability lookup failed for {requested_code}: {message}") from exc
            return AvailabilityResult(
                requested_code=requested_code,
                available_codes=[],
                series_count=0,
                error_message=message,
            )
        except Exception as exc:
            if strict:
                raise AvailabilityLookupError(
                    f"Availability lookup failed for {requested_code}: {exc}"
                ) from exc
            return AvailabilityResult(
                requested_code=requested_code,
                available_codes=[],
                series_count=0,
                error_message=str(exc),
            )

        return AvailabilityResult(
            requested_code=requested_code,
            available_codes=sorted(set(_extract_constraint_values(payload, component_id))),
            series_count=_extract_series_count(payload),
            error_message=None,
        )

    def _fetch_batched_available_codes(self, *, component_id: str, key: str) -> list[str]:
        query = AvailabilityQuery(
            context=DataContext.DATAFLOW,
            agency_id="IMF.RES",
            resource_id="WEO",
            version="+",
            key=key,
            component_id=component_id,
            mode=AvailabilityMode.AVAILABLE,
        )
        try:
            payload = self._fetch_availability_json(query)
        except Invalid as exc:
            message = _short_invalid_message(exc)
            raise AvailabilityLookupError(f"Availability lookup failed: {message}") from exc
        return sorted(set(_extract_constraint_values(payload, component_id)))

    def _fetch_codelist(self, agency: str, resource_id: str) -> dict[str, str]:
        payload = self._fetch_structure_json(
            StructureQuery(
                artefact_type=StructureType.CODELIST,
                agency_id=agency,
                resource_id=resource_id,
                version="+",
                item_id="*",
                detail=StructureDetail.FULL,
            )
        )
        codes = payload["data"]["codelists"][0].get("codes", [])
        return {item["id"]: item["name"] for item in codes}

    def _fetch_structure_json(self, query: StructureQuery) -> dict[str, Any]:
        return json.loads(self._service.structure(query).decode("utf-8"))

    def _fetch_availability_json(self, query: AvailabilityQuery) -> dict[str, Any]:
        return json.loads(self._service.availability(query).decode("utf-8"))

    @staticmethod
    def _build_key(country_codes: list[str], indicator_codes: list[str], frequency: str) -> str:
        return ".".join(
            [
                "+".join(country_codes) if country_codes else "*",
                "+".join(indicator_codes) if indicator_codes else "*",
                frequency,
            ]
        )

    @staticmethod
    def _split_data_request(
        country_codes: list[str],
        indicator_codes: list[str],
    ) -> list[tuple[list[str], list[str]]]:
        country_length = _joined_key_values_length(country_codes)
        indicator_length = _joined_key_values_length(indicator_codes)

        if len(country_codes) > 1 and (country_length >= indicator_length or len(indicator_codes) <= 1):
            midpoint = len(country_codes) // 2
            return [
                (country_codes[:midpoint], indicator_codes),
                (country_codes[midpoint:], indicator_codes),
            ]
        if len(indicator_codes) > 1:
            midpoint = len(indicator_codes) // 2
            return [
                (country_codes, indicator_codes[:midpoint]),
                (country_codes, indicator_codes[midpoint:]),
            ]
        return [(country_codes, indicator_codes)]


def _extract_constraint_values(payload: dict[str, Any], component_id: str) -> list[str]:
    constraints = payload.get("data", {}).get("dataConstraints", [])
    if not constraints:
        return []
    for region in constraints[0].get("cubeRegions", []):
        for component in region.get("components", []):
            if component.get("id") == component_id:
                return [value["value"] for value in component.get("values", [])]
    return []


def _split_weo_locations(locations: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    countries: dict[str, str] = {}
    country_groups: dict[str, str] = {}
    for code, label in locations.items():
        if COUNTRY_CODE_PATTERN.fullmatch(code):
            countries[code] = label
        else:
            country_groups[code] = label
    return countries, country_groups


def _extract_series_count(payload: dict[str, Any]) -> int:
    constraints = payload.get("data", {}).get("dataConstraints", [])
    if not constraints:
        return 0
    for annotation in constraints[0].get("annotations", []):
        if annotation.get("id") == "series_count":
            try:
                return int(annotation.get("title", 0) or 0)
            except (TypeError, ValueError):
                return 0
    return 0


def _short_invalid_message(exc: Invalid) -> str:
    message = str(exc)
    marker = "The error message was:"
    if marker in message:
        raw_payload = message.split(marker, 1)[1].strip().strip("`")
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            pass
        else:
            if payload.get("message"):
                return str(payload["message"])
    if ". The query was" in message:
        return message.split(". The query was", 1)[0]
    return message


def _is_invalid_url_error(exc: Invalid) -> bool:
    message = str(exc).lower()
    return "invalid url" in message or "request url is invalid" in message


def _joined_key_values_length(values: list[str]) -> int:
    if not values:
        return 1
    return sum(len(value) for value in values) + max(0, len(values) - 1)


def _read_sdmx_dataframe(raw_payload: bytes | str) -> pd.DataFrame:
    raw_text = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else raw_payload
    normalized = raw_text.replace("STRUCTURE[;]", "STRUCTURE", 1)
    message = read_sdmx(normalized, validate=False)

    frames: list[pd.DataFrame] = []
    for dataset in getattr(message, "data", []):
        frame = getattr(dataset, "data", None)
        if isinstance(frame, pd.DataFrame):
            frames.append(frame.copy())

    if not frames:
        return pd.DataFrame()

    dataframe = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    dataframe.columns = [str(column).replace("[;]", "").strip() for column in dataframe.columns]
    dataframe = dataframe.dropna(axis=1, how="all")
    return dataframe


def _build_dataframe(
    csv_frame: pd.DataFrame,
    dataset_version: str,
    country_labels: dict[str, str],
    subject_labels: dict[str, str],
    unit_labels: dict[str, str],
    scale_labels: dict[str, str],
    indicator_unit_labels: dict[str, str],
    indicator_unit_codes: dict[str, str],
    indicator_scale_labels: dict[str, str],
) -> pd.DataFrame:
    columns = [
        "dataset_version",
        "country_code",
        "country",
        "indicator_code",
        "subject_descriptor",
        "unit_code",
        "units",
        "scale_code",
        "scale",
        "frequency",
        "time_period",
        "obs_value",
        "country_update_date",
    ]
    if csv_frame.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    for row in csv_frame.itertuples(index=False):
        row_dict = row._asdict()
        indicator_code = row_dict["INDICATOR"]
        unit_code = indicator_unit_codes.get(indicator_code, "")
        scale_code = str(row_dict.get("SCALE", "") or "")
        rows.append(
            {
                "dataset_version": dataset_version,
                "country_code": row_dict["COUNTRY"],
                "country": country_labels[row_dict["COUNTRY"]],
                "indicator_code": indicator_code,
                "subject_descriptor": subject_labels[indicator_code],
                "unit_code": unit_code,
                "units": unit_labels.get(unit_code, indicator_unit_labels.get(indicator_code, "")),
                "scale_code": scale_code,
                "scale": scale_labels.get(scale_code, indicator_scale_labels.get(indicator_code, scale_code)),
                "frequency": row_dict["FREQUENCY"],
                "time_period": int(row_dict["TIME_PERIOD"]),
                "obs_value": _coerce_value(row_dict["OBS_VALUE"]),
                "country_update_date": str(row_dict.get("COUNTRY_UPDATE_DATE", "") or ""),
            }
        )

    return pd.DataFrame(rows, columns=columns).sort_values(
        ["country", "subject_descriptor", "units", "scale", "time_period"],
        ignore_index=True,
    )


def _coerce_value(value: Any) -> Any:
    if value in (None, "", "n/a", "N/A"):
        return pd.NA
    try:
        return float(value)
    except (TypeError, ValueError):
        return pd.NA
