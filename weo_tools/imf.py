from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd


API_BASE = "https://api.imf.org/external/sdmx/3.0"


@dataclass(slots=True)
class ReleaseInfo:
    version: str
    updated_at: str
    name: str


@dataclass(slots=True)
class Catalog:
    release: ReleaseInfo
    countries: dict[str, str]
    indicators: dict[str, str]
    units: dict[str, str]
    scales: dict[str, str]


class ImfWeoClient:
    def __init__(self) -> None:
        self._catalog: Catalog | None = None

    def fetch_catalog(self) -> Catalog:
        if self._catalog is not None:
            return self._catalog

        dataflow = self._fetch_json("/structure/dataflow/IMF.RES/WEO/+?detail=full")
        flow = dataflow["data"]["dataflows"][0]
        updated_at = ""
        for annotation in flow.get("annotations", []):
            if annotation.get("id") == "lastUpdatedAt":
                updated_at = annotation.get("value", "")
                break

        release = ReleaseInfo(version=flow["version"], updated_at=updated_at, name=flow["name"])
        countries = self._fetch_codelist("IMF.RES", "CL_WEO_COUNTRY")
        indicators = self._fetch_codelist("IMF.RES", "CL_WEO_INDICATOR")
        units = self._fetch_codelist("IMF", "CL_UNIT")
        scales = self._fetch_codelist("IMF", "CL_UNIT_MULT")

        self._catalog = Catalog(
            release=release,
            countries=countries,
            indicators=indicators,
            units=units,
            scales=scales,
        )
        return self._catalog

    def fetch_available_attributes(
        self,
        country_codes: list[str],
        indicator_codes: list[str],
        frequency: str,
    ) -> list[dict[str, str]]:
        preview = self._fetch_data_response(
            country_codes=country_codes,
            indicator_codes=indicator_codes,
            frequency=frequency,
            unit_codes=[],
            scale_codes=[],
            start_year=None,
            end_year=None,
            preview_only=True,
        )
        return _extract_series_metadata(preview)

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
        response = self._fetch_data_response(
            country_codes=country_codes,
            indicator_codes=indicator_codes,
            frequency=frequency,
            unit_codes=unit_codes,
            scale_codes=scale_codes,
            start_year=None,
            end_year=None,
            preview_only=False,
        )
        release = self.fetch_catalog().release
        dataframe = _build_dataframe(
            response=response,
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

    def fetch_indicator_unit_codes(
        self,
        country_code: str,
        indicator_codes: list[str],
        frequency: str,
    ) -> dict[str, str]:
        codes: dict[str, str] = {}
        for indicator_code in indicator_codes:
            preview = self._fetch_data_response(
                country_codes=[country_code],
                indicator_codes=[indicator_code],
                frequency=frequency,
                unit_codes=[],
                scale_codes=[],
                start_year=None,
                end_year=None,
                preview_only=True,
            )
            rows = _extract_series_metadata(preview)
            codes[indicator_code] = rows[0]["unit_code"] if rows else ""
        return codes

    def _fetch_codelist(self, agency: str, resource_id: str) -> dict[str, str]:
        payload = self._fetch_json(f"/structure/codelist/{agency}/{resource_id}/+/*?detail=full")
        codes = payload["data"]["codelists"][0].get("codes", [])
        return {item["id"]: item["name"] for item in codes}

    def _fetch_data_response(
        self,
        country_codes: list[str],
        indicator_codes: list[str],
        frequency: str,
        unit_codes: list[str],
        scale_codes: list[str],
        start_year: int | None,
        end_year: int | None,
        preview_only: bool,
    ) -> dict[str, Any]:
        key = ".".join(
            [
                "+".join(country_codes) if country_codes else "*",
                "+".join(indicator_codes) if indicator_codes else "*",
                frequency,
            ]
        )

        params: list[tuple[str, str]] = [("dimensionAtObservation", "TIME_PERIOD"), ("attributes", "dsd")]
        if preview_only:
            params.append(("firstNObservations", "1"))
        if unit_codes:
            params.append(("c[UNIT]", "+".join(unit_codes)))
        if scale_codes:
            params.append(("c[SCALE]", "+".join(scale_codes)))
        query = urlencode(params, quote_via=quote)
        path = f"/data/dataflow/IMF.RES/WEO/+/{key}?{query}"
        return self._fetch_json(path)

    def _fetch_json(self, path: str) -> dict[str, Any]:
        url = f"{API_BASE}{path}"
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "weo-tools/1.0"})
        with urlopen(request, timeout=60) as response:
            return json.load(response)


def _extract_series_metadata(response: dict[str, Any]) -> list[dict[str, str]]:
    structure = response["data"]["structures"][0]
    data_set = response["data"]["dataSets"][0]

    series_dimensions = structure["dimensions"]["series"]
    series_attr_defs = structure["attributes"]["series"]
    group_attr_defs = structure["attributes"].get("dimensionGroup", [])

    unit_attr_index = _attribute_definition_index(group_attr_defs, "UNIT")
    scale_attr_index = _attribute_definition_index(series_attr_defs, "SCALE")
    country_update_index = _attribute_definition_index(series_attr_defs, "COUNTRY_UPDATE_DATE")

    unit_codes_by_indicator: dict[int, str] = {}
    for group_key, values in data_set.get("dimensionGroupAttributes", {}).items():
        indicator_idx = _indicator_index_from_group_key(group_key)
        if indicator_idx is None or unit_attr_index is None:
            continue
        value_index = values[unit_attr_index]
        if value_index is None:
            continue
        unit_codes_by_indicator[indicator_idx] = group_attr_defs[unit_attr_index]["values"][value_index]["id"]

    series_rows: list[dict[str, str]] = []
    for series_key, series_value in data_set.get("series", {}).items():
        key_indexes = [int(part) for part in series_key.split(":") if part != ""]
        country_idx, indicator_idx, frequency_idx = key_indexes
        scale_code = ""
        country_update_date = ""
        attributes = series_value.get("attributes", [])
        if scale_attr_index is not None and scale_attr_index < len(attributes):
            scale_index = attributes[scale_attr_index]
            if scale_index is not None:
                scale_code = series_attr_defs[scale_attr_index]["values"][scale_index]["id"]
        if country_update_index is not None and country_update_index < len(attributes):
            country_update_date = attributes[country_update_index] or ""

        series_rows.append(
            {
                "country_code": series_dimensions[0]["values"][country_idx]["id"],
                "indicator_code": series_dimensions[1]["values"][indicator_idx]["id"],
                "frequency": series_dimensions[2]["values"][frequency_idx]["id"],
                "unit_code": unit_codes_by_indicator.get(indicator_idx, ""),
                "scale_code": scale_code,
                "country_update_date": country_update_date,
            }
        )

    return series_rows


def _build_dataframe(
    response: dict[str, Any],
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
    structure = response["data"]["structures"][0]
    data_set = response["data"]["dataSets"][0]
    series_meta = _extract_series_metadata(response)
    series_lookup = {
        (item["country_code"], item["indicator_code"], item["frequency"]): item
        for item in series_meta
    }

    series_dimensions = structure["dimensions"]["series"]
    time_dimension = structure["dimensions"]["observation"][0]["values"]

    rows: list[dict[str, Any]] = []
    for series_key, series_value in data_set.get("series", {}).items():
        key_indexes = [int(part) for part in series_key.split(":") if part != ""]
        country_code = series_dimensions[0]["values"][key_indexes[0]]["id"]
        indicator_code = series_dimensions[1]["values"][key_indexes[1]]["id"]
        frequency = series_dimensions[2]["values"][key_indexes[2]]["id"]
        meta = series_lookup[(country_code, indicator_code, frequency)]

        for observation_key, observation_value in series_value.get("observations", {}).items():
            time_entry = time_dimension[int(observation_key)]
            time_period = int(time_entry.get("id", time_entry.get("value")))
            raw_value = observation_value[0] if observation_value else None
            rows.append(
                {
                    "dataset_version": dataset_version,
                    "country_code": country_code,
                    "country": country_labels[country_code],
                    "indicator_code": indicator_code,
                    "subject_descriptor": subject_labels[indicator_code],
                    "unit_code": meta["unit_code"] or indicator_unit_codes.get(indicator_code, ""),
                    "units": unit_labels.get(
                        meta["unit_code"] or indicator_unit_codes.get(indicator_code, ""),
                        indicator_unit_labels.get(indicator_code, meta["unit_code"]),
                    ),
                    "scale_code": meta["scale_code"],
                    "scale": scale_labels.get(meta["scale_code"], indicator_scale_labels.get(indicator_code, meta["scale_code"])),
                    "frequency": frequency,
                    "time_period": time_period,
                    "obs_value": _coerce_value(raw_value),
                    "country_update_date": meta["country_update_date"],
                }
            )

    dataframe = pd.DataFrame(rows, columns=columns)
    if dataframe.empty:
        return dataframe
    return dataframe.sort_values(
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


def _attribute_definition_index(definitions: list[dict[str, Any]], attribute_id: str) -> int | None:
    for index, definition in enumerate(definitions):
        if definition["id"] == attribute_id:
            return index
    return None


def _indicator_index_from_group_key(key: str) -> int | None:
    parts = key.split(":")
    if len(parts) < 2 or not parts[1]:
        return None
    return int(parts[1])
