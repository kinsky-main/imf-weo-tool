from __future__ import annotations

from dataclasses import dataclass
import json
from io import StringIO
from typing import Any

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
        self._service = RestService(
            API_BASE,
            ApiVersion.V2_0_0,
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

    def fetch_available_indicator_codes(
        self,
        country_codes: list[str],
        frequency: str,
    ) -> list[str]:
        common_codes: set[str] | None = None
        for country_code in country_codes:
            query = AvailabilityQuery(
                context=DataContext.DATAFLOW,
                agency_id="IMF.RES",
                resource_id="WEO",
                version="+",
                key=f"{country_code}.*.{frequency}",
                component_id="INDICATOR",
                mode=AvailabilityMode.AVAILABLE,
            )
            payload = self._fetch_availability_json(query)
            values = _extract_constraint_values(payload, "INDICATOR")
            if common_codes is None:
                common_codes = set(values)
            else:
                common_codes &= set(values)

        return sorted(common_codes or set())

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
        query = DataQuery(
            context=DataContext.DATAFLOW,
            agency_id="IMF.RES",
            resource_id="WEO",
            version="+",
            key=self._build_key(country_codes, indicator_codes, frequency),
            components=None,
            obs_dimension="TIME_PERIOD",
            attributes="dsd",
        )
        raw_csv = self._service.data(query).decode("utf-8")
        csv_frame = _read_sdmx_csv(raw_csv)
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


def _extract_constraint_values(payload: dict[str, Any], component_id: str) -> list[str]:
    constraints = payload.get("data", {}).get("dataConstraints", [])
    if not constraints:
        return []
    for region in constraints[0].get("cubeRegions", []):
        for component in region.get("components", []):
            if component.get("id") == component_id:
                return [value["value"] for value in component.get("values", [])]
    return []


def _read_sdmx_csv(raw_csv: str) -> pd.DataFrame:
    dataframe = pd.read_csv(StringIO(raw_csv), keep_default_na=False)
    dataframe.columns = [column.replace("[;]", "").strip() for column in dataframe.columns]
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
