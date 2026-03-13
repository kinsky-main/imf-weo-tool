from __future__ import annotations

import pandas as pd
from pysdmx.errors import Invalid

from weo_tools.imf import (
    AvailabilityAggregate,
    AvailabilityLookupError,
    AvailabilityResult,
    ImfWeoClient,
    _extract_series_count,
    _read_sdmx_dataframe,
    _split_weo_locations,
)


def _availability_payload(component_id: str, values: list[str], series_count: int) -> dict[str, object]:
    return {
        "data": {
            "dataConstraints": [
                {
                    "annotations": [{"id": "series_count", "title": str(series_count), "type": "sdmx_metrics"}],
                    "cubeRegions": [
                        {
                            "components": [
                                {
                                    "id": component_id,
                                    "values": [{"value": value} for value in values],
                                }
                            ]
                        }
                    ],
                }
            ]
        }
    }


def test_fetch_country_availability_uses_country_dimension(monkeypatch) -> None:
    client = ImfWeoClient()
    recorded_component_ids: list[str] = []
    payloads = {
        "*.NGDPD.A": _availability_payload("COUNTRY", ["GBR", "USA"], 2),
        "*.PCPIPCH.A": _availability_payload("COUNTRY", ["GBR", "AUT"], 2),
    }

    def fake_fetch(query):
        recorded_component_ids.append(query.component_id)
        return payloads[query.key]

    monkeypatch.setattr(client, "_fetch_availability_json", fake_fetch)

    result = client.fetch_country_availability(["NGDPD", "PCPIPCH"], "A")

    assert recorded_component_ids == ["COUNTRY", "COUNTRY"]
    assert result.available_codes == ["AUT", "GBR", "USA"]
    assert result.common_codes == ["GBR"]
    assert result.counts_by_code == {"AUT": 1, "GBR": 2, "USA": 1}


def test_split_weo_locations_separates_groups_from_country_codes() -> None:
    countries, country_groups = _split_weo_locations(
        {
            "AUT": "Austria",
            "GBR": "United Kingdom",
            "G001": "World",
            "GX123": "Other Advanced Economies",
            "GASEAN": "ASEAN",
        }
    )

    assert countries == {"AUT": "Austria", "GBR": "United Kingdom"}
    assert country_groups == {
        "G001": "World",
        "GX123": "Other Advanced Economies",
        "GASEAN": "ASEAN",
    }


def test_fetch_available_location_codes_uses_batched_country_query(monkeypatch) -> None:
    client = ImfWeoClient()
    recorded_keys: list[str] = []

    def fake_fetch(query):
        recorded_keys.append(query.key)
        return _availability_payload("COUNTRY", ["AUT", "GBR", "G001"], 3)

    monkeypatch.setattr(client, "_fetch_availability_json", fake_fetch)

    result = client.fetch_available_location_codes("A")

    assert result == ["AUT", "G001", "GBR"]
    assert recorded_keys == ["*.*.A"]


def test_fetch_available_indicator_catalog_codes_uses_batched_indicator_query(monkeypatch) -> None:
    client = ImfWeoClient()
    recorded_keys: list[str] = []

    def fake_fetch(query):
        recorded_keys.append(query.key)
        return _availability_payload("INDICATOR", ["NGDP", "NGDPD"], 2)

    monkeypatch.setattr(client, "_fetch_availability_json", fake_fetch)

    result = client.fetch_available_indicator_catalog_codes("A")

    assert result == ["NGDP", "NGDPD"]
    assert recorded_keys == ["*.*.A"]


def test_fetch_available_frequency_codes_uses_batched_frequency_query(monkeypatch) -> None:
    client = ImfWeoClient()
    recorded_keys: list[str] = []

    def fake_fetch(query):
        recorded_keys.append(query.key)
        return _availability_payload("FREQUENCY", ["A", "Q"], 2)

    monkeypatch.setattr(client, "_fetch_availability_json", fake_fetch)

    result = client.fetch_available_frequency_codes()

    assert result == ["A", "Q"]
    assert recorded_keys == ["*.*.*"]


def test_fetch_available_time_periods_reads_and_caches_sorted_years(monkeypatch) -> None:
    client = ImfWeoClient()
    calls: list[tuple[list[str], list[str], str]] = []

    def fake_fetch_batched_dataframe(*, country_codes, indicator_codes, frequency):
        calls.append((list(country_codes), list(indicator_codes), frequency))
        return pd.DataFrame({"TIME_PERIOD": ["2024", "2022", "2024", "bad", None]})

    monkeypatch.setattr(client, "_fetch_batched_dataframe", fake_fetch_batched_dataframe)

    first = client.fetch_available_time_periods(["GBR"], ["NGDPD"], "A")
    second = client.fetch_available_time_periods(["GBR"], ["NGDPD"], "A")

    assert first == [2022, 2024]
    assert second == [2022, 2024]
    assert calls == [(["GBR"], ["NGDPD"], "A")]


def test_extract_series_count_reads_annotation_title() -> None:
    payload = _availability_payload("COUNTRY", ["GBR"], 44)

    assert _extract_series_count(payload) == 44


def test_fetch_indicator_availability_aggregates_per_candidate_counts(monkeypatch) -> None:
    client = ImfWeoClient()
    payloads = {
        "GBR.*.A": _availability_payload("INDICATOR", ["NGDP", "NGDPD"], 2),
        "AUT.*.A": _availability_payload("INDICATOR", ["NGDP"], 1),
    }

    monkeypatch.setattr(client, "_fetch_availability_json", lambda query: payloads[query.key])

    result = client.fetch_indicator_availability(["GBR", "AUT"], "A")

    assert result.available_codes == ["NGDP", "NGDPD"]
    assert result.common_codes == ["NGDP"]
    assert result.counts_by_code == {"NGDP": 2, "NGDPD": 1}
    assert result.results == [
        AvailabilityResult(requested_code="GBR", available_codes=["NGDP", "NGDPD"], series_count=2),
        AvailabilityResult(requested_code="AUT", available_codes=["NGDP"], series_count=1),
    ]


def test_fetch_indicator_availability_can_tolerate_invalid_rows_when_not_strict(monkeypatch) -> None:
    client = ImfWeoClient()

    def fake_fetch(query):
        if query.key == "AUT.*.A":
            raise Invalid(
                "Client error 400: The query returned a 400 error code. "
                "The query was `ignored`. "
                'The error message was: `{"message":"Invalid availability selection"}`'
            )
        return _availability_payload("INDICATOR", ["NGDPD"], 1)

    monkeypatch.setattr(client, "_fetch_availability_json", fake_fetch)

    result = client.fetch_indicator_availability(["GBR", "AUT"], "A", strict=False)

    assert result.available_codes == ["NGDPD"]
    assert result.common_codes == ["NGDPD"]
    assert result.counts_by_code == {"NGDPD": 1}
    assert result.results[1].error_message == "Invalid availability selection"


def test_fetch_indicator_availability_raises_concise_error_when_strict(monkeypatch) -> None:
    client = ImfWeoClient()

    def fake_fetch(_query):
        raise Invalid(
            "Client error 400: The query returned a 400 error code. "
            "The query was `ignored`. "
            'The error message was: `{"message":"Invalid availability selection"}`'
        )

    monkeypatch.setattr(client, "_fetch_availability_json", fake_fetch)

    try:
        client.fetch_indicator_availability(["GBR"], "A")
    except AvailabilityLookupError as exc:
        assert str(exc) == "Availability lookup failed for GBR: Invalid availability selection"
    else:
        raise AssertionError("Expected AvailabilityLookupError")


def test_read_sdmx_dataframe_uses_reader_for_imf_csv() -> None:
    raw_csv = "\n".join(
        [
            "STRUCTURE[;],STRUCTURE_ID,ACTION,COUNTRY,INDICATOR,FREQUENCY,TIME_PERIOD,OBS_VALUE,SCALE,UNIT,COUNTRY_UPDATE_DATE",
            "dataflow,IMF.RES:WEO(9.0.0),R,GBR,NGDPD,A,2024,3644636000000,9,USD,9/13/2025",
        ]
    )

    frame = _read_sdmx_dataframe(raw_csv)

    assert list(frame.columns) == [
        "COUNTRY",
        "INDICATOR",
        "FREQUENCY",
        "TIME_PERIOD",
        "OBS_VALUE",
        "SCALE",
        "UNIT",
        "COUNTRY_UPDATE_DATE",
    ]
    assert frame.iloc[0]["COUNTRY"] == "GBR"
    assert frame.iloc[0]["OBS_VALUE"] == "3644636000000"


def test_fetch_dataframe_splits_invalid_url_batches_and_combines_results(monkeypatch) -> None:
    class FakeService:
        def __init__(self) -> None:
            self.keys: list[str] = []
            self.failed_keys: list[str] = []
            self.successful_keys: list[str] = []

        def data(self, query):
            self.keys.append(query.key)
            countries_key, indicators_key, frequency = query.key.split(".")
            countries = countries_key.split("+")
            indicators = indicators_key.split("+")
            if len(countries) > 1 and len(indicators) > 1:
                self.failed_keys.append(query.key)
                raise Invalid(
                    "Client error 400: The query returned a 400 error code. "
                    "The query was `ignored`. "
                    "The error message was: `<!DOCTYPE HTML><html><body>Bad Request - Invalid URL</body></html>`"
                )
            self.successful_keys.append(query.key)

            rows = [
                "STRUCTURE[;],STRUCTURE_ID,ACTION,COUNTRY,INDICATOR,FREQUENCY,TIME_PERIOD,OBS_VALUE,SCALE,UNIT,COUNTRY_UPDATE_DATE"
            ]
            for country in countries:
                for indicator in indicators:
                    rows.append(
                        ",".join(
                            [
                                "dataflow",
                                "IMF.RES:WEO(9.0.0)",
                                "R",
                                country,
                                indicator,
                                frequency,
                                "2024",
                                "1",
                                "9",
                                "USD",
                                "9/13/2025",
                            ]
                        )
                    )
            return "\n".join(rows)

    client = ImfWeoClient()
    service = FakeService()
    monkeypatch.setattr(client, "_service", service)
    monkeypatch.setattr(
        client,
        "fetch_catalog",
        lambda: type("CatalogStub", (), {"release": type("ReleaseStub", (), {"version": "9.0.0"})()})(),
    )

    dataframe = client.fetch_dataframe(
        country_codes=["AUT", "GBR", "USA"],
        indicator_codes=["NGDP", "NGDPD"],
        unit_codes=["USD"],
        scale_codes=["9"],
        frequency="A",
        start_year=None,
        end_year=None,
        subject_labels={
            "NGDP": "Gross domestic product, volume",
            "NGDPD": "Gross domestic product, current prices",
        },
        country_labels={
            "AUT": "Austria",
            "GBR": "United Kingdom",
            "USA": "United States",
        },
        unit_labels={"USD": "U.S. dollars"},
        scale_labels={"9": "Billions"},
        indicator_unit_labels={
            "NGDP": "U.S. dollars",
            "NGDPD": "U.S. dollars",
        },
        indicator_unit_codes={
            "NGDP": "USD",
            "NGDPD": "USD",
        },
        indicator_scale_labels={
            "NGDP": "Billions",
            "NGDPD": "Billions",
        },
    )

    assert len(service.keys) > 1
    assert service.keys[0] == "AUT+GBR+USA.NGDP+NGDPD.A"
    assert service.failed_keys
    assert service.successful_keys
    assert all(
        len(key.split(".")[0].split("+")) == 1 or len(key.split(".")[1].split("+")) == 1
        for key in service.successful_keys
    )
    assert isinstance(dataframe, pd.DataFrame)
    assert len(dataframe) == 6
    assert set(dataframe["country_code"]) == {"AUT", "GBR", "USA"}
    assert set(dataframe["indicator_code"]) == {"NGDP", "NGDPD"}
