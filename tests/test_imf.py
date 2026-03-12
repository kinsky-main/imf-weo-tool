from __future__ import annotations

from weo_tools.imf import ImfWeoClient


def _availability_payload(component_id: str, values: list[str]) -> dict[str, object]:
    return {
        "data": {
            "dataConstraints": [
                {
                    "cubeRegions": [
                        {
                            "components": [
                                {
                                    "id": component_id,
                                    "values": [{"value": value} for value in values],
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }


def test_fetch_available_country_codes_intersects_ref_area_values(monkeypatch) -> None:
    client = ImfWeoClient()
    payloads = {
        "*.NGDPD.A": _availability_payload("REF_AREA", ["GBR", "USA"]),
        "*.PCPIPCH.A": _availability_payload("REF_AREA", ["GBR", "AUT"]),
    }

    monkeypatch.setattr(client, "_fetch_availability_json", lambda query: payloads[query.key])

    result = client.fetch_available_country_codes(["NGDPD", "PCPIPCH"], "A")

    assert result == ["GBR"]
