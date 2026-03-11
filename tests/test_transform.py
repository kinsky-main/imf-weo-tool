from __future__ import annotations

import pandas as pd

from weo_tools.app import enrich_for_legacy_columns, pivot_for_excel
from weo_tools.imf import _build_dataframe


def test_build_dataframe_normalizes_sdmx_response() -> None:
    response = {
        "data": {
            "dataSets": [
                {
                    "series": {
                        "0:0:0": {
                            "attributes": [0, 0, 0, "9/13/2025"],
                            "observations": {"0": ["3644636000000"], "1": ["3958780000000"]},
                        }
                    },
                    "dimensionGroupAttributes": {
                        ":0::": [None, None, 0, None, None, None, None, None, None, None, None, 0, None, None, None, 0, None]
                    },
                }
            ],
            "structures": [
                {
                    "dimensions": {
                        "series": [
                            {"id": "COUNTRY", "values": [{"id": "GBR"}]},
                            {"id": "INDICATOR", "values": [{"id": "NGDPD"}]},
                            {"id": "FREQUENCY", "values": [{"id": "A"}]},
                        ],
                        "observation": [
                            {"id": "TIME_PERIOD", "values": [{"id": "2024"}, {"id": "2025"}]}
                        ],
                    },
                    "attributes": {
                        "series": [
                            {"id": "SCALE", "values": [{"id": "9"}]},
                            {"id": "DECIMALS_DISPLAYED", "values": [{"id": "3"}]},
                            {"id": "OVERLAP", "values": [{"id": "OL"}]},
                            {"id": "COUNTRY_UPDATE_DATE", "values": []},
                        ],
                        "dimensionGroup": [
                            {"id": "FUNCTIONAL_CAT", "values": []},
                            {"id": "INT_ACC_ITEM", "values": []},
                            {"id": "NA_STO", "values": [{"id": "B1GQ"}]},
                            {"id": "GFS_STO", "values": []},
                            {"id": "COICOP_1999", "values": []},
                            {"id": "TRADE_FLOW", "values": []},
                            {"id": "COMMODITY", "values": []},
                            {"id": "SOC_CONCEPTS", "values": []},
                            {"id": "SECTOR", "values": []},
                            {"id": "ACCOUNTING_ENTRY", "values": []},
                            {"id": "INDEX_TYPE", "values": []},
                            {"id": "PRICES", "values": [{"id": "V"}]},
                            {"id": "STATISTICAL_MEASURES", "values": []},
                            {"id": "EXRATE", "values": []},
                            {"id": "TRANSFORMATION", "values": []},
                            {"id": "UNIT", "values": [{"id": "USD"}]},
                            {"id": "REPORTING_PERIOD_TYPE", "values": []},
                        ],
                    },
                }
            ],
        }
    }

    dataframe = _build_dataframe(
        response=response,
        dataset_version="9.0.0",
        country_labels={"GBR": "United Kingdom"},
        subject_labels={"NGDPD": "Gross domestic product, current prices"},
        unit_labels={"USD": "U.S. dollars"},
        scale_labels={"9": "Billions"},
        indicator_unit_labels={"NGDPD": "U.S. dollars"},
        indicator_unit_codes={"NGDPD": "USD"},
        indicator_scale_labels={"NGDPD": "Billions"},
    )
    dataframe = enrich_for_legacy_columns(dataframe)

    assert list(dataframe["Country"].unique()) == ["United Kingdom"]
    assert list(dataframe["Subject Descriptor"].unique()) == ["Gross domestic product, current prices"]
    assert list(dataframe["Units"].unique()) == ["U.S. dollars"]
    assert list(dataframe["Scale"].unique()) == ["Billions"]
    assert dataframe["obs_value"].tolist() == [3644636000000.0, 3958780000000.0]


def test_pivot_for_excel_creates_wide_year_columns() -> None:
    dataframe = pd.DataFrame(
        [
            {
                "Country": "United Kingdom",
                "Subject Descriptor": "Gross domestic product, current prices",
                "Units": "U.S. dollars",
                "Scale": "Billions",
                "time_period": 2024,
                "obs_value": 3644.636,
            },
            {
                "Country": "United Kingdom",
                "Subject Descriptor": "Gross domestic product, current prices",
                "Units": "U.S. dollars",
                "Scale": "Billions",
                "time_period": 2025,
                "obs_value": 3958.78,
            },
        ]
    )

    wide = pivot_for_excel(dataframe)

    assert list(wide.columns) == ["Country", "Subject Descriptor", "Units", "Scale", "2024", "2025"]
    assert wide.iloc[0]["2024"] == 3644.636
