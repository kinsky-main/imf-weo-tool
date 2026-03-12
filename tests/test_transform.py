from __future__ import annotations

import pandas as pd

from weo_tools.app import enrich_for_legacy_columns, pivot_for_excel
from weo_tools.imf import _build_dataframe


def test_build_dataframe_normalizes_sdmx_response() -> None:
    csv_frame = pd.DataFrame(
        [
            {
                "COUNTRY": "GBR",
                "INDICATOR": "NGDPD",
                "FREQUENCY": "A",
                "TIME_PERIOD": "2024",
                "OBS_VALUE": "3644636000000",
                "SCALE": "9",
                "COUNTRY_UPDATE_DATE": "9/13/2025",
            },
            {
                "COUNTRY": "GBR",
                "INDICATOR": "NGDPD",
                "FREQUENCY": "A",
                "TIME_PERIOD": "2025",
                "OBS_VALUE": "3958780000000",
                "SCALE": "9",
                "COUNTRY_UPDATE_DATE": "9/13/2025",
            },
        ]
    )

    dataframe = _build_dataframe(
        csv_frame=csv_frame,
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
