from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

from weo_tools.legacy import load_legacy_catalog, normalize_label


def test_load_legacy_catalog_reads_old_selector_labels(tmp_path: Path) -> None:
    workbook_path = tmp_path / "legacy.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["WEO Country Code", "ISO", "WEO Subject Code", "Country", "Subject Descriptor", "Units", "Scale"])
    sheet.append(["112", "AUT", "NGDPD", "Austria", "Gross domestic product, current prices", "U.S. dollars", "Billions"])
    workbook.save(workbook_path)

    catalog = load_legacy_catalog(workbook_path)

    assert catalog.preferred_country_labels["AUT"] == "Austria"
    assert catalog.preferred_subject_labels["NGDPD"] == "Gross domestic product, current prices"
    assert "Austria" in catalog.country_aliases["AUT"]
    assert normalize_label("U.S. dollars") == "u s dollars"
