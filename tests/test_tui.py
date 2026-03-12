from __future__ import annotations

from weo_tools.tui import Choice, SearchableMultiSelect


def test_searchable_multiselect_filters_by_typed_terms() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
            Choice(name="United States [USA]", value="USA"),
            Choice(name="Austria [AUT]", value="AUT"),
        ],
    )
    prompt.search.text = "united kin"

    assert prompt._filtered_indexes() == [0]


def test_searchable_multiselect_preserves_selected_values() -> None:
    prompt = SearchableMultiSelect(
        "Select units",
        [
            Choice(name="U.S. dollars", value="usd", checked=True),
            Choice(name="National currency", value="xdc"),
        ],
        required=False,
    )

    assert prompt._selected_values() == ["usd"]
