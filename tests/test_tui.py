from __future__ import annotations

from prompt_toolkit.layout.containers import ConditionalContainer
from prompt_toolkit.widgets import Frame
from prompt_toolkit.data_structures import Point
from prompt_toolkit.mouse_events import MouseButton, MouseEvent, MouseEventType

from weo_tools.tui import Choice, SearchableMultiSelect


def _split_rows(fragments: list[tuple[object, ...]]) -> list[list[tuple[object, ...]]]:
    rows: list[list[tuple[object, ...]]] = []
    current: list[tuple[object, ...]] = []
    for fragment in fragments:
        current.append(fragment)
        if "\n" in str(fragment[1]):
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    return rows


def _row_handler(prompt: SearchableMultiSelect, row_index: int):
    remaining = row_index
    for row in _split_rows(prompt._render_choices()):
        if remaining == 0:
            for fragment in row:
                if len(fragment) >= 3:
                    return fragment[2]
            break
        remaining -= 1
    raise AssertionError(f"No handler found for row {row_index}")


def _mouse_event(event_type: MouseEventType, y: int, button: MouseButton) -> MouseEvent:
    return MouseEvent(
        position=Point(x=0, y=y),
        event_type=event_type,
        button=button,
        modifiers=frozenset(),
    )


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


def test_searchable_multiselect_search_field_is_click_focusable() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
        ],
    )

    assert prompt.search_focus_on_click is True
    assert isinstance(prompt.search_frame, Frame)


def test_searchable_multiselect_instructions_include_visible_bulk_actions() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
            Choice(name="Austria [AUT]", value="AUT"),
        ],
    )

    assert "Ctrl-A to select visible" in prompt._instructions()
    assert "Ctrl-D to clear visible" in prompt._instructions()


def test_searchable_multiselect_single_choice_replaces_previous_selection() -> None:
    prompt = SearchableMultiSelect(
        "Choose selection order",
        [
            Choice(name="Country first", value="country-first"),
            Choice(name="Indicator first", value="indicator-first"),
        ],
        max_selections=1,
    )

    prompt._toggle_current()
    prompt._move(1)
    prompt._toggle_current()

    assert prompt._selected_values() == ["indicator-first"]


def test_render_choices_marks_active_row_for_scrolling() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
            Choice(name="United States [USA]", value="USA"),
        ],
    )
    prompt.cursor = 1

    rows = _split_rows(prompt._render_choices())

    assert any(fragment[0] == "[SetCursorPosition]" for fragment in rows[1])
    assert any(fragment[0] == "class:active" for fragment in rows[1] if fragment[1])


def test_searchable_multiselect_builds_named_layout_sections() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
        ],
    )
    root = prompt._build_root_container()

    assert root.children[0] is prompt.title_window
    assert root.children[-1] is prompt.error_window
    assert isinstance(prompt.matches_frame, Frame)
    assert isinstance(prompt.error_window, ConditionalContainer)


def test_render_choices_includes_detail_suffix() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR", detail="2/3 subjects"),
        ],
    )

    fragments = prompt._render_choices()

    assert any("2/3 subjects" in str(fragment[1]) for fragment in fragments)


def test_select_visible_only_selects_filtered_rows() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
            Choice(name="United States [USA]", value="USA"),
            Choice(name="Austria [AUT]", value="AUT"),
        ],
        required=False,
    )
    prompt.search.text = "united"

    prompt._select_visible()

    assert prompt._selected_values() == ["GBR", "USA"]


def test_clear_visible_only_clears_filtered_rows() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR", checked=True),
            Choice(name="United States [USA]", value="USA", checked=True),
            Choice(name="Austria [AUT]", value="AUT", checked=True),
        ],
        required=False,
    )
    prompt.search.text = "united"

    prompt._clear_visible()

    assert prompt._selected_values() == ["AUT"]


def test_mouse_click_moves_cursor_and_toggles_item() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
            Choice(name="United States [USA]", value="USA"),
        ],
    )

    handler = _row_handler(prompt, 1)
    handler(_mouse_event(MouseEventType.MOUSE_UP, y=1, button=MouseButton.LEFT))

    assert prompt.cursor == 1
    assert prompt._selected_values() == ["USA"]


def test_mouse_wheel_moves_cursor_within_bounds() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
            Choice(name="United States [USA]", value="USA"),
            Choice(name="Austria [AUT]", value="AUT"),
        ],
    )

    handler = _row_handler(prompt, 0)

    handler(_mouse_event(MouseEventType.SCROLL_UP, y=0, button=MouseButton.NONE))
    assert prompt.cursor == 0

    handler(_mouse_event(MouseEventType.SCROLL_DOWN, y=0, button=MouseButton.NONE))
    handler(_mouse_event(MouseEventType.SCROLL_DOWN, y=0, button=MouseButton.NONE))
    handler(_mouse_event(MouseEventType.SCROLL_DOWN, y=0, button=MouseButton.NONE))
    assert prompt.cursor == 2

    handler(_mouse_event(MouseEventType.SCROLL_UP, y=0, button=MouseButton.NONE))
    assert prompt.cursor == 1


def test_matches_window_uses_terminal_height_without_fixed_max() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
        ],
    )

    assert prompt.list_window.height.min == 10
    assert prompt.list_window.height.max > 1000


def test_no_matches_disables_cursor_movement() -> None:
    prompt = SearchableMultiSelect(
        "Select countries",
        [
            Choice(name="United Kingdom [GBR]", value="GBR"),
            Choice(name="United States [USA]", value="USA"),
        ],
    )
    prompt.search.text = "zzz"
    prompt._reset_cursor(None)

    assert prompt._render_choices() == [("class:muted", "No matches.\n")]

    prompt._move(1)
    assert prompt.cursor == -1
