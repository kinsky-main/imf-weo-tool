from __future__ import annotations

from dataclasses import dataclass
from itertools import cycle
import sys
from threading import Event, Thread
from time import sleep
from typing import Any, Callable, TypeVar

from prompt_toolkit.application import Application
from prompt_toolkit.application.current import get_app_or_none
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import Dimension, HSplit, Layout, Window
from prompt_toolkit.layout.containers import ConditionalContainer
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.margins import ScrollbarMargin
from prompt_toolkit.mouse_events import MouseEvent, MouseEventType
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Frame, TextArea

from .legacy import normalize_label


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Choice:
    name: str
    value: str
    checked: bool = False
    detail: str = ""


class SearchableMultiSelect:
    def __init__(
        self,
        title: str,
        choices: list[Choice],
        required: bool = True,
        max_selections: int | None = None,
    ) -> None:
        self.title = title
        self.choices = choices
        self.required = required
        self.max_selections = max_selections
        self.selected = {choice.value for choice in choices if choice.checked}
        self.cursor = 0
        self.error_message = ""
        self.search_focus_on_click = True

        self.search = TextArea(
            prompt="Search: ",
            multiline=False,
            wrap_lines=False,
            focus_on_click=self.search_focus_on_click,
        )
        self.search.buffer.on_text_changed += self._reset_cursor
        self.list_control = FormattedTextControl(self._render_choices, focusable=True)
        self.list_window = Window(
            content=self.list_control,
            wrap_lines=False,
            always_hide_cursor=True,
            height=Dimension(min=10),
            right_margins=[ScrollbarMargin(display_arrows=True)],
        )
        self.error_control = FormattedTextControl(self._render_error)
        self.title_window = self._build_title_window()
        self.search_frame = self._build_search_frame()
        self.matches_frame = self._build_matches_frame()
        self.error_window = self._build_error_window()
        self.style = Style.from_dict(
            {
                "title": "bold",
                "active": "bg:ansiblue fg:ansiwhite bold",
                "checked": "fg:ansigreen",
                "checked-active": "bg:ansiblue fg:ansiwhite bold",
                "muted": "italic",
                "error": "fg:ansired",
            }
        )

    def run(self) -> list[str]:
        bindings = self._bindings()
        root = self._build_root_container()
        application = Application(
            layout=Layout(root, focused_element=self.search),
            key_bindings=bindings,
            full_screen=True,
            mouse_support=True,
            style=self.style,
        )
        result = application.run()
        if result is None:
            raise KeyboardInterrupt("Selection cancelled.")
        return result

    def _build_title_window(self) -> Window:
        return Window(
            content=FormattedTextControl(
                [
                    ("class:title", self.title),
                    ("", f"\n{self._instructions()}"),
                ]
            ),
            height=2,
        )

    def _build_search_frame(self) -> Frame:
        return Frame(self.search, title="Search")

    def _build_matches_frame(self) -> Frame:
        return Frame(self.list_window, title="Matches")

    def _build_error_window(self) -> ConditionalContainer:
        return ConditionalContainer(
            Window(content=self.error_control, height=1),
            filter=Condition(lambda: bool(self.error_message)),
        )

    def _build_root_container(self) -> HSplit:
        return HSplit(
            [
                self.title_window,
                self.search_frame,
                self.matches_frame,
                self.error_window,
            ]
        )

    def _instructions(self) -> str:
        if self.max_selections == 1:
            return "Use Up/Down to move, Space to select, type to search, Enter to confirm, Esc to cancel."
        return (
            "Use Up/Down to move, Space to toggle, type to search, Ctrl-A to select visible, "
            "Ctrl-D to clear visible, Enter to confirm, Esc to cancel."
        )

    def _bindings(self) -> KeyBindings:
        kb = KeyBindings()

        @kb.add("down")
        def _down(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(1)
            event.app.invalidate()

        @kb.add("up")
        def _up(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(-1)
            event.app.invalidate()

        @kb.add("pagedown")
        def _page_down(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(10)
            event.app.invalidate()

        @kb.add("pageup")
        def _page_up(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(-10)
            event.app.invalidate()

        @kb.add(" ")
        def _toggle(event: Any) -> None:
            if event.app.layout.has_focus(self.list_window):
                self._toggle_current()
                event.app.invalidate()
                return
            self.search.buffer.insert_text(" ")

        @kb.add("tab")
        def _toggle_focus(event: Any) -> None:
            if event.app.layout.has_focus(self.search):
                event.app.layout.focus(self.list_window)
            else:
                event.app.layout.focus(self.search)

        @kb.add("c-a")
        def _select_all_visible(event: Any) -> None:
            if event.app.layout.has_focus(self.list_window):
                self._select_visible()
                event.app.invalidate()

        @kb.add("c-d")
        def _clear_all_visible(event: Any) -> None:
            if event.app.layout.has_focus(self.list_window):
                self._clear_visible()
                event.app.invalidate()

        @kb.add("enter")
        def _submit(event: Any) -> None:
            values = self._selected_values()
            if self.required and not values:
                self.error_message = "Select at least one item."
                event.app.invalidate()
                return
            self.error_message = ""
            event.app.exit(result=values)

        @kb.add("escape")
        @kb.add("c-c")
        def _cancel(event: Any) -> None:
            event.app.exit(result=None)

        return kb

    def _filtered_indexes(self) -> list[int]:
        query = normalize_label(self.search.text)
        if not query:
            return list(range(len(self.choices)))
        tokens = query.split()
        filtered: list[int] = []
        for index, choice in enumerate(self.choices):
            haystack = normalize_label(choice.name)
            if all(token in haystack for token in tokens):
                filtered.append(index)
        return filtered

    def _reset_cursor(self, _event: Any) -> None:
        filtered = self._filtered_indexes()
        self.cursor = 0 if filtered else -1

    def _move(self, amount: int) -> None:
        filtered = self._filtered_indexes()
        if not filtered:
            self.cursor = -1
            return
        if self.cursor < 0:
            self.cursor = 0
            return
        self.cursor = max(0, min(self.cursor + amount, len(filtered) - 1))

    def _toggle_current(self) -> None:
        filtered = self._filtered_indexes()
        if not filtered or self.cursor < 0:
            return
        choice = self.choices[filtered[self.cursor]]
        if choice.value in self.selected:
            self.selected.remove(choice.value)
            return
        if self.max_selections == 1:
            self.selected = {choice.value}
            return
        self.selected.add(choice.value)

    def _selected_values(self) -> list[str]:
        return [choice.value for choice in self.choices if choice.value in self.selected]

    def _select_visible(self) -> None:
        if self.max_selections == 1:
            return
        for choice_index in self._filtered_indexes():
            self.selected.add(self.choices[choice_index].value)

    def _clear_visible(self) -> None:
        if self.max_selections == 1:
            return
        for choice_index in self._filtered_indexes():
            self.selected.discard(self.choices[choice_index].value)

    def _choice_style(self, visible_index: int, checked: bool) -> str:
        if visible_index == self.cursor:
            return "class:checked-active" if checked else "class:active"
        if checked:
            return "class:checked"
        return ""

    def _row_mouse_handler(self, visible_index: int) -> Callable[[MouseEvent], object | None]:
        def handler(mouse_event: MouseEvent) -> object | None:
            app = get_app_or_none()
            if app is not None:
                app.layout.focus(self.list_window)

            if mouse_event.event_type == MouseEventType.MOUSE_UP:
                self.cursor = visible_index
                self._toggle_current()
            elif mouse_event.event_type == MouseEventType.SCROLL_DOWN:
                self._move(1)
            elif mouse_event.event_type == MouseEventType.SCROLL_UP:
                self._move(-1)
            else:
                return NotImplemented

            if app is not None:
                app.invalidate()
            return None

        return handler

    def _render_choices(self) -> list[tuple[str, str] | tuple[str, str, Callable[[MouseEvent], object | None]]]:
        fragments: list[tuple[str, str] | tuple[str, str, Callable[[MouseEvent], object | None]]] = []
        filtered = self._filtered_indexes()
        if not filtered:
            return [("class:muted", "No matches.\n")]

        for visible_index, choice_index in enumerate(filtered):
            choice = self.choices[choice_index]
            prefix = "[x]" if choice.value in self.selected else "[ ]"
            style = self._choice_style(visible_index, choice.value in self.selected)
            handler = self._row_mouse_handler(visible_index)
            label = f"{prefix} {choice.name}"
            if choice.detail:
                label = f"{label}  {choice.detail}"
            if visible_index == self.cursor:
                fragments.append(("[SetCursorPosition]", "", handler))
            fragments.append((style, label, handler))
            fragments.append((style, "\n", handler))
        return fragments

    def _render_error(self) -> list[tuple[str, str]]:
        return [("class:error", self.error_message)]


def prompt_for_choices(title: str, raw_choices: list[dict[str, Any]], required: bool = True) -> list[str]:
    choices = [
        Choice(
            name=str(item["name"]),
            value=str(item["value"]),
            checked=bool(item.get("checked", False)),
            detail=str(item.get("detail", "")),
        )
        for item in raw_choices
    ]
    return SearchableMultiSelect(title=title, choices=choices, required=required).run()


def prompt_for_choice(title: str, raw_choices: list[dict[str, Any]]) -> str:
    choices = [Choice(name=str(item["name"]), value=str(item["value"])) for item in raw_choices]
    result = SearchableMultiSelect(title=title, choices=choices, required=True, max_selections=1).run()
    return result[0]


def run_with_status(message: str, func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    stop = Event()

    def spinner() -> None:
        symbols = cycle("|/-\\")
        while not stop.is_set():
            sys.stderr.write(f"\r{message} {next(symbols)}")
            sys.stderr.flush()
            sleep(0.1)
        sys.stderr.write(f"\r{message} done\n")
        sys.stderr.flush()

    thread = Thread(target=spinner, daemon=True)
    thread.start()
    try:
        return func(*args, **kwargs)
    finally:
        stop.set()
        thread.join(timeout=1)
